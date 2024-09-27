const std = @import("std");
const io = std.io;
const fmt = std.fmt;
const heap = std.heap;
const json = std.json;
const meta = std.meta;
const mem = std.mem;
const math = std.math;
const process = std.process;
const testing = std.testing;
const print = std.debug.print;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectError = std.testing.expectError;
const expectApproxEqAbs = std.testing.expectApproxEqAbs;
const assert = std.debug.assert;
const file = @embedFile("exchange_rates_usd.json");

// roughly 4 decimal digits precision.
const tolerance = 1e-4 - math.floatEps(f64);

const ConversionError = error{
    /// Indicates a problem with our dataset.
    DataProblem,
    /// Indicates a problem with the input or result is infinite or Nan.
    Uncomputable,
    /// Indicates a problem with the source currency code.
    SourceCurrencyNotFound,
    /// Indicates a problem with the target currency code.
    TargetCurrencyNotFound,
};

const ArgsError = error{
    MissingAmount,
    MissingSource,
    TooManyArguments,
};

const DetectedIssue = enum {
    tooManyDecimalDigits,
    lossOfPrecision,
};

/// ConversionRate holds the parsed json data.
const ConversionRate = struct {
    base: []const u8,
    rates: json.Value,
};

pub fn main() !void {
    const stderr = std.io.getStdErr().writer();
    const stdout = std.io.getStdOut().writer();

    // we use a general purpose allocator which internally has a backing page allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const ally = gpa.allocator();

    // parse the json
    const parsed = try parseDataset(ally, file);
    defer parsed.deinit();

    // parse the command line args
    const amount, const source, const target = processArgs(&process.args(), parsed.value.base) catch |err| {
        return switch (err) {
            error.MissingAmount => {
                try stderr.print("Expected amount and currency arguments. None specified.\n", .{});
            },
            error.MissingSource => {
                const supported_currencies = try listSupportedCurrencies(ally, parsed.value);
                defer ally.free(supported_currencies);
                try stderr.print("Please specify a currency from the list of: {s}\n", .{supported_currencies});
            },
            error.TooManyArguments => {
                try stderr.print("Too many arguments!\n", .{});
            },
        };
    };

    // parse the amount to convert
    const amount_to_convert, const possible_issue = parseAmountSafely(ally, amount) catch |err| {
        return switch (err) {
            error.InvalidCharacter => try stderr.print("Not a valid amount: {s}\n", .{amount}),
            error.OutOfMemory => @panic("OutOfMemory error."),
        };
    };

    // if detected any issues with input, warn the user and continue
    if (possible_issue) |issue| {
        switch (issue) {
            .tooManyDecimalDigits => try stderr.print("WARNING: Too many decimal digits in {s}. Value will be rounded down to 4 decimal places.\n", .{amount}),
            .lossOfPrecision => try stderr.print("WARNING: Detected loss of precision for amount {s}.\n", .{amount}),
        }
    }

    // convert the amount to the target currency
    const converted = convert(parsed.value, amount_to_convert, source, target) catch |err| {
        return switch (err) {
            error.DataProblem => try stderr.print("We're sorry - we messed up! Currency {s} has an exchange rate of zero and that conversion would wreck havoc in the world's financial systems were it allowed!\n", .{source}),
            error.Uncomputable => try stderr.print("Whoa! It looks like you just broke the bank - literally!\n", .{}),
            error.SourceCurrencyNotFound => try stderr.print("Currency not found: {s}\n", .{source}),
            error.TargetCurrencyNotFound => try stderr.print("Currency not found: {s}\n", .{target}),
        };
    };

    // print the results
    try stdout.print("{d:.4} {s} = {d:.4} {s}\n", .{ amount_to_convert, source, converted, target });
}

/// Processes command line args.
fn processArgs(args: *const process.ArgIterator, base_currency: []const u8) !struct { []const u8, []const u8, []const u8 } {
    var it = @constCast(args);
    defer it.deinit();

    _ = it.skip();
    const amount = it.next() orelse return error.MissingAmount;
    const source = it.next() orelse return error.MissingSource;
    const target = it.next() orelse base_currency;

    if (it.skip()) return error.TooManyArguments;
    return .{ amount, source, target };
}

/// Parses the amount and checks if there are any potential issues.
fn parseAmountSafely(ally: mem.Allocator, amount: []const u8) !std.meta.Tuple(&[_]type{ f64, ?DetectedIssue }) {
    const parsed = try std.fmt.parseFloat(f64, amount);
    const formatted = try fmt.allocPrint(ally, "{d:.4}", .{parsed});
    defer ally.free(formatted);

    // this check only works if not using scientific notation...
    if (mem.indexOfAnyPos(u8, amount, 1, "eE") == null) {
        // check if user input has a decimal sep and determine if all decimals are equal
        if (mem.indexOfScalar(u8, amount, '.')) |sep| {
            if (amount.len - sep - 1 > 4) {
                return .{ parsed, .tooManyDecimalDigits };
            } else if (!mem.eql(u8, amount[sep + 1 ..], formatted[sep + 1 ..])) {
                return .{ parsed, .lossOfPrecision };
            }
        }
    }

    return .{ parsed, null };
}

/// Converts between source and target currency amount using a dataset.
fn convert(dataset: ConversionRate, amount: f64, source: []const u8, target: []const u8) !f64 {
    @setFloatMode(.optimized);
    // source and target currencies are the same
    if (mem.eql(u8, source, target)) {
        return amount;
    }

    // if the amount is zero or very close to zero, we return zero without converting
    if (math.approxEqAbs(f64, amount, 0.0, tolerance)) {
        return 0;
    }

    // if the amount is Nan or Inf, we return an error
    if (math.isNan(amount) or math.isInf(amount)) {
        return error.Uncomputable;
    }

    const to_base_rate = findExchangeRate(dataset, source) orelse return error.SourceCurrencyNotFound;
    const to_target_rate = findExchangeRate(dataset, target) orelse return error.TargetCurrencyNotFound;

    // if the source currency exchange rate (the denominator) is zero, we know that the problem lies in our dataset
    if (math.approxEqAbs(f64, to_base_rate, 0.0, tolerance)) return error.DataProblem;

    // we convert the amount from the base to the target currency
    const converted_to_base = amount / to_base_rate;
    const result = converted_to_base * to_target_rate;

    // if the result is infinite, we return an error
    if (math.isInf(result)) return error.Uncomputable;
    return result;
}

/// Looks up the conversion rate for a given currency.
/// If found, it returns an f64 value; Otherwise it returns null.
fn findExchangeRate(conv: ConversionRate, currency: []const u8) ?f64 {
    // return 1.0 if using the base currency
    if (mem.eql(u8, currency, conv.base)) return 1.0;
    switch (conv.rates) {
        .object => |obj| {
            if (obj.get(currency)) |rate| {
                return switch (rate) {
                    .float => |value| value,
                    .integer => |value| @floatFromInt(value),
                    else => @panic("invalid json!"),
                };
            }
            return null;
        },
        else => unreachable,
    }
}

/// Returns an owned slice containing the codes of all supported currencies.
fn listSupportedCurrencies(ally: mem.Allocator, conv: ConversionRate) ![]u8 {
    const map = switch (conv.rates) {
        .object => |obj| obj,
        else => unreachable,
    };

    // all currency codes contain exactly 3 characters
    // so we are able to prealloc all the space that we need
    const n = map.count();
    const size = (3 * n) + (n - 1);
    const list = try ally.alloc(u8, size);
    errdefer ally.free(list);

    var slide: usize = 0;
    for (map.keys()) |key| {
        // some of the test data needs to be discarded...
        if (key.len != 3) continue;

        // concat sep
        if (slide > 0) {
            list[slide] = ' ';
            slide += 1;
        }

        // concat currency code
        @memcpy(list[slide .. slide + 3], key[0..3]);
        slide += 3;
    }

    // resize is safe because slide < old len
    assert(ally.resize(list, slide));
    return list[0..slide];
}

/// Decodes json into a struct containing base and a map of all exchanges rates.
fn parseDataset(ally: mem.Allocator, buffer: []const u8) !json.Parsed(ConversionRate) {
    var parsed = try json.parseFromSlice(ConversionRate, ally, buffer, .{
        .ignore_unknown_fields = true,
    });
    errdefer parsed.deinit();
    return parsed;
}

test "convert" {
    const ally = testing.allocator;
    const parsed = try parseDataset(ally, file);
    defer parsed.deinit();

    const dataset = parsed.value;

    // zero amount
    try expectEqual(0, convert(dataset, 0.0, "USD", "EUR"));
    try expectEqual(0, convert(dataset, -0.0, "USD", "EUR"));
    try expectEqual(0, convert(dataset, 0.00009, "USD", "EUR"));
    try expectEqual(0, convert(dataset, -0.00009, "USD", "EUR"));
    try expectEqual(0, convert(dataset, 0.000099999999, "USD", "EUR"));
    try expectEqual(0, convert(dataset, -0.000099999999, "USD", "EUR"));

    // Nan
    try expectError(error.Uncomputable, convert(dataset, math.nan(f64), "USD", "EUR"));

    // Infinite
    try expectError(error.Uncomputable, convert(dataset, math.inf(f64), "USD", "EUR"));

    // USD to EUR
    {
        const res = (try convert(dataset, 1, "USD", "EUR"));
        try expectApproxEqAbs(0.8927, res, tolerance);
    }
    {
        const res = (try convert(dataset, 10, "USD", "EUR"));
        try expectApproxEqAbs(8.927, res, tolerance);
    }
    {
        const res = (try convert(dataset, math.phi, "USD", "EUR"));
        try expectApproxEqAbs(1.4444, res, tolerance);
    }
    {
        const res = (try convert(dataset, math.pi, "USD", "EUR"));
        try expectApproxEqAbs(2.8044, res, tolerance);
    }

    // EUR to USD
    {
        const res = (try convert(dataset, 1, "EUR", "USD"));
        try expectApproxEqAbs(1.1201, res, tolerance);
    }
    {
        const res = (try convert(dataset, 10, "EUR", "USD"));
        try expectApproxEqAbs(11.2019, res, tolerance);
    }
    {
        const res = (try convert(dataset, math.phi, "EUR", "USD"));
        try expectApproxEqAbs(1.8125, res, tolerance);
    }
    {
        const res = (try convert(dataset, math.pi, "EUR", "USD"));
        try expectApproxEqAbs(3.5192, res, tolerance);
    }

    // GBP to EUR
    {
        const res = (try convert(dataset, 1, "GBP", "EUR"));
        try expectApproxEqAbs(1.2991, res, tolerance);
    }
    {
        const res = (try convert(dataset, 10, "GBP", "EUR"));
        try expectApproxEqAbs(12.9913, res, tolerance);
    }
    {
        const res = (try convert(dataset, math.phi, "GBP", "EUR"));
        try expectApproxEqAbs(2.1020, res, tolerance);
    }
    {
        const res = (try convert(dataset, math.pi, "GBP", "EUR"));
        try expectApproxEqAbs(4.0813, res, tolerance);
    }

    // Negatives
    {
        const res = (try convert(dataset, -2, "USD", "EUR"));
        try expectApproxEqAbs(-1.7854, res, tolerance);
    }
    {
        const res = (try convert(dataset, -4, "EUR", "GBP"));
        try expectApproxEqAbs(-3.0789, res, tolerance);
    }

    // Amount is too large (infinite float)
    try expectError(error.Uncomputable, convert(dataset, math.floatMax(f64) / 2, "GBP", "KRW"));
    try expectError(error.Uncomputable, convert(dataset, math.floatMax(f64) / -2, "GBP", "KRW"));

    // Data problem (max precision is 4 digits)
    try expectError(error.DataProblem, convert(dataset, 1, "ERR", "TRY"));
    try expectError(error.DataProblem, convert(dataset, 1, "SHITCOIN", "USD"));
}

test "findExchangeRate" {
    const ally = testing.allocator;
    const parsed = try parseDataset(ally, file);
    defer parsed.deinit();

    try expect(findExchangeRate(parsed.value, "USD") != null);
    try expect(findExchangeRate(parsed.value, "EUR") != null);
    try expect(findExchangeRate(parsed.value, "GBP") != null);
    try expectEqual(null, findExchangeRate(parsed.value, "XXX"));
}

test "parseAmountSafely" {
    const ally = testing.allocator;

    {
        const parsed, _ = try parseAmountSafely(ally, "3.1415");
        try expectEqual(3.1415, parsed);
    }

    {
        const parsed, const issue = try parseAmountSafely(ally, "1.6180339887498948482045868343656381177203091798057628621");
        try expectApproxEqAbs(1.6180339887498948482045868343656381177203091798057628621, parsed, tolerance);
        try expectEqual(.tooManyDecimalDigits, issue);
    }

    {
        const parsed, const issue = try parseAmountSafely(ally, "8888888888888888.7777");
        try expectApproxEqAbs(8888888888888888.7000, parsed, tolerance);
        try expectEqual(.lossOfPrecision, issue);
    }

    {
        const parsed, const issue = try parseAmountSafely(ally, "888888888888888.7777");
        try expectApproxEqAbs(888888888888888.7700, parsed, tolerance);
        try expectEqual(.lossOfPrecision, issue);
    }

    {
        const parsed, const issue = try parseAmountSafely(ally, "888888888888888.7777");
        try expectApproxEqAbs(888888888888888.7770, parsed, tolerance);
        try expectEqual(.lossOfPrecision, issue);
    }

    {
        const parsed, _ = try parseAmountSafely(ally, "1e-2");
        try expectEqual(0.01, parsed);
    }

    {
        const parsed, _ = try parseAmountSafely(ally, "1e256");
        try expectEqual(1e256, parsed);
    }
}
