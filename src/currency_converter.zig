const std = @import("std");
const heap = std.heap;
const json = std.json;
const meta = std.meta;
const mem = std.mem;
const print = std.debug.print;
const file = @embedFile("exchange_rates_usd.json");

const ConversionRate = struct {
    base: []const u8,
    date: []const u8,
    rates: json.Value,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const ally = gpa.allocator();

    const parsed = try json.parseFromSlice(ConversionRate, ally, file, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var args = std.process.args();
    defer args.deinit();

    _ = args.next();
    const arg = args.next().?;
    const currency = args.next() orelse "EUR";

    const amount_to_convert = try std.fmt.parseFloat(f32, arg);
    if (findExchangeRate(parsed.value, currency)) |rate| {
        print("{d} USD is {d} {s}\n", .{ amount_to_convert, amount_to_convert * rate, currency });
    } else {
        print("currency not found: {s}\n", .{currency});
    }
}

fn findExchangeRate(conv: ConversionRate, currency: []const u8) ?f32 {
    switch (conv.rates) {
        .object => |obj| {
            if (obj.get(currency)) |rate| {
                switch (rate) {
                    .float => |value| return @floatCast(value),
                    else => @panic("invalid json!"),
                }
            }
            return null;
        },
        else => unreachable,
    }
}
