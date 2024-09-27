const std = @import("std");
const heap = std.heap;
const json = std.json;
const meta = std.meta;
const mem = std.mem;
const print = std.debug.print;
const file = @embedFile("exchange_rates_usd.json");

const ConversionRate = struct { base: []const u8, date: []const u8, rates: struct {
    BGN: f32,
    BRL: f32,
    CAD: f32,
    CHF: f32,
    CNY: f32,
    CZK: f32,
    DKK: f32,
    GBP: f32,
    HKD: f32,
    HRK: f32,
    HUF: f32,
    IDR: f32,
    ILS: f32,
    INR: f32,
    JPY: f32,
    KRW: f32,
    MXN: f32,
    MYR: f32,
    NOK: f32,
    NZD: f32,
    PHP: f32,
    PLN: f32,
    RON: f32,
    RUB: f32,
    SEK: f32,
    SGD: f32,
    THB: f32,
    TRY: f32,
    ZAR: f32,
    EUR: f32,
} };

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
    inline for (meta.fields(@TypeOf(conv.rates))) |field| {
        if (mem.eql(u8, field.name, currency)) {
            return @field(conv.rates, field.name);
        }
    } else return null;
}
