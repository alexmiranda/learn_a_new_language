const std = @import("std");
const heap = std.heap;
const mem = std.mem;
const simd = std.simd;
const Pool = std.Thread.Pool;
const WaitGroup = std.Thread.WaitGroup;
const Mutex = std.Thread.Mutex;
const Order = std.math.Order;
const print = std.debug.print;
const assert = std.debug.assert;
const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;

// the type for counting total sales, the smaller it is, the better is the performance for vectorised computations
const T = u16;
const max_countries = 26 * 26;

const Stat = struct {
    country: u16,
    total_sales: T,
};

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    const start = std.time.nanoTimestamp();
    defer {
        const end = std.time.nanoTimestamp();
        const millis = @divFloor(end - start, 1000);
        stdout.print("Took: {d} nanoseconds {d} Milliseconds.\n", .{ end - start, millis }) catch {};
    }

    const process = std.process;
    const fs = std.fs;

    // open the file for read-only
    const file_name = blk: {
        var args = try process.argsWithAllocator(heap.c_allocator);
        defer args.deinit();
        _ = args.skip(); // 0: program name
        break :blk args.next() orelse "data/payments-1000.jsonl";
    };
    const path = blk: {
        var buf: [fs.max_path_bytes:0]u8 = undefined;
        break :blk try fs.cwd().realpathZ(file_name, &buf);
    };
    const file = try fs.openFileAbsolute(path, .{ .mode = .read_only });
    defer file.close();

    // map the file into memory
    const file_len = try file.getEndPos();
    const mmap = try std.posix.mmap(null, file_len, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, file.handle, 0);
    defer std.posix.munmap(mmap);

    // let's use an arena to try and see how it affects the performance
    var arena = heap.ArenaAllocator.init(heap.page_allocator);
    defer arena.deinit();
    const ally = arena.allocator();

    // create a thread pool with the number of cpus
    var wg = WaitGroup{};
    var pool: Pool = undefined;
    const n_jobs: u32 = @intCast(try std.Thread.getCpuCount());
    try pool.init(.{ .allocator = ally, .n_jobs = n_jobs });
    defer pool.deinit();

    // country sales
    var country_sales: @Vector(max_countries, T) = @splat(0);
    var mutex = Mutex{};

    // spawn each worker reusing the thread pool
    var start_pos: usize = 0;
    for (1..n_jobs + 1) |job_id| {
        const pos = mmap.len / n_jobs * job_id;
        const end_pos = mem.indexOfScalarPos(u8, mmap, pos, '\n') orelse mmap.len;
        pool.spawnWg(&wg, work, .{ ally, job_id, mmap[start_pos..end_pos], &country_sales, &mutex });
        start_pos = end_pos +| 1;
        if (start_pos >= mmap.len) break;
    }

    // wait for all the jobs to finish
    pool.waitAndWork(&wg);

    // sort countries by total sales
    const Context = struct {
        fn compare(ctx: void, lhs: Stat, rhs: Stat) Order {
            _ = ctx;
            return std.math.order(rhs.total_sales, lhs.total_sales);
        }
    };

    var max_heap = std.PriorityQueue(Stat, void, Context.compare).init(ally, {});
    try max_heap.ensureTotalCapacity(max_countries);
    var i: u16 = 0;
    while (i < max_countries) : (i += 1) {
        if (country_sales[i] > 0) {
            try max_heap.add(.{ .country = i, .total_sales = country_sales[i] });
        }
    }

    // print the result to the screen
    var buf: [2]u8 = undefined;
    for (1..11) |rank| {
        const stat = max_heap.removeOrNull() orelse break;
        try stdout.print("{d:0>2}. country={s} total_sales={d}\n", .{ rank, countryCodeFromIndex(stat.country, &buf), stat.total_sales });
    }
}

fn work(ally: mem.Allocator, job_id: usize, buffer: []const u8, totals: *@Vector(max_countries, T), mutex: *Mutex) void {
    _ = job_id;
    const json = std.json;
    var country_sales: @Vector(26 * 26, T) = @splat(0);

    var it = mem.tokenizeScalar(u8, buffer, '\n');
    outer_loop: while (it.next()) |line| {
        var scanner = json.Scanner.initCompleteInput(ally, line);
        defer scanner.deinit();

        var token: json.Token = undefined;
        var lvl: u8 = 0;
        var last_str: []const u8 = undefined;
        var id: []const u8 = undefined;
        while (true) {
            token = scanner.next() catch |err| {
                switch (err) {
                    error.OutOfMemory => return,
                    else => continue :outer_loop,
                }
            };
            switch (token) {
                .object_begin => {
                    lvl += 1;
                },
                .object_end => {
                    // reached object_end, but the level was 0!
                    if (lvl == 0) @panic("invalid json!");
                },
                .end_of_document => {
                    continue :outer_loop;
                },
                .string => |str| {
                    if (mem.eql(u8, last_str, "id")) id = str;

                    // filter out empty name
                    if (mem.eql(u8, last_str, "name") and str.len == 0) {
                        continue :outer_loop;
                    }

                    // TODO: filter out invalid credit card

                    // increment country sales
                    if (mem.eql(u8, last_str, "country")) {
                        country_sales[countryCodeIndex(str)] += 1;
                    }

                    last_str = str;
                },
                .number => |str| {
                    // filter out negative amount
                    if (mem.eql(u8, last_str, "amount") and str[0] == '-') {
                        continue :outer_loop;
                    }
                },
                else => {},
            }
        }
    }

    // update the total country sales
    mutex.lock();
    defer mutex.unlock();
    totals.* = totals.* + country_sales;
}

fn countryCodeIndex(country: []const u8) u16 {
    assert(country.len == 2);
    assert(std.ascii.isUpper(country[0]) and std.ascii.isUpper(country[1]));
    const a: u16 = country[0] - 'A';
    const b: u16 = country[1] - 'A';
    return a * 26 + b;
}

fn countryCodeFromIndex(country: u16, buf: []u8) []const u8 {
    buf[0] = @truncate(country / 26 + 'A');
    buf[1] = @truncate(country % 26 + 'A');
    return buf[0..2];
}

test "countryCodeIndex" {
    try expectEqual(0, countryCodeIndex("AA"));
    try expectEqual(1, countryCodeIndex("AB"));
    try expectEqual(26, countryCodeIndex("BA"));
    try expectEqual(675, countryCodeIndex("ZZ"));
}

test "countryCodeFromIndex" {
    var buf: [2]u8 = undefined;
    try expectEqualStrings("AA", countryCodeFromIndex(countryCodeIndex("AA"), &buf));
    try expectEqualStrings("BC", countryCodeFromIndex(countryCodeIndex("BC"), &buf));
    try expectEqualStrings("DE", countryCodeFromIndex(countryCodeIndex("DE"), &buf));
    try expectEqualStrings("ZZ", countryCodeFromIndex(countryCodeIndex("ZZ"), &buf));
}
