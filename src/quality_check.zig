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
        const millis = @divFloor(end - start, 1_000_000);
        stdout.print("Took {d} nanoseconds; {d} milliseconds.\n", .{ end - start, millis }) catch {};
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
    // const n_jobs: u32 = 2;
    try pool.init(.{ .allocator = ally, .n_jobs = n_jobs });
    defer pool.deinit();

    // country sales
    var country_sales: [max_countries]std.atomic.Value(T) = undefined;
    for (0..max_countries) |slide| {
        country_sales[slide] = std.atomic.Value(T).init(0);
    }

    // spawn each worker reusing the thread pool
    var start_pos: usize = 0;
    for (1..n_jobs + 1) |job_id| {
        const pos = mmap.len / n_jobs * job_id;
        const end_pos = mem.indexOfScalarPos(u8, mmap, pos, '\n') orelse mmap.len;
        pool.spawnWg(&wg, work, .{ job_id, mmap[start_pos..end_pos], &country_sales });
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
        // safe to use raw value because no other thread is reading or writing at this point
        const sold = country_sales[i].raw;
        if (sold > 0) {
            try max_heap.add(.{ .country = i, .total_sales = sold });
        }
    }

    // print the result to the screen
    var buf: [2]u8 = undefined;
    for (1..11) |rank| {
        const stat = max_heap.removeOrNull() orelse break;
        try stdout.print("{d:0>2}. country={s} total_sales={d}\n", .{ rank, countryCodeFromIndex(stat.country, &buf), stat.total_sales });
    }
}

fn work(job_id: usize, buffer: []const u8, totals: []std.atomic.Value(T)) void {
    const json = std.json;
    var country_sales: [max_countries]T = [_]T{0} ** max_countries;

    assert(buffer[0] == '{' and buffer[buffer.len - 1] == '}');
    var it = mem.tokenizeScalar(u8, buffer, '\n');
    outer_loop: while (it.next()) |line| {
        assert(line[0] == '{' and line[line.len - 1] == '}');
        var scanner = json.Scanner.initCompleteInput(heap.c_allocator, line);
        defer scanner.deinit();

        // prealloc two levels, as we know the json contain at most two nesting levels
        scanner.ensureTotalStackCapacity(2) catch {};

        var token: json.Token = undefined;
        var last_str: []const u8 = "";
        var id: []const u8 = "";
        while (true) {
            token = scanner.next() catch |err| {
                switch (err) {
                    error.OutOfMemory => return,
                    else => {
                        print("job_id={d} err={s} id={s} cursor={d}\n", .{ job_id, @errorName(err), id, scanner.cursor });
                        continue :outer_loop;
                    },
                }
            };
            switch (token) {
                .end_of_document => continue :outer_loop,
                .string, .number => |str| {
                    if (mem.eql(u8, last_str, "id")) id = str;

                    // filter out empty name
                    if (mem.eql(u8, last_str, "name") and str.len == 0) {
                        continue :outer_loop;
                    }

                    // filter out negative amounts
                    if (mem.eql(u8, last_str, "amount") and str[0] == '-') {
                        continue :outer_loop;
                    }

                    // TODO: filter out invalid credit card

                    // increment country sales
                    if (mem.eql(u8, last_str, "country")) {
                        country_sales[countryCodeIndex(str)] += 1;
                    }

                    last_str = str;
                },
                else => {},
            }
        }
    }

    // update the total country sales
    for (totals, 0..) |*sold, slide| {
        if (country_sales[slide] > 0) {
            _ = sold.fetchAdd(country_sales[slide], .acq_rel);
        }
    }
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
