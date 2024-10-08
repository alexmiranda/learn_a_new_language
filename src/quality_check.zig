const std = @import("std");
const heap = std.heap;
const mem = std.mem;
const Pool = std.Thread.Pool;
const WaitGroup = std.Thread.WaitGroup;
const Mutex = std.Thread.Mutex;
const Order = std.math.Order;
const print = std.debug.print;
const assert = std.debug.assert;
const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;

const T = u32;
const max_countries = 26 * 26;
const SalesVec = @Vector(max_countries, T);

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
    try pool.init(.{ .allocator = ally, .n_jobs = n_jobs });
    defer pool.deinit();

    // country sales
    var country_sales = try std.ArrayList(SalesVec).initCapacity(ally, n_jobs);
    defer country_sales.deinit();

    // spawn each worker reusing the thread pool
    var start_pos: usize = 0;
    var mut = Mutex{};
    for (1..n_jobs + 1) |job_id| {
        const pos = mmap.len / n_jobs * job_id;
        const end_pos = mem.indexOfScalarPos(u8, mmap, pos, '\n') orelse mmap.len;
        pool.spawnWg(&wg, work, .{ job_id, mmap[start_pos..end_pos], &country_sales, &mut });
        start_pos = end_pos +| 1;
        if (start_pos >= mmap.len) break;
    }

    // wait for all jobs to finish
    pool.waitAndWork(&wg);

    // aggregate results using simd
    var acc: SalesVec = country_sales.items[0];
    for (country_sales.items[1..]) |partial_sales| {
        acc = acc + partial_sales;
    }

    // sort countries by total sales
    const Context = struct {
        fn compare(ctx: void, lhs: Stat, rhs: Stat) Order {
            _ = ctx;
            return std.math.order(rhs.total_sales, lhs.total_sales);
        }
    };

    var max_heap = std.PriorityQueue(Stat, void, Context.compare).init(ally, {});
    defer max_heap.deinit();
    try max_heap.ensureTotalCapacity(max_countries);

    var i: u16 = 0;
    while (i < max_countries) : (i += 1) {
        const sold = acc[i];
        if (sold > 0) {
            try max_heap.add(.{ .country = i, .total_sales = sold });
        }
    }

    // print the result to the screen
    var buf: [2]u8 = undefined;
    for (1..11) |rank| {
        const stat = max_heap.removeOrNull() orelse break;
        const amount_int = stat.total_sales / 100;
        const amount_dec = stat.total_sales % 100;
        try stdout.print("{d:0>2}. country={s} total_sales={d}.{d}\n", .{ rank, countryCodeFromIndex(stat.country, &buf), amount_int, amount_dec });
    }
}

fn work(job_id: usize, buffer: []const u8, totals: *std.ArrayList(SalesVec), mut: *Mutex) void {
    const json = std.json;
    var country_sales: SalesVec = @splat(0);

    // memory required is equal to the number of bytes needed by the bitstack used by json scanner
    const memory_required = (2 + 7) << 3;
    var bytes: [memory_required]u8 = undefined;
    var fba = heap.FixedBufferAllocator.init(&bytes);
    const ally = fba.allocator();

    assert(buffer[0] == '{' and buffer[buffer.len - 1] == '}');
    var it = mem.tokenizeScalar(u8, buffer, '\n');
    outer_loop: while (it.next()) |line| {
        // ensure we reset the fixed buffer allocator on every new line
        defer fba.reset();

        assert(line[0] == '{' and line[line.len - 1] == '}');
        var scanner = json.Scanner.initCompleteInput(ally, line);
        defer scanner.deinit();

        // prealloc two levels, as we know the json contain at most two nesting levels
        scanner.ensureTotalStackCapacity(2) catch {};

        var token: json.Token = undefined;
        var last_str: []const u8 = "";
        var id: []const u8 = "";
        var amount: T = 0;
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
                    if (mem.eql(u8, last_str, "amount")) {
                        if (str[0] == '-') continue :outer_loop;
                        for (str) |c| {
                            if (c == '.') continue;
                            assert(std.ascii.isDigit(c));
                            amount *= 10;
                            amount += c - '0';
                        }
                    }

                    // TODO: filter out invalid credit card

                    // increment country sales
                    if (mem.eql(u8, last_str, "country")) {
                        country_sales[countryCodeIndex(str)] += amount;
                        continue :outer_loop;
                    }

                    last_str = str;
                },
                else => {},
            }
        }
    }

    // report back the country sales to main thread
    // a block here makes the critical region as minimal as possible
    // if we need to add more code to the function body...
    {
        mut.lock();
        defer mut.unlock();
        // safe because we preallocate capacity in the main thread
        totals.appendAssumeCapacity(country_sales);
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
