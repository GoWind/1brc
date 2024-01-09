const std = @import("std");
const File = std.fs.File;
const numWorkers = 12;
const SliceList = std.ArrayList([]const u8);
const writer = std.io.getStdOut().writer();
pub const Record = struct { min: i32 = 0, max: i32 = 0, total: i32 = 0, count: u32 = 0 };
const TempMap = std.StringHashMap(Record);
var threadMap: [numWorkers]TempMap = undefined;
var threads: [numWorkers]std.Thread = undefined;
pub fn main() !void {
    const start = std.time.nanoTimestamp();
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var threadSafeAllocator = std.heap.ThreadSafeAllocator{ .child_allocator = allocator };
    const threadAllocator = threadSafeAllocator.allocator();
    const args = try std.process.argsAlloc(allocator);
    const filepath = args[1];
    const fd = try std.os.open(filepath, std.os.O.RDONLY, 0);
    defer std.os.close(fd);
    const stat = try std.os.fstat(fd);
    const mapping = try std.os.mmap(null, @as(u64, @intCast(stat.size)), std.os.PROT.READ, std.os.MAP.PRIVATE, fd, 0);
    defer std.os.munmap(mapping);

    const sizeFloat: f64 = @floatFromInt(stat.size);
    const workerSize: u64 = @intFromFloat(@floor(sizeFloat / numWorkers));
    var globalMap = TempMap.init(threadAllocator);
    var i: usize = 0;
    while (i < numWorkers) : (i += 1) {
        threadMap[i] = TempMap.init(threadAllocator);
        const thread = try std.Thread.spawn(.{}, calculate, .{ i, workerSize, threadAllocator, mapping });
        threads[i] = thread;
    }
    i = 0;
    while (i < numWorkers) : (i += 1) {
        threads[i].join();
    }
    const totalProcessed = try mergeMaps(threadAllocator, &globalMap, &threadMap);
    try printGlobalMap(allocator, globalMap);
    const end = std.time.nanoTimestamp();
    try writer.print("{} rows took {d} nanoseconds\n", .{ totalProcessed, end - start });
}

fn calculate(
    idx: usize,
    workerSize: u64,
    allocator: std.mem.Allocator,
    file: []u8,
) !void {
    const finalEndOffset = file.len - 1;
    var startOffset = idx * workerSize;
    var endOffset = (idx + 1) * workerSize - 1;
    if (startOffset > 0) {
        const prev = startOffset - 1;
        if (file[prev] != '\n') {
            while (file[startOffset] != '\n') {
                startOffset += 1;
            }
            startOffset += 1;
        } else {}
    }
    if (endOffset < finalEndOffset) {
        while (endOffset < finalEndOffset and file[endOffset] != '\n') {
            endOffset += 1;
        }
    }
    var i: usize = startOffset;
    var j: usize = i;
    var city: []u8 = undefined;
    var num: []u8 = undefined;
    while (j <= endOffset) : (j += 1) {
        if (file[j] == ';') {
            city = file[i..j];
            i = j + 1;
        } else if (file[j] == '\n') {
            num = file[i..j];
            const temp = parsei32(num);

            const maybeEntry = threadMap[idx].getEntry(city);
            if (maybeEntry) |entry| {
                entry.value_ptr.*.count += 1;
                entry.value_ptr.*.total += temp;
                entry.value_ptr.*.max = @max(entry.value_ptr.*.max, temp);
                entry.value_ptr.*.min = @min(entry.value_ptr.*.min, temp);
            } else {
                const rec = Record{ .count = 1, .min = temp, .max = temp, .total = temp };
                const k = try allocator.alloc(u8, city.len);
                @memcpy(k, city);
                try threadMap[idx].put(k, rec);
            }
            city = undefined;
            num = undefined;
            i = j + 1;
            j = i; // j is inc'ed again at end of the loop , thus point to 2nd char in next line
        }
    }
    var processedCount: usize = 0;
    var iterator = threadMap[idx].iterator();
    while (iterator.next()) |entry| {
        processedCount += entry.value_ptr.*.count;
    }
}

fn mergeMaps(alloc: std.mem.Allocator, global: *TempMap, localMaps: []TempMap) !usize {
    var totalCount: usize = 0;
    for (localMaps) |m| {
        var iterator = m.iterator();
        while (iterator.next()) |kv| {
            const localRecord = kv.value_ptr.*;
            totalCount += localRecord.count;
            const maybeRecord = global.get(kv.key_ptr.*);
            if (maybeRecord) |globalRecord| {
                try global.put(kv.key_ptr.*, Record{ .count = globalRecord.count + localRecord.count, .max = @max(globalRecord.max, localRecord.max), .min = @min(globalRecord.min, localRecord.min), .total = globalRecord.total + localRecord.total });
            } else {
                const key = try alloc.alloc(u8, kv.key_ptr.*.len);
                @memcpy(key, kv.key_ptr.*);
                try global.put(key, kv.value_ptr.*);
            }
        }
    }
    return totalCount;
}

fn printGlobalMap(allocator: std.mem.Allocator, map: TempMap) !void {
    var keyList = SliceList.init(allocator);
    defer keyList.deinit();
    var keyIterator = map.keyIterator();
    while (keyIterator.next()) |key| {
        try keyList.append(key.*);
    }
    std.mem.sort([]const u8, keyList.items, {}, (struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }).lessThan);

    for (keyList.items) |key| {
        const record = map.get(key).?;
        const min = record.min;
        const minf: f32 = @floatFromInt(min);
        //const min: f32 = @as(f32, @floatFromInt(record.min) / 10.0);
        const maxf: f32 = @floatFromInt(record.max);
        const totalf: f32 = @floatFromInt(record.total);
        const countf: f32 = @floatFromInt(record.count);
        try writer.print("{s}: min: {d:3.1}, max:{d:3.1}, avg: {d:3.1}\n", .{ key, minf / 10.0, maxf / 10.0, totalf / (countf * 10.0) });
    }
}

fn parsei32(s: []const u8) i32 {
    var i: usize = 0;
    var num: i32 = 0;
    var mod: i32 = 1;
    while (i < s.len) : (i += 1) {
        if (s[i] == '-') {
            mod = -1;
        } else if (s[i] == '.') {} else {
            num = num * 10 + @as(i32, s[i] - '0');
        }
    }
    return num * mod;
}

test "test parsei32" {
    try std.testing.expect(parsei32(&"123.4".*) == @as(i32, 1234));
    try std.testing.expect(parsei32(&"23.4".*) == @as(i32, 234));
    try std.testing.expect(parsei32(&"3.4".*) == @as(i32, 34));
    try std.testing.expect(parsei32(&"3".*) == @as(i32, 3));
    try std.testing.expect(parsei32(&"-123.4".*) == @as(i32, -1234));
    try std.testing.expect(parsei32(&"-23.4".*) == @as(i32, -234));
}
