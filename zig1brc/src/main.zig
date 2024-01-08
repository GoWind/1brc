const std = @import("std");
const File = std.fs.File;
const numWorkers = 12;
pub const Record = struct { min: f32 = 0, max: f32 = 0, total: f32 = 0, count: u32 = 0 };
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
    printGlobalMap(globalMap);
    const end = std.time.nanoTimestamp();
    std.debug.print("{} rows took {d} nanoseconds\n", .{ totalProcessed, end - start });
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
            const temp = try std.fmt.parseFloat(f32, num);

            const maybeEntry = threadMap[idx].getEntry(city);
            if (maybeEntry) |entry| {
                entry.value_ptr.*.count += 1;
                entry.value_ptr.*.total += temp;
                entry.value_ptr.*.max = @max(entry.value_ptr.*.max, temp);
                entry.value_ptr.*.min = @max(entry.value_ptr.*.max, temp);
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

fn printGlobalMap(map: TempMap) void {
    var iterator = map.iterator();
    while (iterator.next()) |kv| {
        const record = kv.value_ptr.*;
        std.debug.print("{s}: min: {d:3.1}, max:{d:3.1}, avg: {d:3.1}\n", .{ kv.key_ptr.*, record.min, record.max, @as(f32, record.total / @as(f32, @floatFromInt(record.count))) });
    }
}
