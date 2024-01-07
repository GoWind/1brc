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
    const file = try std.fs.openFileAbsoluteZ(filepath, .{});
    defer file.close();
    const stat = try file.stat();
    std.debug.print("file size is {}\n", .{stat.size});
    const sizeFloat: f64 = @floatFromInt(stat.size);
    const workerSize: u64 = @intFromFloat(@floor(sizeFloat / numWorkers));
    std.debug.print("workerSize: {}\n", .{workerSize});
    var globalMap = TempMap.init(threadAllocator);
    var i: usize = 0;
    while (i < numWorkers) : (i += 1) {
        threadMap[i] = TempMap.init(threadAllocator);
        const thread = try std.Thread.spawn(.{}, calculate, .{ i, workerSize, threadAllocator, filepath });
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
    filepath: [*:0]u8,
) !void {
    var buffer = [_]u8{'a'} ** 80000;
    const waterMarkSize: usize = 80000;
    const slice = buffer[0..100];
    const View = struct { slice: []u8, len: usize };
    const file = try std.fs.openFileAbsoluteZ(filepath, .{});
    const stat = try file.stat();
    defer file.close();
    var startOffset = idx * workerSize;
    var endOffset = (idx + 1) * workerSize - 1;
    if (startOffset > 0) {
        const prev = startOffset - 1;
        try file.seekTo(prev);
        const read = try file.readAll(slice);
        if (read == 0) {
            @panic("failed to read from starting offset");
        }
        if (buffer[0] != '\n') {
            var i: usize = 1;
            while (i < read) : (i += 1) {
                if (buffer[i] == '\n') {
                    startOffset += i;
                    break;
                }
            }
        }
    }
    if (endOffset < stat.size) {
        try file.seekTo(endOffset);
        const read = try file.readAll(slice);
        if (read == 0) {
            @panic("failed to read from starting offset");
        }
        var i: usize = 0;
        while (i < read) : (i += 1) {
            if (buffer[i] == '\n') {
                endOffset += i;
                break;
            }
        }
    }
    try file.seekTo(startOffset);
    const totalSizePerWorker = endOffset - startOffset + 1;
    var bytesRead: usize = 0;
    @memset(&buffer, 0);
    var view = View{ .slice = &buffer, .len = 0 };

    while (bytesRead < totalSizePerWorker) {
        const bufferCapacity = waterMarkSize - view.len;
        const bytesToRead = @min(bufferCapacity, totalSizePerWorker - bytesRead);
        // std.debug.print("readSoFar {} ,,, bytesToRead {} and view.len {}\n", .{ bytesRead, bytesToRead, view.len });
        const read = try file.read(buffer[view.len .. view.len + bytesToRead]);
        if (read == 0) @panic("i dunno what to do here");
        const maybeNextLineIdx = findLastLine(buffer[0 .. view.len + read]);

        if (maybeNextLineIdx) |nextLineIdx| {
            try processRows(allocator, &threadMap[idx], buffer[0..nextLineIdx]);
            const remaining = (view.len + read) - nextLineIdx;
            @memcpy(buffer[0 .. view.len + read - nextLineIdx], buffer[nextLineIdx .. view.len + read]);
            view.len = remaining;
        } else {
            try processRows(allocator, &threadMap[idx], buffer[0 .. view.len + read]);
            view.len = 0;
        }

        bytesRead += read;
    }
    std.debug.print("thread {}: adjusted start {} and end {}\n", .{ idx, startOffset, endOffset });
    var processedCount: usize = 0;
    var iterator = threadMap[idx].iterator();
    while (iterator.next()) |entry| {
        processedCount += entry.value_ptr.*.count;
    }
    std.debug.print("total seen count is {}\n", .{processedCount});
}

// if data is a "perfect" set of rows, all of ending in \n, return null
// return index of first character in the last line that doesnt end with \n otherwise
fn findLastLine(data: []u8) ?usize {
    var i: usize = data.len - 1;

    while (i > 0 and data[i] != '\n') : (i -= 1) {}

    // If the last line doesn't end with a newline character,
    // return the index of the first character of the last line.
    if (i + 1 < data.len and data[i + 1] != '\n') {
        return i + 1;
    }

    // If the last line ends with a newline character, return None.
    return null;
}

fn processRows(alloc: std.mem.Allocator, t: *TempMap, data: []u8) !void {
    var iterator = std.mem.splitScalar(u8, data, '\n');
    while (iterator.next()) |row| {
        if (row.len == 0) {
            continue;
        }
        var splitter: ?usize = null;
        for (row, 0..) |c, i| {
            if (c == ';') {
                splitter = i;
                break;
            }
        }
        const city = row[0..splitter.?];
        const temp = try std.fmt.parseFloat(f32, row[splitter.? + 1 ..]);
        const maybeEntry = t.get(city);
        if (maybeEntry) |entry| {
            var entryCopy = entry;
            entryCopy.total += temp;
            entryCopy.count = entry.count + 1;
            if (entry.min > temp) {
                entryCopy.min = temp;
            }
            if (entry.max < temp) {
                entryCopy.max = temp;
            }
            t.putAssumeCapacity(city, entryCopy);
        } else {
            const newRecord = Record{ .count = 1, .total = temp, .max = temp, .min = temp };
            const newKey = try alloc.alloc(u8, city.len);
            @memcpy(newKey, city);
            try t.put(newKey, newRecord);
        }
    }
}

test "splitter tests" {
    const m = "abced\n12345\n23456";
    _ = m;
    const n = "cceeam\nbbddee\n";

    var iterator = std.mem.splitScalar(u8, n, '\n');
    while (iterator.next()) |i| {
        std.debug.print("{s}", .{i});
        try std.testing.exp(i.len == 6);
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
        std.debug.print("{s}: min: {d}, max:{d}, avg: {d}\n", .{ kv.key_ptr.*, record.min, record.max, @as(f32, record.total / @as(f32, @floatFromInt(record.count))) });
    }
}
