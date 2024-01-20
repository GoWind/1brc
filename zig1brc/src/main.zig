const std = @import("std");
const File = std.fs.File;
const numWorkers = 12;
const SliceList = std.ArrayList([]const u8);
const writer = std.io.getStdOut().writer();
pub const Record = struct { city: []const u8, min: i32 = 0, max: i32 = 0, total: i64 = 0, count: u32 = 0 };
const RecordList = std.ArrayList(Record);

const TempMap = std.StringHashMap(Record);
var threadMap: [numWorkers]RecordList = undefined;
var threads: [numWorkers]std.Thread = undefined;
const NumList = std.ArrayList(usize);
const maxSize: usize = 1 << 14;
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
        threadMap[i] = RecordList.init(threadAllocator);
        const thread = try std.Thread.spawn(.{}, calculate, .{ i, workerSize, threadAllocator, mapping });
        threads[i] = thread;
    }
    i = 0;
    while (i < numWorkers) : (i += 1) {
        threads[i].join();
    }
    const totalProcessed = try mergeMaps(threadAllocator, &globalMap, &threadMap);
    // try printGlobalMapNoSort(&globalMap);
    try printGlobalMap(allocator, &globalMap);
    const end = std.time.nanoTimestamp();
    try writer.print("{} rows took {d} nanoseconds\n", .{ totalProcessed, end - start });
}

fn calculate(
    idx: usize,
    workerSize: u64,
    allocator: std.mem.Allocator,
    file: []u8,
) !void {
    var hashList = try NumList.initCapacity(allocator, maxSize);
    var indexList = try NumList.initCapacity(allocator, maxSize);
    hashList.appendNTimesAssumeCapacity(0, maxSize);
    indexList.appendNTimesAssumeCapacity(1 << 16, maxSize);

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
            var hashVal = hashSlice(city, maxSize);
            while (hashList.items[hashVal] != hashVal and hashList.items[hashVal] != 0) {
                hashVal = (hashVal + 1) & (maxSize - 1);
            }
            const entryIdx = indexList.items[hashVal];
            if (entryIdx == 1 << 16) {
                const cityNameForRec = try allocator.alloc(u8, city.len);
                @memcpy(cityNameForRec, city);
                const rec = Record{ .city = cityNameForRec, .count = 1, .min = temp, .max = temp, .total = temp };
                try threadMap[idx].append(rec);
                indexList.items[hashVal] = threadMap[idx].items.len - 1;
                hashList.items[hashVal] = hashVal;
            } else {
                threadMap[idx].items[entryIdx].count += 1;
                threadMap[idx].items[entryIdx].total += temp;
                threadMap[idx].items[entryIdx].max = @max(threadMap[idx].items[entryIdx].max, temp);
                threadMap[idx].items[entryIdx].min = @min(threadMap[idx].items[entryIdx].min, temp);
            }
            city = undefined;
            num = undefined;
            i = j + 1;
            j = i; // j is inc'ed again at end of the loop , thus point to 2nd char in next line
        }
    }
}

fn mergeMaps(alloc: std.mem.Allocator, global: *TempMap, localRecsList: []RecordList) !usize {
    var totalCount: usize = 0;
    for (localRecsList) |localRecs| {
        for (localRecs.items) |rec| {
            totalCount += rec.count;
            const maybeRecord = global.getEntry(rec.city);
            if (maybeRecord) |globalRecord| {
                globalRecord.value_ptr.*.count = globalRecord.value_ptr.*.count + rec.count;
                globalRecord.value_ptr.*.max = @max(globalRecord.value_ptr.*.max, rec.max);
                globalRecord.value_ptr.*.min = @min(globalRecord.value_ptr.*.min, rec.min);
                globalRecord.value_ptr.*.total = globalRecord.value_ptr.*.total + rec.total;
            } else {
                const keyCopy = try alloc.alloc(u8, rec.city.len);
                @memcpy(keyCopy, rec.city);
                try global.put(keyCopy, Record{ .city = keyCopy, .count = rec.count, .total = rec.total, .min = rec.min, .max = rec.max });
            }
        }
    }
    return totalCount;
}

fn printGlobalMap(allocator: std.mem.Allocator, map: *TempMap) !void {
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

fn printGlobalMapNoSort(map: *TempMap) !void {
   var mapvaliter = map.valueIterator();
   while (mapvaliter.next()) |record| {
        const min = record.min;
        const minf: f32 = @floatFromInt(min);
        const maxf: f32 = @floatFromInt(record.max);
        const totalf: f32 = @floatFromInt(record.total);
        const countf: f32 = @floatFromInt(record.count);
        try writer.print("{s}: min: {d:3.1}, max:{d:3.1}, avg: {d:3.1}\n", .{ record.city, minf / 10.0, maxf / 10.0, totalf / (countf * 10.0) });
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

fn hashSlice(data: []u8, totalSize: usize) usize {
    var k: usize = 0;
    var hash: usize = 0;
    while (k < data.len) : (k += 1) {
        hash = (hash * 31 + data[k]) & (totalSize - 1);
    }
    return hash;
}
