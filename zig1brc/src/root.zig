const std = @import("std");
const testing = std.testing;

export fn add(a: i32, b: i32) i32 {
    return a + b;
}

pub const Recrod = struct { min: f32, max: f32, total: f32, count: u32 };

test "basic add functionality" {
    try testing.expect(add(3, 7) == 10);
}
