const std = @import("std");
const build_options = @import("build_options");

pub const string: []const u8 = build_options.version;

test "version string is non-empty" {
    try std.testing.expect(string.len > 0);
}
