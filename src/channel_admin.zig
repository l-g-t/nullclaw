const std = @import("std");
const config_types = @import("config_types.zig");
const health = @import("health.zig");

pub const ChannelAccountSummary = struct {
    type: []const u8,
    account_id: []const u8,
    configured: bool = true,
    status: []const u8,
};

pub const ChannelAccountDetail = struct {
    account_id: []const u8,
    configured: bool = true,
};

pub const ChannelTypeDetail = struct {
    type: []const u8,
    status: []const u8,
    accounts: []const ChannelAccountDetail,

    pub fn deinit(self: *ChannelTypeDetail, allocator: std.mem.Allocator) void {
        if (self.accounts.len > 0) allocator.free(self.accounts);
        self.* = undefined;
    }
};

const ChannelTypeEntry = struct {
    field: []const u8,
    type_name: []const u8,
};

pub const channel_types = [_]ChannelTypeEntry{
    .{ .field = "telegram", .type_name = "telegram" },
    .{ .field = "discord", .type_name = "discord" },
    .{ .field = "slack", .type_name = "slack" },
    .{ .field = "imessage", .type_name = "imessage" },
    .{ .field = "matrix", .type_name = "matrix" },
    .{ .field = "mattermost", .type_name = "mattermost" },
    .{ .field = "whatsapp", .type_name = "whatsapp" },
    .{ .field = "teams", .type_name = "teams" },
    .{ .field = "irc", .type_name = "irc" },
    .{ .field = "lark", .type_name = "lark" },
    .{ .field = "dingtalk", .type_name = "dingtalk" },
    .{ .field = "wechat", .type_name = "wechat" },
    .{ .field = "wecom", .type_name = "wecom" },
    .{ .field = "signal", .type_name = "signal" },
    .{ .field = "email", .type_name = "email" },
    .{ .field = "line", .type_name = "line" },
    .{ .field = "qq", .type_name = "qq" },
    .{ .field = "onebot", .type_name = "onebot" },
    .{ .field = "maixcam", .type_name = "maixcam" },
    .{ .field = "web", .type_name = "web" },
    .{ .field = "max", .type_name = "max" },
    .{ .field = "external", .type_name = "external" },
};

pub fn isKnownType(type_name: []const u8) bool {
    inline for (channel_types) |entry| {
        if (std.mem.eql(u8, entry.type_name, type_name)) return true;
    }
    return false;
}

pub fn collectConfiguredChannels(
    allocator: std.mem.Allocator,
    channels: *const config_types.ChannelsConfig,
    snapshot: health.HealthSnapshot,
) ![]ChannelAccountSummary {
    var items = std.ArrayList(ChannelAccountSummary).empty;
    errdefer items.deinit(allocator);

    inline for (channel_types) |entry| {
        try appendChannelAccounts(allocator, &items, @field(channels, entry.field), entry.type_name, snapshot);
    }

    return try items.toOwnedSlice(allocator);
}

pub fn readChannelTypeDetail(
    allocator: std.mem.Allocator,
    channels: *const config_types.ChannelsConfig,
    snapshot: health.HealthSnapshot,
    type_name: []const u8,
) !?ChannelTypeDetail {
    inline for (channel_types) |entry| {
        if (std.mem.eql(u8, entry.type_name, type_name)) {
            var accounts = std.ArrayList(ChannelAccountDetail).empty;
            errdefer accounts.deinit(allocator);

            const slice = @field(channels, entry.field);
            for (slice) |item| {
                try accounts.append(allocator, .{
                    .account_id = accountId(item),
                });
            }

            return .{
                .type = entry.type_name,
                .status = healthStatus(snapshot, entry.type_name),
                .accounts = try accounts.toOwnedSlice(allocator),
            };
        }
    }

    return null;
}

fn appendChannelAccounts(
    allocator: std.mem.Allocator,
    items: *std.ArrayList(ChannelAccountSummary),
    slice: anytype,
    type_name: []const u8,
    snapshot: health.HealthSnapshot,
) !void {
    for (slice) |item| {
        try items.append(allocator, .{
            .type = type_name,
            .account_id = accountId(item),
            .status = healthStatus(snapshot, type_name),
        });
    }
}

fn accountId(item: anytype) []const u8 {
    if (comptime @hasField(@TypeOf(item), "account_id")) {
        return item.account_id;
    }
    return "default";
}

fn healthStatus(snapshot: health.HealthSnapshot, type_name: []const u8) []const u8 {
    for (snapshot.components) |entry| {
        if (std.mem.eql(u8, entry.name, type_name)) return entry.health.status;
    }
    return "unknown";
}

test "collectConfiguredChannels reports configured accounts and health by type" {
    const allocator = std.testing.allocator;
    const telegram_accounts = [_]config_types.TelegramConfig{
        .{ .account_id = "main", .bot_token = "tok-main" },
        .{ .account_id = "backup", .bot_token = "tok-backup" },
    };
    const discord_accounts = [_]config_types.DiscordConfig{
        .{ .account_id = "guild-a", .token = "disc-token" },
    };
    const channels = config_types.ChannelsConfig{
        .telegram = &telegram_accounts,
        .discord = &discord_accounts,
    };

    health.reset();
    health.markComponentOk("telegram");
    health.markComponentError("discord", "gateway down");

    const snapshot = try health.snapshot(allocator);
    defer snapshot.deinit(allocator);

    const items = try collectConfiguredChannels(allocator, &channels, snapshot);
    defer allocator.free(items);

    try std.testing.expectEqual(@as(usize, 3), items.len);
    try std.testing.expectEqualStrings("telegram", items[0].type);
    try std.testing.expectEqualStrings("main", items[0].account_id);
    try std.testing.expectEqualStrings("ok", items[0].status);
    try std.testing.expectEqualStrings("backup", items[1].account_id);
    try std.testing.expectEqualStrings("discord", items[2].type);
    try std.testing.expectEqualStrings("error", items[2].status);
}

test "readChannelTypeDetail returns empty accounts for known unconfigured type" {
    const allocator = std.testing.allocator;
    const channels = config_types.ChannelsConfig{};

    health.reset();
    const snapshot = try health.snapshot(allocator);
    defer snapshot.deinit(allocator);

    var detail = (try readChannelTypeDetail(allocator, &channels, snapshot, "discord")).?;
    defer detail.deinit(allocator);

    try std.testing.expectEqualStrings("discord", detail.type);
    try std.testing.expectEqualStrings("unknown", detail.status);
    try std.testing.expectEqual(@as(usize, 0), detail.accounts.len);
}

test "readChannelTypeDetail returns null for unknown type" {
    const allocator = std.testing.allocator;
    const channels = config_types.ChannelsConfig{};

    health.reset();
    const snapshot = try health.snapshot(allocator);
    defer snapshot.deinit(allocator);

    try std.testing.expect((try readChannelTypeDetail(allocator, &channels, snapshot, "nonexistent")) == null);
}
