//! Hierarchical summarization — multi-level L1, L2, ... summaries from messages.
//!
//! Monitors workspace/messages/ for new message files, creates summaries when
//! character thresholds are exceeded, and stores summaries in workspace/memory/L*.
//!
//! State is persisted to workspace/.hierarchical_summarizer_state.json for crash
//! recovery and incremental operation.

const std = @import("std");
const fs_compat = @import("../../fs_compat.zig");
const json = std.json;
const Provider = @import("../providers/root.zig").Provider;

const MAX_LEVELS_DEFAULT: u8 = 5;
const CHUNK_SIZE_DEFAULT: usize = 200_000;
const OVERLAP_ENTRIES_DEFAULT: usize = 1;

pub const HierarchicalSummarizerConfig = struct {
    enabled: bool = false,
    chunk_size_chars: usize = CHUNK_SIZE_DEFAULT,
    overlap_entries: usize = OVERLAP_ENTRIES_DEFAULT,
    max_levels: u8 = MAX_LEVELS_DEFAULT,
    model: ?[]const u8 = null, // null = use default model
};

/// Pending item representing either a message file or a summary file at some level.
pub const PendingItem = struct {
    file_path: []const u8,
    timestamp: i64,           // For messages: message time; For summaries: start of source range
    source_end: i64,          // For messages: same as timestamp; For summaries: end of source range
    content_length: usize,
    key: []const u8,          // e.g., "L1:20260322T100000_20260322T113000"
    level: u8,                // The level this item belongs to (0 = messages, 1+ = summaries)

    pub fn deinit(self: *PendingItem, allocator: std.mem.Allocator) void {
        allocator.free(self.file_path);
        allocator.free(self.key);
    }
};

/// State for a single summarization level.
pub const LevelState = struct {
    pending_items: std.ArrayListUnmanaged(PendingItem) = .empty,
    total_chars: usize = 0,

    pub fn deinit(self: *LevelState, allocator: std.mem.Allocator) void {
        for (self.pending_items.items) |*item| {
            item.deinit(allocator);
        }
        self.pending_items.deinit(allocator);
    }
};

/// Persisted state for the entire hierarchical summarizer.
pub const State = struct {
    version: u32 = 1,
    last_scanned_timestamp: i64 = 0,
    levels: std.ArrayListUnmanaged(LevelState) = .empty,

    pub fn deinit(self: *State, allocator: std.mem.Allocator) void {
        for (self.levels.items) |*level| {
            level.deinit(allocator);
        }
        self.levels.deinit(allocator);
    }
};

/// HierarchicalSummarizer performs automatic multi-level summarization.
pub const HierarchicalSummarizer = struct {
    allocator: std.mem.Allocator,
    provider: Provider,
    model_name: []const u8,
    chunk_size: usize,
    overlap_entries: usize,
    max_levels: u8,
    workspace_dir: []const u8,
    messages_dir: []const u8,
    memory_dir: []const u8,
    state: State,
    state_file_path: []const u8,

    const Self = @This();

    /// Initialize summarizer, loading or creating state.
    pub fn init(
        allocator: std.mem.Allocator,
        workspace_dir: []const u8,
        provider: Provider,
        model_name: []const u8,
        cfg: HierarchicalSummarizerConfig,
    ) !Self {
        const messages_dir = try std.fs.path.join(allocator, &.{ workspace_dir, "messages" });
        errdefer allocator.free(messages_dir);

        const memory_dir = try std.fs.path.join(allocator, &.{ workspace_dir, "memory" });
        errdefer allocator.free(memory_dir);

        const state_file_path = try std.fs.path.join(allocator, &.{ workspace_dir, ".hierarchical_summarizer_state.json" });
        errdefer allocator.free(state_file_path);

        var state = State{};
        errdefer state.deinit(allocator);

        // Try to load existing state
        if (std.fs.cwd().access(state_file_path, .{})) |_| {
            const file = std.fs.cwd().openFile(state_file_path, .{}) catch |err| {
                std.debug.print("Failed to open state file: {s}\n", .{@errorName(err)});
                // Continue with empty state
            } else {
                defer file.close();
                const content = file.readToEndAlloc(allocator, 1024 * 1024) catch |err| {
                    std.debug.print("Failed to read state file: {s}\n", .{@errorName(err)});
                } else {
                    defer allocator.free(content);
                    state = try parseState(allocator, content);
                }
            }
        } else |_| {}

        // Ensure state has correct number of levels
        try ensureLevels(&state, cfg.max_levels, allocator);

        return Self{
            .allocator = allocator,
            .provider = provider,
            .model_name = model_name,
            .chunk_size = cfg.chunk_size_chars,
            .overlap_entries = cfg.overlap_entries,
            .max_levels = cfg.max_levels,
            .workspace_dir = try allocator.dupe(u8, workspace_dir),
            .messages_dir = messages_dir,
            .memory_dir = memory_dir,
            .state = state,
            .state_file_path = state_file_path,
        };
    }

    /// Deinitialize summarizer and save state.
    pub fn deinit(self: *Self) void {
        self.saveState() catch |err| {
            std.debug.print("Failed to save summarizer state: {s}\n", .{@errorName(err)});
        };
        self.state.deinit(self.allocator);
        self.allocator.free(self.workspace_dir);
        self.allocator.free(self.messages_dir);
        self.allocator.free(self.memory_dir);
        self.allocator.free(self.state_file_path);
    }

    /// Scan for new messages and process summarization as needed.
    pub fn scanAndProcess(self: *Self) !void {
        // First, scan messages directory for new files
        try scanNewMessages(self);

        // Then, check all levels for threshold exceedance
        var level: u8 = 0;
        while (level < self.state.levels.items.len) : (level += 1) {
            while (try shouldSummarizeLevel(self, level)) {
                try summarizeLevel(self, level);
            }
        }
    }

    /// Scan messages directory and add new files to level 0 pending.
    fn scanNewMessages(self: *Self) !void {
        const last_scanned = self.state.last_scanned_timestamp;

        // Walk the messages directory
        const messages_dir_obj = std.fs.cwd().openDir(self.messages_dir, .{ .iterate = true }) catch |err| {
            if (err == error.PathNotFound) return; // No messages yet
            return err;
        };
        defer messages_dir_obj.close();

        var walker = try std.fs.WalkIterator.init(messages_dir_obj, &.{});
        defer walker.deinit();

        while (try walker.next()) |entry| {
            if (entry.kind != .file or !std.mems.endsWith(u8, entry.path, ".md")) {
                continue;
            }

            // Skip hidden files
            if (std.mem.lastIndexOf(u8, entry.path, "/")) |slash_idx| {
                const filename = entry.path[slash_idx + 1 ..];
                if (filename[0] == '.') continue;
            }

            const full_path = try std.fs.path.join(self.allocator, &.{ self.messages_dir, entry.path });
            errdefer self.allocator.free(full_path);

            // Get file timestamp from frontmatter or mtime
            const file_info = fs_compat.statFile(self.allocator, full_path) catch continue;
            const mtime_sec = @as(i64, @intCast(@divTrunc(file_info.mtime, std.time.ns_per_s)));

            // Only process if newer than last scanned
            if (mtime_sec > last_scanned) {
                const timestamp = try extractMessageTimestamp(full_path) catch mtime_sec;
                const content_len = file_info.size;

                // Generate key based on timestamp
                const epoch_secs = @as(u64, @intCast(timestamp));
                const es = std.time.epoch.EpochSeconds{ .secs = epoch_secs };
                const day = es.getEpochDay().calculateYearDay();
                const md = day.calculateMonthDay();

                const date_str = try std.fmt.allocPrint(self.allocator, "{d:0>4}-{d:0>2}-{d:0>2}", .{
                    day.year,
                    @intFromEnum(md.month),
                    md.day_index + 1,
                });
                errdefer self.allocator.free(date_str);

                const hour = @as(u8, @intCast((epoch_secs % 86400) / 3600));
                const minute = @as(u8, @intCast((epoch_secs % 3600) / 60));
                const second = @as(u8, @intCast(epoch_secs % 60));

                const time_str = try std.fmt.allocPrint(self.allocator, "{d:0>2}{d:0>2}{d:0>2}", .{
                    hour, minute, second
                });
                errdefer self.allocator.free(time_str);

                const key = try std.fmt.allocPrint(self.allocator, "L0:{s}T{s}", .{ date_str, time_str });
                errdefer self.allocator.free(key);

                const item = PendingItem{
                    .file_path = full_path,
                    .timestamp = timestamp,
                    .source_end = timestamp,
                    .content_length = content_len,
                    .key = key,
                    .level = 0,
                };

                try self.addPendingItem(0, item);
            }
        }

        // Update last scanned timestamp to now
        self.state.last_scanned_timestamp = std.time.timestamp();
    }

    /// Check if a level should be summarized (total_chars >= chunk_size).
    fn shouldSummarizeLevel(self: *Self, level: u8) !bool {
        if (level >= self.state.levels.items.len) return false;
        const level_state = self.state.levels.items[level];
        return level_state.total_chars >= self.chunk_size;
    }

    /// Create a summary for the given level by consuming pending items.
    fn summarizeLevel(self: *Self, level: u8) !void {
        if (level >= self.state.levels.items.len) return;

        const level_state = self.state.levels.items[level];
        const items = level_state.pending_items.items;

        if (items.len == 0) return;

        // Determine how many items to consume (retain overlap at end)
        const overlap = @min(self.overlap_entries, items.len);
        const consume_count = if (items.len > overlap) items.len - overlap else 0;

        if (consume_count == 0) return; // Not enough items to summarize yet

        const to_consume = items[0..consume_count];

        // Build the summary from these items
        const summary = try self.createSummary(level, to_consume);
        errdefer self.allocator.free(summary);

        // Determine key for this summary (timestamp range covering all consumed items)
        const start_ts = to_consume[0].timestamp;
        const end_ts = to_consume[consume_count - 1].source_end;

        const summary_key = try self.makeSummaryKey(level + 1, start_ts, end_ts);
        errdefer self.allocator.free(summary_key);

        // Write summary file to appropriate directory
        const summary_file_path = try self.writeSummaryFile(level + 1, summary_key, summary);
        errdefer self.allocator.free(summary_file_path);

        // Add to next level pending (if not max levels)
        if (level + 1 < self.max_levels) {
            const next_level_item = PendingItem{
                .file_path = summary_file_path,
                .timestamp = start_ts,
                .source_end = end_ts,
                .content_length = summary.len,
                .key = summary_key,
                .level = @intCast(level + 1),
            };
            try self.addPendingItem(level + 1, next_level_item);
        }

        // Remove consumed items from pending queue (but keep overlap)
        for (to_consume) |item| {
            self.state.levels.items[level].total_chars -= item.content_length;
        }
        // Shift array to remove consumed items, keeping overlap at front
        std.mem.copy(PendingItem, level_state.pending_items.items[0..overlap], items[consume_count..]);
        level_state.pending_items.items.len = overlap;

        // Save state after successful summarization
        try self.saveState();
    }

    /// Create a summary text from a batch of items using LLM.
    fn createSummary(self: *Self, level: u8, items: []const PendingItem) ![]u8 {
        // Build prompt
        const prompt = try buildSummaryPrompt(self.allocator, level, items);
        errdefer self.allocator.free(prompt);

        // Call LLM
        const summary = try self.callLLM(prompt);

        return summary;
    }

    /// Build summarization prompt for given items.
    fn buildSummaryPrompt(allocator: std.mem.Allocator, level: u8, items: []const PendingItem) ![]u8 {
        var buf = std.ArrayListUnmanaged(u8){};
        errdefer buf.deinit(allocator);

        const prompt_intro = if (level == 0)
            "Summarize the following conversation messages concisely, preserving key facts, " ++
            "decisions, and important details. Include who said what (user/assistant). " ++
            "The summary should be significantly shorter than the input.\n\n"
        else
            "Combine the following summaries into a higher-level overview, eliminating " ++
            "redundancy while preserving the most important information.\n\n";

        try buf.appendSlice(allocator, prompt_intro);
        try buf.appendSlice(allocator, "--- BEGIN INPUT ---\n");

        for (items) |item| {
            // Read content
            const content = try readItemContent(allocator, item) catch continue;
            defer allocator.free(content);

            // Format header with timestamp range
            const start_dt = try formatTimestamp(allocator, item.timestamp);
            errdefer allocator.free(start_dt);
            const end_dt = try formatTimestamp(allocator, item.source_end);
            errdefer allocator.free(end_dt);

            if (level == 0) {
                try buf.appendSlice(allocator, "[");
                try buf.appendSlice(allocator, start_dt);
                try buf.appendSlice(allocator, "] ");
                // For messages, content already includes role prefix from file parsing
                try buf.appendSlice(allocator, content);
            } else {
                try buf.appendSlice(allocator, "[L");
                try std.fmt.formatInt(level, buf.writer(allocator), 10);
                try buf.appendSlice(allocator, " ");
                try buf.appendSlice(allocator, start_dt);
                try buf.appendSlice(allocator, "-");
                try buf.appendSlice(allocator, end_dt);
                try buf.appendSlice(allocator, "]: ");
                try buf.appendSlice(allocator, content);
            }
            try buf.appendSlice(allocator, "\n");
        }

        try buf.appendSlice(allocator, "--- END INPUT ---\n");

        return buf.toOwnedSlice(allocator);
    }

    /// Read the content of an item (message or summary).
    fn readItemContent(allocator: std.mem.Allocator, item: PendingItem) ![]u8 {
        const file = std.fs.cwd().openFile(item.file_path, .{}) catch return error.FileNotFound;
        defer file.close();

        // For message files: parse frontmatter and return body
        // For summary files: parse as markdown entry and return content after ": "
        const ext = std.fs.path.extension(item.file_path);
        if (std.mem.eql(u8, ext, ".md")) {
            // Check if it's a summary file (in L* directory) or a message file
            const is_summary = std.mem.indexOf(u8, item.file_path, "/memory/L") != null;

            const content = file.readToEndAlloc(allocator, 1024 * 1024) catch return error.ReadFailed;
            errdefer allocator.free(content);

            if (is_summary) {
                // Parse as single markdown entry: "- **Lx:start_end**: summary text"
                // Extract the content after the colon
                if (std.mem.indexOf(u8, content, ": ")) |colon_idx| {
                    const after_colon = content[colon_idx + 2..];
                    const trimmed = std.mem.trim(u8, after_colon, " \t\r\n");
                    return allocator.dupe(u8, trimmed);
                }
                return allocator.dupe(u8, content);
            } else {
                // Message file: has YAML frontmatter, extract body after "---"
                if (std.mem.indexOf(u8, content, "---")) |first_dash| {
                    if (std.mem.indexOf(u8, content[first_dash + 3..], "---")) |second_dash| {
                        const body_start = first_dash + 3 + second_dash + 3;
                        const body = content[body_start..];
                        const trimmed = std.mem.trim(u8, body, " \t\r\n");
                        return allocator.dupe(u8, trimmed);
                    }
                }
                // Fallback: return full content
                return allocator.dupe(u8, content);
            }
        }

        // Unknown file type
        return error.UnsupportedFormat;
    }

    /// Format timestamp as YYYY-MM-DDTHH:MM:SSZ
    fn formatTimestamp(allocator: std.mem.Allocator, ts: i64) ![]u8 {
        const epoch_secs = @as(u64, @intCast(ts));
        const es = std.time.epoch.EpochSeconds{ .secs = epoch_secs };
        const day = es.getEpochDay().calculateYearDay();
        const md = day.calculateMonthDay();

        const year = day.year;
        const month = @intFromEnum(md.month);
        const day_index = md.day_index + 1;

        const day_secs = epoch_secs % 86400;
        const hour = @as(u8, @intCast(day_secs / 3600));
        const minute = @as(u8, @intCast((day_secs % 3600) / 60));
        const second = @as(u8, @intCast(day_secs % 60));

        return std.fmt.allocPrint(allocator, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
            year, month, day_index, hour, minute, second,
        });
    }

    /// Call LLM with the given prompt, returning the summary text.
    fn callLLM(self: *Self, prompt: []const u8) ![]u8 {
        const model = if (self.model_name.len > 0) self.model_name else self.provider.getName();
        const response = try self.provider.chatWithSystem(
            self.allocator,
            null, // no system prompt for summarization
            prompt,
            model,
            0.0, // temperature = 0 for determinism
        );
        // Response is owned by caller? Provider returns allocator.alloc'ed string that we must free
        // We'll transfer ownership to caller
        return response;
    }

    /// Write summary to a markdown file and return the path.
    fn writeSummaryFile(self: *Self, level: u8, key: []const u8, summary: []const u8) ![]u8 {
        const level_dir = try std.fmt.allocPrint(self.allocator, "memory/L{d}", .{level});
        errdefer self.allocator.free(level_dir);

        // Ensure directory exists
        const full_level_dir = try std.fs.path.join(self.allocator, &.{ self.workspace_dir, level_dir });
        errdefer self.allocator.free(full_level_dir);
        std.fs.makeDirAbsolute(full_level_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        // Generate filename from key: Lx_YYYYMMDDTHHMMSS_YYYYMMDDTHHMMSS.md
        const filename = try std.fmt.allocPrint(self.allocator, "{s}.md", .{key});
        errdefer self.allocator.free(filename);

        const full_path = try std.fs.path.join(self.allocator, &.{ full_level_dir, filename });
        errdefer self.allocator.free(full_path);

        // Write file with single entry: "- **key**: summary"
        const entry_text = try std.fmt.allocPrint(self.allocator, "- **{s}**: {s}", .{ key, summary });
        errdefer self.allocator.free(entry_text);

        try std.fs.cwd().writeFile(.{
            .data = entry_text,
            .sub_path = full_path,
        });

        return full_path;
    }

    /// Generate a summary key like "L1:20260322T100000_20260322T113000"
    fn makeSummaryKey(self: *Self, level: u8, start_ts: i64, end_ts: i64) ![]u8 {
        const start_dt = try formatTimestamp(self.allocator, start_ts);
        errdefer self.allocator.free(start_dt);
        const end_dt = try formatTimestamp(self.allocator, end_ts);
        errdefer self.allocator.free(end_dt);

        // Remove colons and dashes to make filename-safe
        const start_clean = try removeTimestampSeparators(self.allocator, start_dt);
        errdefer self.allocator.free(start_clean);
        const end_clean = try removeTimestampSeparators(self.allocator, end_dt);
        errdefer self.allocator.free(end_clean);

        return std.fmt.allocPrint(self.allocator, "L{d}:{s}_{s}", .{ level, start_clean, end_clean });
    }

    /// Remove separators from ISO timestamp for filename (e.g., "2026-03-22T10:00:00Z" -> "20260322T100000Z")
    fn removeTimestampSeparators(allocator: std.mem.Allocator, ts: []const u8) ![]u8 {
        var out = std.ArrayListUnmanaged(u8){};
        for (ts) |c| {
            if (c != '-' and c != ':' and c != 'T' and c != 'Z' and c != '+' and c != '.') {
                try out.append(allocator, c);
            } else {
                switch (c) {
                    '-' => {},
                    ':' => {},
                    'T' => try out.append(allocator, 'T'),
                    'Z' => try out.append(allocator, 'Z'),
                    '+' => try out.append(allocator, '+'),
                    '.' => {},
                    else => {},
                }
            }
        }
        return out.toOwnedSlice(allocator);
    }

    /// Add an item to the pending queue for a level, updating total_chars.
    fn addPendingItem(self: *Self, level: u8, item: PendingItem) !void {
        // Ensure levels array has enough capacity
        while (self.state.levels.items.len <= level) {
            try self.state.levels.append(self.allocator, .{});
        }

        const level_state = &self.state.levels.items[level];
        try level_state.pending_items.append(self.allocator, item);
        level_state.total_chars += item.content_length;
    }

    /// Save state to JSON file atomically.
    fn saveState(self: *Self) !void {
        const json_str = try json.encodeAlloc(self.allocator, self.state, .{});
        errdefer self.allocator.free(json_str);

        // Write to temp file then rename
        const temp_path = try std.fmt.allocPrint(self.allocator, "{s}.tmp", .{self.state_file_path});
        errdefer self.allocator.free(temp_path);

        try std.fs.cwd().writeFile(.{
            .data = json_str,
            .sub_path = temp_path,
        });

        try std.fs.renameAbsolute(temp_path, self.state_file_path, .{});
        self.allocator.free(temp_path);
    }

    /// Extract message timestamp from frontmatter, or return error.
    fn extractMessageTimestamp(file_path: []const u8) !i64 {
        const file = std.fs.cwd().openFile(file_path, .{}) catch return error.OpenFailed;
        defer file.close();

        const content = try file.readToEndAlloc(std.testing.allocator, 1024 * 1024);
        defer std.testing.allocator.free(content);

        // Parse YAML frontmatter
        var lines = std.mem.splitScalar(u8, content, '\n');
        var in_frontmatter = false;
        var found_timestamp = false;
        var timestamp_str: []const u8 = "";

        while (lines.next()) |line| {
            if (std.mem.eql(u8, line, "---")) {
                if (!in_frontmatter) {
                    in_frontmatter = true;
                } else {
                    break; // End of frontmatter
                }
                continue;
            }
            if (!in_frontmatter) continue;

            if (std.mem.startsWith(u8, line, "timestamp:")) {
                const value = std.mem.trim(u8, line["timestamp:".len..], " \t\r\"");
                timestamp_str = value;
                found_timestamp = true;
                break;
            }
        }

        if (!found_timestamp) return error.NoTimestamp;

        // Parse ISO 8601 timestamp (simplified: expect format like 2026-03-22T10:00:00.000000Z)
        const iso_re =
            \\(?<year>\d{4})-(?<month>\d{2})-(?<day>\d{2})
            \\T
            \\(?<hour>\d{2}):(?<minute>\d{2}):(?<second>\d{2})
            \\.(?<micro>\d{6})Z
        ;
        // For simplicity, use manual parsing
        if (timestamp_str.len < 20) return error.InvalidTimestamp;

        const year = std.fmt.parseInt(i16, timestamp_str[0..4], 10) catch return error.InvalidTimestamp;
        const month = std.fmt.parseInt(u8, timestamp_str[5..7], 10) catch return error.InvalidTimestamp;
        const day = std.fmt.parseInt(u8, timestamp_str[8..10], 10) catch return error.InvalidTimestamp;
        const hour = std.fmt.parseInt(u8, timestamp_str[11..13], 10) catch return error.InvalidTimestamp;
        const minute = std.fmt.parseInt(u8, timestamp_str[14..16], 10) catch return error.InvalidTimestamp;
        const second = std.fmt.parseInt(u8, timestamp_str[17..19], 10) catch return error.InvalidTimestamp;

        const epoch_day = ymdToEpochDays(year, month, day);
        var total_seconds: i64 = epoch_day * 86400;
        total_seconds += @as(i64, @intCast(hour)) * 3600;
        total_seconds += @as(i64, @intCast(minute)) * 60;
        total_seconds += @as(i64, @intCast(second));

        return total_seconds;
    }

    /// Convert Gregorian Y-M-D to epoch days.
    fn ymdToEpochDays(year: i16, month: u8, day: u8) i64 {
        var y = @as(i32, @intCast(year));
        var m = @as(i32, @intCast(month));
        if (m <= 2) {
            y -= 1;
            m += 12;
        }
        const era = if (y >= 0) @divTrunc(y, 400) else @divTrunc(y - 399, 400);
        const yoe = @as(u32, @intCast(y - era * 400));
        const doy = @as(u32, @intCast(@divTrunc(153 * @as(i32, m - 3) + 2, 5))) + @as(u32, @intCast(day)) - 1;
        const doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        const days = era * 146097 + @as(i64, @intCast(doe)) - 719468;
        return days;
    }

    /// Ensure the state has at least `min_levels` levels initialized.
    fn ensureLevels(state: *State, min_levels: u8, allocator: std.mem.Allocator) !void {
        while (state.levels.items.len < min_levels) {
            try state.levels.append(allocator, .{});
        }
    }
};

// ── State Parsing ─────────────────────────────────────────────────────

fn parseState(allocator: std.mem.Allocator, content: []const u8) !State {
    var parsed = json.parse(json.Value, allocator, content, .{}) catch return State{};
    defer parsed.deinit();

    var state = State{};

    if (parsed.object.get("version")) |v| {
        state.version = v.integer;
    }

    if (parsed.object.get("last_scanned_timestamp")) |v| {
        state.last_scanned_timestamp = @as(i64, @intCast(v.integer));
    }

    if (parsed.object.get("levels")) |levels_val| {
        if (levels_val.array.items.len > 0) {
            try state.levels.ensureTotalCapacity(allocator, levels_val.array.items.len);
            for (levels_val.array.items) |item| {
                var level = LevelState{};
                if (item.object.get("total_chars")) |tc| {
                    level.total_chars = @as(usize, @intCast(tc.integer));
                }
                if (item.object.get("pending_items")) |items_val| {
                    try level.pending_items.ensureTotalCapacity(allocator, items_val.array.items.len);
                    for (items_val.array.items) |pitem| {
                        var pending = PendingItem{
                            .timestamp = 0,
                            .source_end = 0,
                            .content_length = 0,
                            .level = 0,
                        };
                        if (pitem.object.get("file_path")) |fp| {
                            pending.file_path = try allocator.dupe(u8, fp.string);
                        }
                        if (pitem.object.get("timestamp")) |ts| {
                            pending.timestamp = @as(i64, @intCast(ts.integer));
                        }
                        if (pitem.object.get("source_end")) |se| {
                            pending.source_end = @as(i64, @intCast(se.integer));
                        }
                        if (pitem.object.get("content_length")) |cl| {
                            pending.content_length = @as(usize, @intCast(cl.integer));
                        }
                        if (pitem.object.get("key")) |k| {
                            pending.key = try allocator.dupe(u8, k.string);
                        }
                        try level.pending_items.append(allocator, pending);
                    }
                }
                try state.levels.append(allocator, level);
            }
        }
    }

    return state;
}

// ── Tests ─────────────────────────────────────────────────────────────

test "HierarchicalSummarizer: parseState roundtrip" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var original = State{};
    try original.levels.ensureTotalCapacity(allocator, 2);

    var level0 = LevelState{};
    try level0.pending_items.append(allocator, .{
        .file_path = "/workspace/messages/test.md",
        .timestamp = 1700000000,
        .source_end = 1700000000,
        .content_length = 1234,
        .key = "L0:20231114T123000",
        .level = 0,
    });
    level0.total_chars = 1234;
    try original.levels.append(allocator, level0);

    var level1 = LevelState{};
    try level1.pending_items.append(allocator, .{
        .file_path = "/workspace/memory/L1/testL1.md",
        .timestamp = 1700000000,
        .source_end = 1700003600,
        .content_length = 5678,
        .key = "L1:20231114T123000_20231114T124000",
        .level = 1,
    });
    level1.total_chars = 5678;
    try original.levels.append(allocator, level1);

    original.last_scanned_timestamp = 1700000000;

    const json_str = try json.encodeAlloc(allocator, original, .{});
    const parsed = try parseState(allocator, json_str);
    defer parsed.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), parsed.levels.items.len);
    try std.testing.expectEqual(@as(usize, 1234), parsed.levels.items[0].total_chars);
    try std.testing.expectEqual(@as(usize, 1), parsed.levels.items[0].pending_items.items.len);
    try std.testing.expectEqualStrings("L0:20231114T123000", parsed.levels.items[0].pending_items.items[0].key);
}

test "HierarchicalSummarizer: ensureLevels adds missing levels" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var state = State{};
    try ensureLevels(&state, 3, allocator);

    try std.testing.expectEqual(@as(usize, 3), state.levels.items.len);
    for (state.levels.items) |level| {
        try std.testing.expectEqual(@as(usize, 0), level.total_chars);
        try std.testing.expectEqual(@as(usize, 0), level.pending_items.items.len);
    }
}

test "HierarchicalSummarizer: removeTimestampSeparators formats correctly" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const summarizer = HierarchicalSummarizer{
        .allocator = allocator,
        .provider = undefined, // not used
        .model_name = "",
        .chunk_size = 200000,
        .overlap_entries = 1,
        .max_levels = 5,
        .workspace_dir = "",
        .messages_dir = "",
        .memory_dir = "",
        .state = State{},
        .state_file_path = "",
    };

    const input = "2026-03-22T10:00:00Z";
    const result = try summarizer.removeTimestampSeparators(allocator, input);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("20260322T100000Z", result);
}

test "HierarchicalSummarizer: formatTimestamp produces ISO8601" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const summarizer = HierarchicalSummarizer{
        .allocator = allocator,
        .provider = undefined,
        .model_name = "",
        .chunk_size = 200000,
        .overlap_entries = 1,
        .max_levels = 5,
        .workspace_dir = "",
        .messages_dir = "",
        .memory_dir = "",
        .state = State{},
        .state_file_path = "",
    };

    // Timestamp for 2026-03-22 10:30:45 UTC
    const ts: i64 = 1713730245;
    const formatted = try summarizer.formatTimestamp(allocator, ts);
    defer allocator.free(formatted);
    try std.testing.expectEqualStrings("2026-03-22T10:30:45Z", formatted);
}

// Note: More integration tests would require mocking the provider
// Those belong in a separate integration test suite
