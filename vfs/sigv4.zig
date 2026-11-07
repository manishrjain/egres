// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! AWS Signature Version 4 (SigV4) request signing — the auth scheme every
//! S3-compatible store speaks (AWS, GCS interop, SeaweedFS, proxies). Pure
//! stdlib crypto (Sha256 + HmacSha256); no I/O. The S3 client signs exactly
//! three headers — host, x-amz-content-sha256, x-amz-date — which SigV4
//! requires at minimum (host + every x-amz-*); anything else we send
//! (Range, Content-Length, Connection) legally rides unsigned.

const std = @import("std");
const utils = @import("utils");
const String = utils.String;
const Sha256 = std.crypto.hash.sha2.Sha256;
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;

pub const hash_hex_len = Sha256.digest_length * 2;

/// Hex sha256 of a payload. The well-known empty-payload digest
/// (e3b0c442...) falls out of data="".
pub fn hashHex(data: String) [hash_hex_len]u8 {
    var digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(data, &digest, .{});
    var out: [hash_hex_len]u8 = undefined;
    _ = std.fmt.bufPrint(&out, "{x}", .{&digest}) catch unreachable;
    return out;
}

/// UTC timestamps in the two SigV4 formats, derived from epoch seconds.
pub const AmzDate = struct {
    /// ISO8601 basic: YYYYMMDD'T'HHMMSS'Z' (x-amz-date header).
    stamp: [16]u8,

    pub fn fromEpochSeconds(secs: u64) AmzDate {
        const total: i64 = @intCast(secs);
        const c = utils.date.civilFromDays(@divFloor(total, std.time.s_per_day));
        const day_secs: u64 = @intCast(@mod(total, std.time.s_per_day));
        var self: AmzDate = undefined;
        // Unsigned on purpose: a `0` fill on a signed integer prints an
        // explicit `+` sign.
        _ = std.fmt.bufPrint(&self.stamp, "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{
            @as(u64, @intCast(c.year)),
            @as(u64, @intCast(c.month)),
            @as(u64, @intCast(c.day)),
            day_secs / std.time.s_per_hour,
            (day_secs / std.time.s_per_min) % 60,
            day_secs % 60,
        }) catch unreachable;
        return self;
    }

    /// The YYYYMMDD prefix (credential scope date).
    pub fn date(self: *const AmzDate) String {
        return self.stamp[0..8];
    }
};

/// Build the Authorization header value for one request. `path` is the
/// canonical URI (absolute, already URI-safe — our object keys are hex
/// names and prefixes are restricted to unreserved characters, so no
/// percent-encoding pass is needed). Caller owns the returned slice.
pub fn authorization(
    a: std.mem.Allocator,
    access_key: String,
    secret_key: String,
    region: String,
    method: String,
    path: String,
    /// Canonical query string: parameters sorted by name, each name and
    /// value URI-encoded, joined with `&`. Empty for a request with no
    /// query (every object op). The caller builds it, because only the
    /// caller knows which parameters it is sending.
    query: String,
    host: String,
    payload_hash: String,
    amz: *const AmzDate,
    /// Temporary-credential token (AWS_SESSION_TOKEN), null for
    /// long-lived keys. Sent as x-amz-security-token, which SigV4
    /// requires in the signed set; it sorts after x-amz-date.
    session_token: ?String,
) ![]u8 {
    const signed: String = if (session_token != null)
        signed_headers ++ ";x-amz-security-token"
    else
        signed_headers;
    const token_hdr: String = if (session_token) |t|
        try std.fmt.allocPrint(a, "x-amz-security-token:{s}\n", .{t})
    else
        "";
    defer if (session_token != null) a.free(token_hdr);
    const canonical = try std.fmt.allocPrint(a, "{s}\n{s}\n{s}\nhost:{s}\n" ++
        "x-amz-content-sha256:{s}\nx-amz-date:{s}\n{s}\n{s}\n{s}", .{
        method,     path,      query,  host,         payload_hash,
        &amz.stamp, token_hdr, signed, payload_hash,
    });
    defer a.free(canonical);

    const canonical_hash = hashHex(canonical);
    const sts = try std.fmt.allocPrint(a, "AWS4-HMAC-SHA256\n{s}\n{s}/{s}/" ++ service ++ "/aws4_request\n{s}", .{
        &amz.stamp, amz.date(), region, &canonical_hash,
    });
    defer a.free(sts);

    const sig = signature(secret_key, amz.date(), region, service, sts);
    return std.fmt.allocPrint(a, "AWS4-HMAC-SHA256 Credential={s}/{s}/{s}/" ++
        service ++ "/aws4_request, SignedHeaders={s}, Signature={s}", .{
        access_key, amz.date(), region, signed, &sig,
    });
}

const service = "s3";
const signed_headers = "host;x-amz-content-sha256;x-amz-date";

/// The HMAC signing chain over `string_to_sign`, hex-encoded. `svc` is a
/// parameter (not the s3 const) so tests can run AWS's published
/// test-suite vectors, which use service name "service".
pub fn signature(secret_key: String, date: String, region: String, svc: String, string_to_sign: String) [64]u8 {
    var key: [HmacSha256.mac_length]u8 = undefined;
    {
        var buf: [4 + 64]u8 = undefined; // "AWS4" ++ secret (secrets are ≤ 40B)
        const seed = std.fmt.bufPrint(&buf, "AWS4{s}", .{secret_key}) catch unreachable;
        HmacSha256.create(&key, date, seed);
    }
    HmacSha256.create(&key, region, &key);
    HmacSha256.create(&key, svc, &key);
    HmacSha256.create(&key, "aws4_request", &key);
    HmacSha256.create(&key, string_to_sign, &key);
    var out: [64]u8 = undefined;
    _ = std.fmt.bufPrint(&out, "{x}", .{&key}) catch unreachable;
    return out;
}
