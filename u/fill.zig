// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Filling memory without going through `memset`. From zroar's
//! zero.zig, with the byte as a parameter.
//!
//! `@memset` lowers to a call to `memset`, and in a Zig 0.16 program
//! that links libc the symbol resolves to compiler_rt's copy rather
//! than libc's: a byte-at-a-time loop, unlike its `memcpy` neighbour.
//! `fill` stores 64 bytes per step instead, then finishes with single
//! stores of 32, 16, 8, 4, 2 and 1 bytes, straight-line code with no
//! loop. Against glibc's memset it is even from 1 KB up and ahead
//! below, since there is no call.
//!
//! Two details are load-bearing. The empty `asm volatile` in the loop:
//! without it LLVM recognises the loop as a memset idiom and turns it
//! back into the call this file exists to avoid. The tail needs none,
//! having no loop to recognise. And the two stores per iteration: a
//! one-store loop is short enough that where it lands decides its
//! speed, and straddling a 64-byte boundary halved it (Zen 3, measured
//! 2x). Two stores per trip keep a store a cycle either way.

const std = @import("std");

const Chunk = @Vector(32, u8);

/// Sets every byte of `s` to `byte`. `s` is a slice of any type.
pub fn fill(s: anytype, byte: u8) void {
    const bytes = std.mem.sliceAsBytes(s);
    var p: [*]u8 = bytes.ptr; // drop the source alignment: p walks in bytes
    var n = bytes.len;
    const v: Chunk = @splat(byte);
    while (n >= 64) : (n -= 64) {
        p[0..32].* = v;
        p[32..64].* = v;
        p += 64;
        asm volatile ("" ::: .{ .memory = true });
    }
    if (n >= 32) {
        p[0..32].* = v;
        p += 32;
        n -= 32;
    }
    if (n >= 16) {
        p[0..16].* = @as(@Vector(16, u8), @splat(byte));
        p += 16;
        n -= 16;
    }
    if (n >= 8) {
        p[0..8].* = @as(@Vector(8, u8), @splat(byte));
        p += 8;
        n -= 8;
    }
    if (n >= 4) {
        p[0..4].* = @as(@Vector(4, u8), @splat(byte));
        p += 4;
        n -= 4;
    }
    if (n >= 2) {
        p[0..2].* = @as(@Vector(2, u8), @splat(byte));
        p += 2;
        n -= 2;
    }
    if (n >= 1) p[0] = byte;
}
