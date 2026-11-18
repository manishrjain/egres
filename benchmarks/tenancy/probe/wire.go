// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

// The Postgres wire protocol, as much of it as the probe needs: startup
// with trust authentication and the simple query protocol.
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"time"
)

// How long one connection may take from connect to its last reply.
var convTimeout = 10 * time.Second

func startup(user, db string) []byte {
	body := []byte{0, 3, 0, 0}
	for _, s := range []string{"user", user, "database", db} {
		body = append(body, s...)
		body = append(body, 0)
	}
	body = append(body, 0)
	out := make([]byte, 4, 4+len(body))
	binary.BigEndian.PutUint32(out, uint32(4+len(body)))
	return append(out, body...)
}

func simpleQuery(sql string) []byte {
	out := []byte{'Q', 0, 0, 0, 0}
	out = append(out, sql...)
	out = append(out, 0)
	binary.BigEndian.PutUint32(out[1:], uint32(len(out)-1))
	return out
}

// pgError is an error the server sent. The connection it came on is
// still good for the next statement.
type pgError struct{ severity, code, msg string }

func (e *pgError) Error() string {
	return "server: " + e.severity + " " + e.code + ": " + e.msg
}

// serverError parses an ErrorResponse body.
func serverError(body []byte) error {
	var sev, code, msg string
	for _, f := range bytes.Split(body, []byte{0}) {
		if len(f) < 2 {
			continue
		}
		switch f[0] {
		case 'S':
			sev = string(f[1:])
		case 'C':
			code = string(f[1:])
		case 'M':
			msg = string(f[1:])
		}
	}
	return &pgError{sev, code, msg}
}

// result is what a statement's reply carried: its rows, each column
// as the text the server sent (empty for NULL), and the command tag,
// "SELECT 1" or "UPDATE 1". A checker reads it and says what was
// wrong, so that a reply is held to the data the tenant was loaded
// with, not only to having arrived.
type result struct {
	rows [][]string
	tag  string
}

type checker func(*result) error

// dataRow parses a DataRow body: a column count, then each column's
// length (-1 for NULL) and bytes.
func dataRow(body []byte) []string {
	if len(body) < 2 {
		return nil
	}
	n := int(binary.BigEndian.Uint16(body))
	body = body[2:]
	cols := make([]string, 0, n)
	for i := 0; i < n && len(body) >= 4; i++ {
		l := int(int32(binary.BigEndian.Uint32(body)))
		body = body[4:]
		if l < 0 || l > len(body) {
			cols = append(cols, "")
			continue
		}
		cols = append(cols, string(body[:l]))
		body = body[l:]
	}
	return cols
}

// untilReady reads messages up to ReadyForQuery and returns what they
// carried. An error from the server is returned only once its
// ReadyForQuery has been read too, so that a connection kept for the
// next statement is not left one reply behind.
func untilReady(r *bufio.Reader) (*result, error) {
	var hdr [5]byte
	res := &result{}
	var failed error
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			// During startup the server closes after its error.
			if failed != nil {
				return nil, failed
			}
			return nil, err
		}
		n := int(binary.BigEndian.Uint32(hdr[1:])) - 4
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			if failed != nil {
				return nil, failed
			}
			return nil, err
		}
		switch hdr[0] {
		case 'E':
			failed = serverError(body)
		case 'D':
			res.rows = append(res.rows, dataRow(body))
		case 'C':
			res.tag = strings.TrimRight(string(body), "\x00")
		case 'Z':
			if failed != nil {
				return nil, failed
			}
			return res, nil
		}
	}
}

// splitTarget parses ip:port or ip:port/database.
func splitTarget(target string) (addr, db string) {
	addr, db, found := strings.Cut(target, "/")
	if !found {
		db = "postgres"
	}
	return addr, db
}

// run connects, sends one statement and reads its reply; a reply the
// checker (nil: none) rejects is an error.
func run(target, sql string, dial time.Duration, check checker) error {
	_, err := runTimed(target, sql, dial, check)
	return err
}

// phases is where one connection's time went: the TCP connect, the
// wait for the server to answer the startup message, and the query.
type phases struct{ connect, startup, query time.Duration }

func runTimed(target, sql string, dial time.Duration, check checker) (
	phases, error) {
	var ph phases
	addr, db := splitTarget(target)
	t0 := time.Now()
	c, err := net.DialTimeout("tcp", addr, dial)
	if err != nil {
		return ph, err
	}
	defer c.Close()
	t1 := time.Now()
	ph.connect = t1.Sub(t0)
	c.SetDeadline(t1.Add(convTimeout))
	r := bufio.NewReader(c)
	if _, err := c.Write(startup("postgres", db)); err != nil {
		return ph, err
	}
	if _, err := untilReady(r); err != nil {
		return ph, err
	}
	t2 := time.Now()
	ph.startup = t2.Sub(t1)
	if _, err := c.Write(simpleQuery(sql)); err != nil {
		return ph, err
	}
	res, err := untilReady(r)
	if err != nil {
		return ph, err
	}
	ph.query = time.Since(t2)
	if check != nil {
		return ph, check(res)
	}
	return ph, nil
}

// retry runs fn until it succeeds or the deadline passes. The first
// attempts have a short dial and little pause between them: a server
// that is starting refuses or drops them, and a SYN the guest missed
// would otherwise be resent only after a full second.
func retry(deadline time.Time, fn func(dial time.Duration) error) error {
	dial := 5 * time.Millisecond
	for {
		err := fn(dial)
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(500 * time.Microsecond)
		dial = min(dial*2, time.Second)
	}
}

// waitSQL returns once the server answers sql, or fails at the deadline.
func waitSQL(target, sql string, deadline time.Time) error {
	return retry(deadline, func(dial time.Duration) error {
		return run(target, sql, dial, nil)
	})
}

// session is a connection kept open for statement after statement.
type session struct {
	c net.Conn
	r *bufio.Reader
}

func open(target string, dial time.Duration) (*session, error) {
	addr, db := splitTarget(target)
	c, err := net.DialTimeout("tcp", addr, dial)
	if err != nil {
		return nil, err
	}
	s := &session{c: c, r: bufio.NewReader(c)}
	c.SetDeadline(time.Now().Add(convTimeout))
	if _, err := c.Write(startup("postgres", db)); err != nil {
		c.Close()
		return nil, err
	}
	if _, err := untilReady(s.r); err != nil {
		c.Close()
		return nil, err
	}
	return s, nil
}

// openRetry is open, retried until the deadline.
func openRetry(target string, deadline time.Time) (*session, error) {
	var s *session
	err := retry(deadline, func(dial time.Duration) error {
		var err error
		s, err = open(target, dial)
		return err
	})
	return s, err
}

func (s *session) close() { s.c.Close() }

func (s *session) exec(sql string) error {
	_, err := s.query(sql)
	return err
}

// query is exec that also returns what the reply carried.
func (s *session) query(sql string) (*result, error) {
	s.c.SetDeadline(time.Now().Add(convTimeout))
	if _, err := s.c.Write(simpleQuery(sql)); err != nil {
		return nil, err
	}
	return untilReady(s.r)
}

// check is query with the reply held to a checker (nil: none).
func (s *session) check(sql string, check checker) error {
	res, err := s.query(sql)
	if err != nil || check == nil {
		return err
	}
	return check(res)
}
