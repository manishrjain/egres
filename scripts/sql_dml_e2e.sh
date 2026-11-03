#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# End-to-end smoke test for SQL INSERT/SELECT/UPDATE/DELETE over the pgwire
# endpoint. Builds, starts a fresh single-node egres, drives psql through
# insert/select/filter, egid visibility, update (point + multi-SET + scan +
# full-table), delete (point + scan + full-table), a restart-persistence +
# monotonic-egid leg (incl. no-reuse of a deleted max), the extended query
# protocol with $n parameters (psql's \bind / \parse / \bind_named), and
# greps the server log. Wipes /data/egres — that dir is throw-away.
set -euo pipefail
cd "$(dirname "$0")/.."

DATA="${DATA:-/data/egres}"
LOG=/tmp/zdb/server.log
E="host=127.0.0.1 port=3210 dbname=egres sslmode=disable"

pass=0
fail=0
ok() { echo "ok: $1"; pass=$((pass + 1)); }
bad() { echo "FAIL: $1"; fail=$((fail + 1)); }

# expect <desc> <needle> <cmd...>: run, assert output contains needle.
expect() {
    local desc=$1 needle=$2
    shift 2
    local out
    out=$("$@" 2>&1) || true
    if grep -q -- "$needle" <<<"$out"; then ok "$desc"; else bad "$desc — got: $out"; fi
}

q() { psql "$E" -X -c "$1"; }     # full output (row counts, headers)
v() { psql "$E" -X -tA -c "$1"; } # tuples-only, unaligned (bare value)
# A block, each statement its own message, from stdin.
tx() { psql "$E" -X -tA -v ON_ERROR_STOP=0 -f - <<<"$1" 2>&1; }

# Ready = listener up AND election won (see sql_ddl_e2e.sh).
start_server() {
    ./zig-out/bin/egres --raft="id=1" \
        --store="dir_path=$DATA/sst" --wal="dir=$DATA/wal" >"$LOG" 2>&1 &
    SERVER_PID=$!
    for _ in $(seq 1 50); do
        if grep -q "sql listener started" "$LOG" 2>/dev/null &&
            grep -q "become-leader" "$LOG" 2>/dev/null; then return; fi
        sleep 0.2
    done
    echo "server never became ready"
    exit 1
}

stop_server() {
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}

zig build
rm -rf "$DATA" && mkdir -p "$DATA" /tmp/zdb
start_server
trap stop_server EXIT

# --- create + insert -------------------------------------------------------
# No PRIMARY KEY: this suite exercises egid-as-identity + data mechanics (rows
# are looked up by egid/name, and several inserts omit `id`). PK enforcement
# and its concurrency live in sql_txn_e2e.sh.
expect "CREATE TABLE" "CREATE TABLE" q "CREATE TABLE t (id int4, name text);"
expect "INSERT two rows" "INSERT 0 2" q "INSERT INTO t VALUES (1, 'a'), (2, 'b');"

# --- select back -----------------------------------------------------------
expect "SELECT * returns both" "(2 rows)" q "SELECT * FROM t;"
expect "egid 1 = first row" "a" v "SELECT name FROM t WHERE egid = 1;"
expect "egid 2 = second row" "b" v "SELECT name FROM t WHERE egid = 2;"
expect "egid 0 rejected" "positive integer" q "SELECT name FROM t WHERE egid = 0;"
expect "negative egid rejected (uint8 parse)" "invalid value" q "SELECT name FROM t WHERE egid = -1;"
expect "WHERE on payload column" "b" v "SELECT name FROM t WHERE id = 2;"
expect "SELECT egid, id, name" "(2 rows)" q "SELECT egid, id, name FROM t;"

# --- boolean WHERE: AND / OR / NOT, precedence, egid conjunct --------------
expect "AND" "a" v "SELECT name FROM t WHERE id = 1 AND name = 'a';"
expect "AND no match" "(0 rows)" q "SELECT name FROM t WHERE id = 1 AND name = 'b';"
expect "OR" "(2 rows)" q "SELECT name FROM t WHERE id = 1 OR name = 'b';"
expect "NOT" "b" v "SELECT name FROM t WHERE NOT id = 1;"
expect "AND binds tighter than OR" "(2 rows)" q "SELECT name FROM t WHERE id = 2 OR id = 1 AND name = 'a';"
expect "parentheses override" "(0 rows)" q "SELECT name FROM t WHERE (id = 2 OR id = 1) AND name = 'zz';"
expect "egid conjunct drives a point read" "b" v "SELECT name FROM t WHERE egid = 2 AND id = 2;"
expect "egid conjunct, false residual" "(0 rows)" q "SELECT name FROM t WHERE egid = 2 AND id = 1;"
expect ">" "b" v "SELECT name FROM t WHERE id > 1;"
expect "<= and <>" "a" v "SELECT name FROM t WHERE id <= 2 AND name <> 'b';"
expect ">= on text" "b" v "SELECT name FROM t WHERE name >= 'b';"
expect "literal on the left turns the operator" "b" v "SELECT name FROM t WHERE 1 < id;"
expect "< with an indexed = conjunct" "a" v "SELECT name FROM t WHERE egid = 1 AND id < 2;"
expect "column vs column" "(0 rows)" q "SELECT name FROM t WHERE id > id;"
expect "function in WHERE" "b" v "SELECT name FROM t WHERE upper(name) = 'B';"
expect "arithmetic in WHERE" "a" v "SELECT name FROM t WHERE id * 2 = 2;"
expect "constants in WHERE" "0" v "SELECT count(*) FROM t WHERE 5 > 6;"
expect "expression IS NOT NULL" "1" \
    v "SELECT count(*) FROM t WHERE nullif(id, 1) IS NOT NULL;"
expect "IN" "(2 rows)" q "SELECT name FROM t WHERE id IN (1, 2, 3);"
expect "NOT IN" "b" v "SELECT name FROM t WHERE id NOT IN (1, 3);"
expect "NOT IN with a NULL matches nothing" "0" \
    v "SELECT count(*) FROM t WHERE id NOT IN (1, NULL);"
expect "IN with an expression" "a" v "SELECT name FROM t WHERE id IN (0 + 1, 5);"
expect "BETWEEN" "(2 rows)" q "SELECT name FROM t WHERE id BETWEEN 1 AND 2;"
expect "NOT BETWEEN" "b" v "SELECT name FROM t WHERE id NOT BETWEEN 0 AND 1;"
expect "IS DISTINCT FROM" "b" v "SELECT name FROM t WHERE id IS DISTINCT FROM 1;"
expect "IS NOT DISTINCT FROM" "a" v "SELECT name FROM t WHERE 1 IS NOT DISTINCT FROM id;"
expect "IS NOT DISTINCT FROM NULL matches nothing" "0" \
    v "SELECT count(*) FROM t WHERE id IS NOT DISTINCT FROM NULL;"
expect "IS TRUE on an int is a type mismatch" "does not match" \
    q "SELECT name FROM t WHERE id IS TRUE;"
expect "CREATE TABLE with a bool" "CREATE TABLE" \
    q "CREATE TABLE flags (id int4, on_ bool);"
expect "INSERT true, false, NULL" "INSERT 0 3" \
    q "INSERT INTO flags VALUES (1, true), (2, false), (3, NULL);"
expect "bare boolean column" "1" v "SELECT id FROM flags WHERE on_;"
expect "NOT bare boolean column" "2" v "SELECT id FROM flags WHERE NOT on_;"
expect "IS TRUE" "1" v "SELECT id FROM flags WHERE on_ IS TRUE;"
expect "IS NOT TRUE takes the NULL row too" "(2 rows)" \
    q "SELECT id FROM flags WHERE on_ IS NOT TRUE;"
expect "IS FALSE" "2" v "SELECT id FROM flags WHERE on_ IS FALSE;"
expect "IS UNKNOWN" "3" v "SELECT id FROM flags WHERE on_ IS UNKNOWN;"
expect "IS NOT UNKNOWN" "(2 rows)" q "SELECT id FROM flags WHERE on_ IS NOT UNKNOWN;"
expect "DROP the bool table" "DROP TABLE" q "DROP TABLE flags;"
expect "expression target with an alias" "3" \
    v "SELECT id + 1 AS next FROM t WHERE id = 2;"
expect "function target" "A" v "SELECT upper(name) FROM t WHERE id = 1;"
expect "function target header" "upper" q "SELECT upper(name) FROM t WHERE id = 1;"
expect "constant target header" "?column?" q "SELECT 7 FROM t WHERE id = 1;"
expect "division by zero in WHERE" "division by zero" \
    q "SELECT name FROM t WHERE id / 0 = 1;"
expect "division by zero in a target" "division by zero" \
    q "SELECT id / 0 FROM t;"
expect "text vs number in WHERE refused" "operator does not exist" \
    q "SELECT name FROM t WHERE name = id;"
expect "unsupported operator rejected" "comparisons" q "SELECT name FROM t WHERE name ~ 'a';"

# egid is a hidden system column: not in SELECT *.
star_out=$(q "SELECT * FROM t;" 2>&1)
if grep -q "egid" <<<"$star_out"; then bad "egid leaked into SELECT *"; else ok "egid excluded from SELECT *"; fi

# --- count(*): one int8 row; WHERE narrows it, only LIMIT 0 drops it -------
expect "count(*)" "2" v "SELECT count(*) FROM t;"
expect "count(*) header" "count" q "SELECT count(*) FROM t;"
expect "count(*) tag" "(1 row)" q "SELECT count(*) FROM t;"
expect "count(*) with WHERE" "1" v "SELECT count(*) FROM t WHERE id = 1;"
expect "count(*) by egid" "1" v "SELECT count(*) FROM t WHERE egid = 2;"
expect "count(*) no match is 0" "0" v "SELECT count(*) FROM t WHERE id = 99;"
expect "count(*) LIMIT 0 drops the row" "(0 rows)" \
    q "SELECT count(*) FROM t LIMIT 0;"
expect "count(*) LIMIT 5 keeps it" "2" v "SELECT count(*) FROM t LIMIT 5;"
expect "count(col) rejected" "only count" q "SELECT count(id) FROM t;"
expect "count(*) with columns rejected" "other targets" \
    q "SELECT id, count(*) FROM t;"
expect "count(*) FOR UPDATE rejected" "FOR UPDATE" \
    q "SELECT count(*) FROM t FOR UPDATE;"

# --- timestamps: CURRENT_TIMESTAMP and literals into timestamp columns -----
q "CREATE TABLE ts (id int4, ts1 timestamp, ts2 timestamptz);" >/dev/null
expect "INSERT CURRENT_TIMESTAMP" "INSERT 0 1" \
    q "INSERT INTO ts VALUES (1, CURRENT_TIMESTAMP, LOCALTIMESTAMP);"
ts_lit="'2026-09-12 10:00:00.5', '2026-09-12 12:00:00+02'"
expect "INSERT timestamp literals" "INSERT 0 1" \
    q "INSERT INTO ts VALUES (2, $ts_lit);"
expect "timestamp renders" "2026-09-12 10:00:00.5" \
    v "SELECT ts1 FROM ts WHERE id = 2;"
expect "timestamptz normalizes to UTC" "2026-09-12 10:00:00+00" \
    v "SELECT ts2 FROM ts WHERE id = 2;"
expect "CURRENT_TIMESTAMP is now (this year)" "$(date -u +%Y)-" \
    v "SELECT ts1 FROM ts WHERE id = 1;"
expect "WHERE on a timestamp" "2" \
    v "SELECT id FROM ts WHERE ts1 = '2026-09-12 10:00:00.5';"
expect "CURRENT_TIMESTAMP into int rejected" "timestamp column" \
    q "INSERT INTO ts VALUES (CURRENT_TIMESTAMP, NULL, NULL);"
expect "bad timestamp literal" "invalid value" \
    q "INSERT INTO ts VALUES (3, 'yesterday', NULL);"
expect "count(*) on ts" "2" v "SELECT count(*) FROM ts;"
q "DROP TABLE ts;" >/dev/null

# --- omitted column defaults NULL; egid auto-allocates ---------------------
expect "INSERT partial cols" "INSERT 0 1" q "INSERT INTO t (name) VALUES ('c');"
expect "auto egid 3 = c" "c" v "SELECT name FROM t WHERE egid = 3;"

# --- SQL three-valued logic: NULL is unknown, only TRUE matches ------------
expect "= NULL matches nothing" "(0 rows)" q "SELECT name FROM t WHERE id = NULL;"
expect "NOT (= NULL) is still unknown" "(0 rows)" q "SELECT name FROM t WHERE NOT id = NULL;"
expect "IS NULL" "c" v "SELECT name FROM t WHERE id IS NULL;"
expect "IS NOT NULL" "(2 rows)" q "SELECT name FROM t WHERE id IS NOT NULL;"
expect "unknown OR true is true" "c" v "SELECT name FROM t WHERE id = NULL OR name = 'c';"
expect "unknown AND true is unknown" "(0 rows)" q "SELECT name FROM t WHERE id = 5 AND name = 'c';"

# --- egid is read-only at insert time --------------------------------------
expect "cannot insert egid" "not supported" q "INSERT INTO t (egid, name) VALUES (100, 'x');"

# --- error paths -----------------------------------------------------------
expect "unknown table" "does not exist" q "SELECT * FROM nope;"
expect "unknown column" "does not exist" q "SELECT bogus FROM t;"

# --- restart: rows persist, egid stays monotonic (no reuse) ----------------
stop_server
start_server
expect "rows survive restart" "(3 rows)" q "SELECT * FROM t;"
expect "INSERT post-restart" "INSERT 0 1" q "INSERT INTO t (name) VALUES ('d');"
expect "egid 4 post-restart" "d" v "SELECT name FROM t WHERE egid = 4;"
# State here: egids 1=a, 2=b, 3=c, 4=d, all live; ids 1, 2, NULL, NULL.

# --- update: point, multi-SET, scan, full-table ----------------------------
expect "UPDATE by egid" "UPDATE 1" q "UPDATE t SET name = 'b2' WHERE egid = 2;"
expect "updated value visible" "b2" v "SELECT name FROM t WHERE egid = 2;"
expect "multi-SET" "UPDATE 1" q "UPDATE t SET id = 20, name = 'b3' WHERE egid = 2;"
expect "multi-SET visible" "b3" v "SELECT name FROM t WHERE id = 20;"
expect "UPDATE by column" "UPDATE 1" q "UPDATE t SET id = 30 WHERE name = 'c';"
expect "scan-update visible" "c" v "SELECT name FROM t WHERE id = 30;"
expect "UPDATE with AND" "UPDATE 1" q "UPDATE t SET id = 31 WHERE id = 30 AND name = 'c';"
expect "UPDATE with OR" "UPDATE 2" q "UPDATE t SET id = 32 WHERE id = 31 OR egid = 1;"
expect "UPDATE with IS NULL" "UPDATE 1" q "UPDATE t SET id = 33 WHERE id IS NULL;"
expect "UPDATE no match is 0" "UPDATE 0" q "UPDATE t SET id = 7 WHERE name = 'zzz';"
expect "UPDATE all rows" "UPDATE 4" q "UPDATE t SET id = 99;"
expect "all rows updated" "(4 rows)" q "SELECT * FROM t WHERE id = 99;"

# --- update: SET expressions, evaluated per row on the pre-image ------------
# Own table so the legs above and below keep their state. `n` is
# indexed: an arithmetic SET must move the row between postings.
q "CREATE TABLE ex (n int4, s text, m int4, f float8);" >/dev/null
q "CREATE INDEX ex_n ON ex (n);" >/dev/null
q "INSERT INTO ex VALUES (1, 'a', 10, 1.5), (2, 'b', 20, 2.5),
    (3, NULL, NULL, NULL);" >/dev/null
expect "SET n = n + 1 (all rows)" "UPDATE 3" q "UPDATE ex SET n = n + 1;"
expect "increment via index (old key gone)" "(0 rows)" \
    q "SELECT * FROM ex WHERE n = 1;"
expect "increment via index (new key)" "a" v "SELECT s FROM ex WHERE n = 2;"
expect "SET n = n * 2 + 1" "UPDATE 1" \
    q "UPDATE ex SET n = n * 2 + 1 WHERE s = 'a';"
expect "arithmetic visible" "5" v "SELECT n FROM ex WHERE s = 'a';"
expect "SET n = -n" "UPDATE 1" q "UPDATE ex SET n = -n WHERE s = 'a';"
expect "negation visible (negative index key)" "a" \
    v "SELECT s FROM ex WHERE n = -5;"
expect "SET m = egid * 10" "UPDATE 3" q "UPDATE ex SET m = egid * 10;"
expect "egid operand visible" "20" v "SELECT m FROM ex WHERE s = 'b';"
expect "SET m = m / 3 (integer division)" "UPDATE 1" \
    q "UPDATE ex SET m = m / 3 WHERE s = 'b';"
expect "integer division visible" "6" v "SELECT m FROM ex WHERE s = 'b';"
expect "SET f = f * 2" "UPDATE 1" q "UPDATE ex SET f = f * 2 WHERE s = 'b';"
expect "float arithmetic visible" "5" v "SELECT f FROM ex WHERE s = 'b';"
expect "SET on a NULL cell stays NULL" "UPDATE 1" \
    q "UPDATE ex SET f = f + 1 WHERE n = 4;"
expect "NULL result visible" "(1 row)" \
    q "SELECT * FROM ex WHERE n = 4 AND f IS NULL;"
expect "SET swap reads the pre-image" "UPDATE 1" \
    q "UPDATE ex SET n = m, m = n WHERE s = 'b';"
expect "swap: n took m" "6" v "SELECT n FROM ex WHERE s = 'b';"
expect "swap: m took n" "3" v "SELECT m FROM ex WHERE s = 'b';"
expect "SET s = s || '!'" "UPDATE 1" \
    q "UPDATE ex SET s = s || '!' WHERE n = -5;"
expect "concat visible by value" "a!" v "SELECT s FROM ex WHERE s = 'a!';"
expect "SET wrapped concat" "UPDATE 1" \
    q "UPDATE ex SET s = 'p-' || s || '-s' WHERE n = -5;"
expect "wrapped concat visible" "p-a!-s" v "SELECT s FROM ex WHERE n = -5;"
expect "SET s = s || NULL" "UPDATE 1" \
    q "UPDATE ex SET s = s || NULL WHERE n = -5;"
expect "NULL concat visible" "(1 row)" \
    q "SELECT * FROM ex WHERE n = -5 AND s IS NULL;"
expect "SET column copy" "UPDATE 1" q "UPDATE ex SET n = m WHERE s = 'b';"
expect "copy visible" "3" v "SELECT n FROM ex WHERE s = 'b';"
# n is indexed: -5, 3, 4. Ranges go through the index, and see a
# pending insert of a value the index has never held.
expect "index range: n > 0" "2" v "SELECT count(*) FROM ex WHERE n > 0;"
expect "index range: n >= 4" "1" v "SELECT count(*) FROM ex WHERE n >= 4;"
expect "index range: n < 0" "1" v "SELECT count(*) FROM ex WHERE n < 0;"
expect "index range: n <= 3" "2" v "SELECT count(*) FROM ex WHERE n <= 3;"
expect "index range: two bounds" "2" \
    v "SELECT count(*) FROM ex WHERE n > -10 AND n <= 3;"
expect "index range: OR of ranges" "2" \
    v "SELECT count(*) FROM ex WHERE n > 3 OR n < -1;"
expect "index range: empty" "0" v "SELECT count(*) FROM ex WHERE n > 100;"
expect "index range: pending value" "1" tx "BEGIN;
INSERT INTO ex VALUES (100, 'r', 0, 0);
SELECT count(*) FROM ex WHERE n > 50;
ROLLBACK;"
expect "index range: rolled back value gone" "0" \
    v "SELECT count(*) FROM ex WHERE n > 50;"
expect "SET s = CONCAT(s, ',', n)" "UPDATE 1" \
    q "UPDATE ex SET s = CONCAT(s, ',', n) WHERE s = 'b';"
expect "CONCAT renders a number" "b,3" v "SELECT s FROM ex WHERE n = 3;"
expect "CONCAT skips NULL" "UPDATE 1" \
    q "UPDATE ex SET s = CONCAT('x', NULL, s, f) WHERE n = 3;"
expect "NULL-skipping concat visible" "xb,35" v "SELECT s FROM ex WHERE n = 3;"
expect "SET f = COALESCE(f, m)" "UPDATE 1" \
    q "UPDATE ex SET f = COALESCE(f, m) WHERE n = 4;"
expect "COALESCE took the second" "30" v "SELECT f FROM ex WHERE n = 4;"
expect "COALESCE of a parameter" "UPDATE 1" \
    q "UPDATE ex SET m = COALESCE(NULL, m + 1, 0) WHERE n = 4;"
expect "COALESCE arithmetic visible" "31" v "SELECT m FROM ex WHERE n = 4;"
expect "COALESCE mixes kinds refused" "operator does not exist" \
    q "UPDATE ex SET s = COALESCE(s, n);"
expect "upper" "UPDATE 1" q "UPDATE ex SET s = upper(s) WHERE n = 3;"
expect "upper visible" "XB,35" v "SELECT s FROM ex WHERE n = 3;"
expect "substring, lower and length" "UPDATE 1" \
    q "UPDATE ex SET s = CONCAT(lower(substring(s, 2, 2)), length(s))
       WHERE n = 3;"
expect "function chain visible" "b,5" v "SELECT s FROM ex WHERE n = 3;"
expect "replace, left, right, concat_ws and btrim" "UPDATE 1" \
    q "UPDATE ex SET s = btrim(concat_ws('-', left(s, 1),
       right(replace(s, ',', 'x'), 2)), 'b') WHERE n = 3;"
expect "string functions visible" "-x5" v "SELECT s FROM ex WHERE n = 3;"
expect "round to places" "UPDATE 1" \
    q "UPDATE ex SET f = round(f / 7, 2) WHERE n = 4;"
expect "round visible" "4.29" v "SELECT f FROM ex WHERE n = 4;"
expect "greatest and nullif" "UPDATE 1" \
    q "UPDATE ex SET m = nullif(greatest(m, 40, 35), 40) WHERE n = 4;"
expect "nullif made NULL" "1" \
    v "SELECT count(*) FROM ex WHERE n = 4 AND m IS NULL;"
expect "least, mod and abs" "UPDATE 1" \
    q "UPDATE ex SET m = abs(mod(least(-17, 5), 5)) WHERE n = 4;"
expect "numeric functions visible" "2" v "SELECT m FROM ex WHERE n = 4;"
expect "power and sqrt" "UPDATE 1" \
    q "UPDATE ex SET f = sqrt(power(2, 10)) WHERE n = 4;"
expect "power visible" "32" v "SELECT f FROM ex WHERE n = 4;"
expect "sqrt of a negative refused" "power function" \
    q "UPDATE ex SET f = sqrt(-1);"
expect "negative substring length refused" "negative substring length" \
    q "UPDATE ex SET s = substring(s, 1, -1);"
expect "wrong argument kind refused" "operator does not exist" \
    q "UPDATE ex SET s = lower(n);"
expect "wrong arity refused" "takes 1 argument" \
    q "UPDATE ex SET s = upper(s, s);"
q "UPDATE ex SET s = 'b' WHERE n = 3;" >/dev/null # the legs below find it by name
expect "division by zero" "division by zero" \
    q "UPDATE ex SET n = n / 0 WHERE s = 'b';"
expect "integer overflow" "out of range" \
    q "UPDATE ex SET n = n + 2147483647 WHERE s = 'b';"
expect "no + on text" "operator does not exist" q "UPDATE ex SET s = s + 1;"
expect "no || with a number" "operator does not exist" \
    q "UPDATE ex SET s = s || n;"
expect "float into int refused" "does not match" q "UPDATE ex SET n = n * 1.5;"
expect "text into int refused" "does not match" q "UPDATE ex SET n = s;"
expect "other function in SET rejected" "not supported" q "UPDATE ex SET n = now();"
expect "unknown column in SET expr" "does not exist" \
    q "UPDATE ex SET n = zz + 1;"
expect "failed statements changed nothing" "3" \
    v "SELECT n FROM ex WHERE s = 'b';"

# --- update rejections / errors --------------------------------------------
expect "UPDATE egid rejected" "egid" q "UPDATE t SET egid = 9 WHERE egid = 1;"
expect "UPDATE unknown table" "does not exist" q "UPDATE nope SET id = 1;"
expect "UPDATE unknown column" "does not exist" q "UPDATE t SET bogus = 1;"

# --- delete by egid (point path) -------------------------------------------
expect "DELETE by egid" "DELETE 1" q "DELETE FROM t WHERE egid = 2;"
expect "deleted row invisible" "(0 rows)" q "SELECT * FROM t WHERE egid = 2;"
expect "DELETE same egid again is 0" "DELETE 0" q "DELETE FROM t WHERE egid = 2;"

# --- delete by payload column (scan path) ----------------------------------
expect "DELETE with a false AND is 0" "DELETE 0" q "DELETE FROM t WHERE name = 'c' AND id = 5;"
expect "DELETE with an unknown AND is 0" "DELETE 0" q "DELETE FROM t WHERE name = 'c' AND id = NULL;"
expect "DELETE by column" "DELETE 1" q "DELETE FROM t WHERE name = 'c';"
expect "DELETE no match is 0" "DELETE 0" q "DELETE FROM t WHERE name = 'zzz';"
expect "survivors intact" "(2 rows)" q "SELECT * FROM t;" # a (egid 1), d (egid 4)

# --- delete rejections / errors --------------------------------------------
expect "DELETE USING rejected" "not supported" q "DELETE FROM t USING u WHERE t.id = u.id;"
expect "DELETE unknown table" "does not exist" q "DELETE FROM nope;"

# --- delete the current max, then restart: egid must NOT be reused ----------
# Deletes egid 4 (the highest stored key). maxKey must count the tombstone and
# seed next_egid past it, else the next INSERT resurrects egid 4.
expect "DELETE current max" "DELETE 1" q "DELETE FROM t WHERE egid = 4;"
stop_server
start_server
expect "deletes survive restart" "(1 row)" q "SELECT * FROM t;" # just a (egid 1)
expect "update survives restart" "99" v "SELECT id FROM t WHERE egid = 1;"
expect "INSERT after delete+restart" "INSERT 0 1" q "INSERT INTO t (name) VALUES ('e');"
expect "egid 4 not reused (lands on 5)" "e" v "SELECT name FROM t WHERE egid = 5;"

# --- delete every row (no WHERE) -------------------------------------------
expect "DELETE all" "DELETE 2" q "DELETE FROM t;" # a (egid 1), e (egid 5)
expect "table empty" "(0 rows)" q "SELECT * FROM t;"
expect "DELETE on empty is 0" "DELETE 0" q "DELETE FROM t;"

# --- multi-batch: 5000 rows cross the executor's 1024-row batches ----------
# values_list <first id> <count>: "(i, 'ri'), ..." for one INSERT.
values_list() {
    local i=$1 end=$(($1 + $2)) sep=""
    while [ "$i" -lt "$end" ]; do
        printf '%s(%d, %s)' "$sep" "$i" "'r$i'"
        sep=", "
        i=$((i + 1))
    done
}
q "CREATE TABLE big (id int4, name text);" >/dev/null
for s in $(seq 1 500 5000); do
    q "INSERT INTO big VALUES $(values_list "$s" 500);" >/dev/null
done
expect "SELECT streams 5000 rows" "(5000 rows)" q "SELECT id FROM big;"
expect "WHERE past batch 1" "r3000" v "SELECT name FROM big WHERE id = 3000;"
expect "UPDATE 5000 rows" "UPDATE 5000" q "UPDATE big SET name = 'u';"
expect "all rewritten" "(5000 rows)" q "SELECT id FROM big WHERE name = 'u';"
expect "FOR UPDATE 5000 rows" "(5000 rows)" q "SELECT id FROM big FOR UPDATE;"

# --- LIMIT: bounds the source, crosses a batch, composes with the rest -----
expect "LIMIT 3" "(3 rows)" q "SELECT id FROM big LIMIT 3;"
expect "LIMIT past batch 1" "(1500 rows)" q "SELECT id FROM big LIMIT 1500;"
expect "LIMIT 0" "(0 rows)" q "SELECT id FROM big LIMIT 0;"
expect "LIMIT ALL" "(5000 rows)" q "SELECT id FROM big LIMIT ALL;"
expect "LIMIT with WHERE" "(2 rows)" \
    q "SELECT id FROM big WHERE name = 'u' LIMIT 2;"
expect "LIMIT above the result" "(1 row)" \
    q "SELECT id FROM big WHERE id = 7 LIMIT 5;"
expect "FOR UPDATE LIMIT" "(2 rows)" q "SELECT id FROM big FOR UPDATE LIMIT 2;"
expect "negative LIMIT rejected" "negative" q "SELECT id FROM big LIMIT -1;"
expect "ORDER BY DESC LIMIT across batches" "5000" \
    v "SELECT id FROM big ORDER BY id DESC LIMIT 1;"
expect "OFFSET past a batch" "(3500 rows)" q "SELECT id FROM big OFFSET 1500;"
expect "LIMIT with OFFSET" "1501" v "SELECT id FROM big LIMIT 1 OFFSET 1500;"

# --- ORDER BY: keys, directions, NULLs, OFFSET -------------------------
# The ids in result order, one line: "3 1 4 2 ".
seq_() { psql "$E" -X -tA -c "$1" | tr '\n' ' '; }
expect "CREATE TABLE for ORDER BY" "CREATE TABLE" \
    q "CREATE TABLE ob (id int4, n int4, s text);"
expect "INSERT rows with NULLs" "INSERT 0 4" \
    q "INSERT INTO ob VALUES (1, 3, 'b'), (2, NULL, 'a'), (3, 1, 'c'),
       (4, 3, NULL);"
expect "ORDER BY: NULLs last, ties in source order" "3 1 4 2 " \
    seq_ "SELECT id FROM ob ORDER BY n;"
expect "ORDER BY DESC: NULLs first" "2 1 4 3 " \
    seq_ "SELECT id FROM ob ORDER BY n DESC;"
expect "ORDER BY NULLS FIRST" "2 3 1 4 " \
    seq_ "SELECT id FROM ob ORDER BY n NULLS FIRST;"
expect "ORDER BY DESC NULLS LAST" "1 4 3 2 " \
    seq_ "SELECT id FROM ob ORDER BY n DESC NULLS LAST;"
expect "ORDER BY two keys" "3 4 1 2 " \
    seq_ "SELECT id FROM ob ORDER BY n, id DESC;"
expect "ORDER BY text" "2 1 3 4 " seq_ "SELECT id FROM ob ORDER BY s;"
expect "ORDER BY an expression" "1 4 3 2 " \
    seq_ "SELECT id FROM ob ORDER BY n * -1;"
expect "ORDER BY an ordinal" "3|4 1|3 |2 3|1 " \
    seq_ "SELECT n, id FROM ob ORDER BY 2 DESC;"
expect "ORDER BY an output name" "4 3 2 1 " \
    seq_ "SELECT id AS k FROM ob ORDER BY k DESC;"
expect "ORDER BY with WHERE" "4 3 1 " \
    seq_ "SELECT id FROM ob WHERE n IS NOT NULL ORDER BY id DESC;"
expect "ORDER BY then LIMIT" "3 " seq_ "SELECT id FROM ob ORDER BY n LIMIT 1;"
expect "OFFSET after the ordering" "3 4 " \
    seq_ "SELECT id FROM ob ORDER BY id OFFSET 2;"
expect "LIMIT and OFFSET" "2 " \
    seq_ "SELECT id FROM ob ORDER BY id LIMIT 1 OFFSET 1;"
expect "OFFSET past the end" "(0 rows)" q "SELECT id FROM ob OFFSET 9;"
expect "count(*) OFFSET 1 drops the row" "(0 rows)" \
    q "SELECT count(*) FROM ob OFFSET 1;"
expect "ORDER BY under FOR UPDATE" "4 3 2 1 " \
    seq_ "SELECT id FROM ob ORDER BY id DESC FOR UPDATE;"
expect "ORDER BY position past the targets" "not in select list" \
    q "SELECT id FROM ob ORDER BY 2;"
expect "ORDER BY with count(*) refused" "not allowed" \
    q "SELECT count(*) FROM ob ORDER BY 1;"
expect "ORDER BY without FROM refused" "need a FROM" q "SELECT 1 ORDER BY 1;"
expect "negative OFFSET rejected" "negative" q "SELECT id FROM ob OFFSET -1;"
expect "DROP the ORDER BY table" "DROP TABLE" q "DROP TABLE ob;"

expect "DELETE 5000 rows" "DELETE 5000" q "DELETE FROM big;"
expect "big is empty" "(0 rows)" q "SELECT id FROM big;"

# --- a row above the batch's byte capacity (256 KiB) -----------------------
# The SQL text is too long for an argv entry, so it goes in on stdin.
qin() { psql "$E" -X -f - <<<"$1"; }
BIGTEXT=$(head -c 300000 /dev/zero | tr '\0' 'x')
big_insert() { qin "INSERT INTO big VALUES ($1, '$BIGTEXT');"; }
expect "INSERT 300 KB row" "INSERT 0 1" big_insert 1
expect "INSERT second 300 KB row" "INSERT 0 1" big_insert 2
expect "small row after them" "INSERT 0 1" q "INSERT INTO big VALUES (3, 's');"
expect "oversized rows stream" "(3 rows)" q "SELECT id FROM big;"
# $(( )) strips the spaces BSD wc pads its count with.
if [ $(( $(v "SELECT name FROM big WHERE id = 2;" | wc -c) )) = 300001 ]; then
    ok "oversized row round-trips intact"
else bad "oversized row came back with the wrong length"; fi
expect "UPDATE oversized rows" "UPDATE 3" q "UPDATE big SET id = 9;"
expect "DELETE oversized rows" "DELETE 3" q "DELETE FROM big WHERE id = 9;"

# --- char(n) / varchar(n): stripped on the way in, padded on the way out ---
# SELECT lists take column references only, so padding is checked by the
# byte length of the bare value (`v` prints it plus a newline).
expect_len() { # <desc> <bytes> <sql>: the value's length in bytes, newline included
    local got; got=$(( $(v "$3" | wc -c) ))
    if [ "$got" = "$2" ]; then ok "$1"; else bad "$1 — got $got bytes"; fi
}
expect "CREATE char/varchar table" "CREATE TABLE" \
    q "CREATE TABLE cn (id int PRIMARY KEY, a char(5), b varchar(3), c char);"
expect "INSERT 'ab ' into char(5)" "INSERT 0 1" q "INSERT INTO cn VALUES (1, 'ab ', 'xy', 'z');"
expect_len "char(5) reads back padded to 5" 6 "SELECT a FROM cn WHERE id = 1;"
expect "char(5) value is the padded text" "ab   " v "SELECT a FROM cn WHERE id = 1;"
expect_len "varchar(3) reads back unpadded" 3 "SELECT b FROM cn WHERE id = 1;"
expect_len "bare char is char(1)" 2 "SELECT c FROM cn WHERE id = 1;"
expect "char(5) rejects 6 chars" "value too long for type character(5)" \
    q "INSERT INTO cn VALUES (2, 'abcdef', 'x', 'y');"
expect "char(5) accepts excess spaces" "INSERT 0 1" q "INSERT INTO cn VALUES (2, 'abcde   ', 'x', 'y');"
expect "varchar(3) rejects 4 chars" "value too long for type character varying(3)" \
    q "INSERT INTO cn VALUES (3, 'a', 'wxyz', 'y');"
expect "INSERT varchar(3) with trailing spaces" "INSERT 0 1" q "INSERT INTO cn VALUES (4, 'q', 'a  ', 'y');"
expect_len "varchar(3) keeps trailing spaces" 4 "SELECT b FROM cn WHERE id = 4;"
expect "char(n) counts codepoints" "INSERT 0 1" q "INSERT INTO cn VALUES (5, 'héllo', 'é', 'é');"
expect_len "multibyte char(5) is 5 codepoints, no padding" 7 "SELECT a FROM cn WHERE id = 5;"
expect "padded literal in WHERE matches stripped store" "(1 row)" q "SELECT id FROM cn WHERE a = 'ab   ';"
expect "unpadded literal in WHERE matches too" "(1 row)" q "SELECT id FROM cn WHERE a = 'ab';"
expect "too-long literal in WHERE matches nothing" "(0 rows)" q "SELECT id FROM cn WHERE a = 'abcdefgh';"
expect "UPDATE SET into char(5)" "UPDATE 1" q "UPDATE cn SET a = 'up   ' WHERE id = 1;"
expect_len "updated char(5) padded" 6 "SELECT a FROM cn WHERE id = 1;"
expect "UPDATE SET too long is 22001" "value too long for type character(5)" \
    q "UPDATE cn SET a = 'toolong' WHERE id = 1;"
expect "concat into varchar(3) is length-checked" "value too long for type character varying(3)" \
    q "UPDATE cn SET b = b || 'zz' WHERE id = 1;"
expect "UNIQUE index on char(n)" "CREATE INDEX" q "CREATE UNIQUE INDEX cn_a ON cn (a);"
expect "unique lookup unpadded" "(1 row)" q "SELECT id FROM cn WHERE a = 'héllo';"
expect "duplicate under padding is a violation" "duplicate key" q "INSERT INTO cn VALUES (6, 'up', 'b', 'c');"
expect "DROP char table" "DROP TABLE" q "DROP TABLE cn;"

# --- date: YYYY-MM-DD in, YYYY-MM-DD out, equality and index on it -------
expect "CREATE table with a date column" "CREATE TABLE" \
    q "CREATE TABLE dt (id int PRIMARY KEY, d date, when_ts timestamp);"
expect "INSERT date literal" "INSERT 0 1" q "INSERT INTO dt VALUES (1, '1996-03-13', '1996-03-13 10:11:12');"
expect "INSERT leap day" "INSERT 0 1" q "INSERT INTO dt VALUES (2, '2000-02-29', NULL);"
expect "INSERT pre-epoch date" "INSERT 0 1" q "INSERT INTO dt VALUES (3, '1969-12-31', NULL);"
expect "date reads back ISO" "1996-03-13" v "SELECT d FROM dt WHERE id = 1;"
expect "leap day reads back" "2000-02-29" v "SELECT d FROM dt WHERE id = 2;"
expect "pre-epoch reads back" "1969-12-31" v "SELECT d FROM dt WHERE id = 3;"
expect "invalid date is 22P02" "invalid value for column type" q "INSERT INTO dt VALUES (4, '2001-02-29', NULL);"
expect "malformed date is 22P02" "invalid value for column type" q "INSERT INTO dt VALUES (4, '13/03/1996', NULL);"
expect "WHERE on a date" "(1 row)" q "SELECT id FROM dt WHERE d = '2000-02-29';"
expect "UPDATE a date" "UPDATE 1" q "UPDATE dt SET d = '2001-01-01' WHERE id = 3;"
expect "updated date reads back" "2001-01-01" v "SELECT d FROM dt WHERE id = 3;"
expect "index on a date column" "CREATE INDEX" q "CREATE INDEX dt_d ON dt (d);"
expect "indexed date lookup" "(1 row)" q "SELECT id FROM dt WHERE d = '2001-01-01';"
expect "DROP date table" "DROP TABLE" q "DROP TABLE dt;"

# --- uuid: alternate input, canonical output, equality and index -------
uuid_input='{00112233445566778899AABBCCDDEEFF}'
uuid_text='00112233-4455-6677-8899-aabbccddeeff'
uuid_next='ffeeddcc-bbaa-9988-7766-554433221100'
expect "CREATE table with a UUID column" "CREATE TABLE" \
    q "CREATE TABLE ut (id int PRIMARY KEY, uid uuid);"
expect "INSERT alternate UUID spelling" "INSERT 0 1" q "INSERT INTO ut VALUES (1, '$uuid_input');"
expect "UUID reads back canonically" "^$uuid_text$" v "SELECT uid FROM ut WHERE id = 1;"
expect "WHERE accepts equivalent UUID spelling" "^1$" v "SELECT id FROM ut WHERE uid = '$uuid_text';"
expect "malformed UUID is 22P02" "22P02:" \
    psql "$E" -X -v VERBOSITY=verbose \
    -c "INSERT INTO ut VALUES (2, 'not-a-uuid');"
expect "rejected UUID did not insert a row" "^1$" v "SELECT count(*) FROM ut;"
expect "index on a UUID column" "CREATE INDEX" q "CREATE INDEX ut_uid ON ut (uid);"
expect "indexed UUID lookup accepts alternate spelling" "^1$" v "SELECT id FROM ut WHERE uid = '$uuid_input';"
expect "UPDATE a UUID" "UPDATE 1" q "UPDATE ut SET uid = '$uuid_next' WHERE id = 1;"
expect "updated UUID reads back" "^$uuid_next$" v "SELECT uid FROM ut WHERE id = 1;"
expect "updated UUID found through index" "^1$" v "SELECT id FROM ut WHERE uid = '$uuid_next';"
expect "old UUID index entry no longer matches" "^0$" v "SELECT count(*) FROM ut WHERE uid = '$uuid_text';"
expect "DROP UUID table" "DROP TABLE" q "DROP TABLE ut;"

# --- SELECT without FROM, SET and RESET: what a driver sends on connect --
expect "SELECT 1" "(1 row)" q "SELECT 1;"
expect "SELECT 1 value" "1" v "SELECT 1;"
expect "SELECT 1 column name" "?column?" q "SELECT 1;"
expect "constants with aliases" "1|a|t||1.5" v \
    "SELECT 1 AS one, 'a' b, true, NULL, 1.5;"
expect "constants keep their aliases" "one | b |" q \
    "SELECT 1 AS one, 'a' b, true, NULL, 1.5;"
expect "SELECT 1 LIMIT 0" "(0 rows)" q "SELECT 1 LIMIT 0;"
expect "a function without FROM errors" "constants" q "SELECT now();"
expect "SET is accepted" "SET" q "SET extra_float_digits = 3;"
expect "RESET is accepted" "RESET" q "RESET ALL;"
# SHOW: the isolation a driver asks about, the connection's settings
# under their canonical names, and 42704 for anything else.
expect "SHOW TRANSACTION ISOLATION LEVEL" "repeatable read" v \
    "SHOW TRANSACTION ISOLATION LEVEL;"
expect "SHOW names its column" "transaction_isolation" q \
    "SHOW transaction_isolation;"
expect "SHOW datestyle value" "ISO, MDY" v "SHOW datestyle;"
expect "SHOW datestyle canonical name" " DateStyle " q "SHOW datestyle;"
expect "SHOW unknown parameter" "unrecognized configuration parameter" q \
    "SHOW no_such_setting;"

# --- NOT NULL: omitted, literal and SET NULL are 23502 -------------------
# The error names the column; a nullable column still takes NULL.
q "CREATE TABLE nn (id int4 PRIMARY KEY, sk int4 NOT NULL, v text);" >/dev/null
expect "NOT NULL: full row inserts" "INSERT 0 1" q \
    "INSERT INTO nn (id, sk, v) VALUES (1, 1, 'a');"
expect "NOT NULL: nullable column takes NULL" "INSERT 0 1" q \
    "INSERT INTO nn (id, sk) VALUES (2, 2);"
expect "NOT NULL: omitted column rejected" 'column "sk"' q \
    "INSERT INTO nn (id, v) VALUES (3, 'c');"
expect "NOT NULL: literal NULL rejected" 'column "sk"' q \
    "INSERT INTO nn (id, sk, v) VALUES (3, NULL, 'c');"
expect "NOT NULL: SET NULL rejected" 'violates not-null' q \
    "UPDATE nn SET sk = NULL WHERE id = 1;"
expect "NOT NULL: SET NULL on nullable column" "UPDATE 1" q \
    "UPDATE nn SET v = NULL WHERE id = 1;"
expect "NOT NULL: rejected rows never landed" "2" v "SELECT count(*) FROM nn;"
# The primary key is a unique index: ranges over it read the index.
expect "unique index range: id >= 2" "1" v "SELECT count(*) FROM nn WHERE id >= 2;"
expect "unique index range: id < 2" "1" v "SELECT count(*) FROM nn WHERE id < 2;"
expect "unique index range: 1 < id AND id <= 2" "2" \
    v "SELECT id FROM nn WHERE 1 < id AND id <= 2;"
expect "unique index range: none above" "0" v "SELECT count(*) FROM nn WHERE id > 5;"
q "DROP TABLE nn;" >/dev/null

# --- ON CONFLICT: DO NOTHING, DO UPDATE with EXCLUDED and a WHERE -----------
q "CREATE TABLE oc (id int4 PRIMARY KEY, sk int4, val text);" >/dev/null
q "CREATE INDEX oc_sk ON oc (sk);" >/dev/null
expect "ON CONFLICT: a new row inserts" "INSERT 0 1" \
    q "INSERT INTO oc VALUES (1, 1, 'a') ON CONFLICT (id) DO NOTHING;"
expect "ON CONFLICT DO NOTHING: the duplicate is skipped" "INSERT 0 0" \
    q "INSERT INTO oc VALUES (1, 1, 'dup') ON CONFLICT (id) DO NOTHING;"
expect "ON CONFLICT DO NOTHING: the row kept its value" "a" \
    v "SELECT val FROM oc WHERE id = 1;"
expect "ON CONFLICT DO UPDATE: the upstream append" "INSERT 0 1" \
    q "INSERT INTO oc AS t (id, sk, val) VALUES (1, 1, 'x')
       ON CONFLICT (id) DO UPDATE SET val = CONCAT(t.val, ',', EXCLUDED.val)
       WHERE t.id = 1;"
expect "ON CONFLICT DO UPDATE: appended" "a,x" v "SELECT val FROM oc WHERE id = 1;"
expect "ON CONFLICT DO UPDATE: a false WHERE skips" "INSERT 0 0" \
    q "INSERT INTO oc AS t VALUES (1, 1, 'no')
       ON CONFLICT (id) DO UPDATE SET val = 'no' WHERE t.sk = 99;"
expect "ON CONFLICT DO UPDATE: skipped row unchanged" "a,x" \
    v "SELECT val FROM oc WHERE id = 1;"
expect "ON CONFLICT: one statement, one update and one insert" "INSERT 0 2" \
    q "INSERT INTO oc VALUES (1, 1, 'y'), (2, 2, 'b')
       ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;"
expect "ON CONFLICT: the update took EXCLUDED" "y" v "SELECT val FROM oc WHERE id = 1;"
expect "ON CONFLICT: the insert landed" "b" v "SELECT val FROM oc WHERE id = 2;"
expect "ON CONFLICT DO UPDATE moves an index entry" "INSERT 0 1" \
    q "INSERT INTO oc VALUES (1, 5, 'z') ON CONFLICT (id) DO UPDATE SET sk = EXCLUDED.sk;"
expect "ON CONFLICT: new index key finds the row" "1" v "SELECT id FROM oc WHERE sk = 5;"
expect "ON CONFLICT: old index key is gone" "(0 rows)" q "SELECT id FROM oc WHERE sk = 1;"
expect "ON CONFLICT DO NOTHING without a target" "INSERT 0 0" \
    q "INSERT INTO oc VALUES (2, 9, 'dup') ON CONFLICT DO NOTHING;"
expect "ON CONFLICT: still two rows" "2" v "SELECT count(*) FROM oc;"
expect "ON CONFLICT: inside a block, pending-aware" "COMMIT" tx "BEGIN;
INSERT INTO oc VALUES (3, 3, 'c');
INSERT INTO oc VALUES (3, 3, 'cc') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;
COMMIT;"
expect "ON CONFLICT: the block's own insert was updated" "cc" \
    v "SELECT val FROM oc WHERE id = 3;"
expect "ON CONFLICT: parameters in simple mode (cast literals)" "INSERT 0 1" \
    q "INSERT INTO oc VALUES ('3'::int4, '7'::int4, 'p'::text)
       ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE oc.id = '3'::int4;"
expect "ON CONFLICT: no unique constraint on the target" \
    "no unique or exclusion constraint" \
    q "INSERT INTO oc VALUES (4, 4, 'd') ON CONFLICT (sk) DO NOTHING;"
expect "ON CONFLICT: the same row twice in one statement" "a second time" \
    q "INSERT INTO oc VALUES (3, 3, 'e'), (3, 3, 'f')
       ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;"
expect "ON CONFLICT DO UPDATE: a new key twice in one statement" \
    "a second time" \
    q "INSERT INTO oc VALUES (7, 7, 'h'), (7, 7, 'i')
       ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;"
expect "ON CONFLICT DO NOTHING: a new key twice inserts the first" \
    "INSERT 0 1" \
    q "INSERT INTO oc VALUES (7, 7, 'h'), (7, 7, 'i')
       ON CONFLICT (id) DO NOTHING;"
expect "ON CONFLICT DO NOTHING: the first of the pair is the row" "h" \
    v "SELECT val FROM oc WHERE id = 7;"
expect "ON CONFLICT: NOT NULL still holds on the update" "not-null" \
    q "INSERT INTO oc VALUES (3, 3, 'g') ON CONFLICT (id) DO UPDATE SET id = NULL;"
expect "ON CONFLICT: failed statements changed nothing" "p" \
    v "SELECT val FROM oc WHERE id = 3;"
expect "DROP the ON CONFLICT table" "DROP TABLE" q "DROP TABLE oc;"

# --- numeric: exact decimals, rounded to the column's scale ---------------
q "CREATE TABLE nm (id int4 PRIMARY KEY, price numeric(10, 2), raw numeric);" \
    >/dev/null
expect "numeric: rounded to the column's scale, half away from zero" \
    "1.01" v "INSERT INTO nm VALUES (1, 1.005, 0.321) RETURNING price;"
expect "numeric: a bare numeric keeps the value's scale" "0.321" \
    v "SELECT raw FROM nm WHERE id = 1;"
expect "numeric: trailing zeros of the scale are printed" "12.50" \
    v "INSERT INTO nm VALUES (2, '12.5', 1e3) RETURNING price;"
expect "numeric: an exponent literal" "1000" v "SELECT raw FROM nm WHERE id = 2;"
expect "numeric: past the precision is 22003" "out of range" \
    q "INSERT INTO nm VALUES (3, 123456789.99, 0);"
expect "numeric: NaN is not a value" "invalid" \
    q "INSERT INTO nm VALUES (3, 'NaN', 0);"
expect "numeric: an integer literal into numeric" "7.00" \
    v "INSERT INTO nm VALUES (3, 7, 7) RETURNING price;"
expect "numeric: WHERE compares exactly" "2" \
    v "SELECT id FROM nm WHERE price = 12.5;"
expect "numeric: ordered comparison" "2" \
    v "SELECT count(*) FROM nm WHERE price > 1.01;"
expect "numeric: column against column" "1" \
    v "SELECT count(*) FROM nm WHERE price = raw;"
expect "numeric: arithmetic in SET is exact" "13.60" \
    v "UPDATE nm SET price = price + 1.1 WHERE id = 2 RETURNING price;"
expect "numeric: a numeric plus an integer stays exact" "1.321" \
    v "SELECT raw + 1 FROM nm WHERE id = 1;"
expect "numeric: division keeps 16 fractional digits" "333.3333333333333333" \
    v "SELECT raw / 3 FROM nm WHERE id = 2;"
expect "numeric: ROUND to places pads the scale" "0.32" \
    v "SELECT round(raw, 2) FROM nm WHERE id = 1;"
expect "numeric: ROUND to a negative place" "1000" \
    v "SELECT round(raw, -2) FROM nm WHERE id = 2;"
expect "numeric: a decimal literal is numeric, the scales add" "0.6420" \
    v "SELECT raw * 2.0 FROM nm WHERE id = 1;"
expect "numeric: 0.1 + 0.2 is exactly 0.3" "0.3" \
    v "SELECT 0.1 + 0.2 FROM nm WHERE id = 1;"
expect "numeric: an index on a numeric column" "CREATE INDEX" \
    q "CREATE INDEX nm_price ON nm (price);"
expect "numeric: the index answers a range" "1" \
    v "SELECT count(*) FROM nm WHERE price >= 10 AND price < 20;"
expect "numeric: the index answers equality across scales" "2" \
    v "SELECT id FROM nm WHERE price = 13.6;"
expect "DROP the numeric table" "DROP TABLE" q "DROP TABLE nm;"

# --- RETURNING: the written rows, projected as a SELECT's ----------------
q "CREATE TABLE rt (id int4 PRIMARY KEY, v text, n int8);" >/dev/null
expect "INSERT RETURNING *" "1|a|10" \
    v "INSERT INTO rt VALUES (1, 'a', 10) RETURNING *;"
expect "INSERT RETURNING one row per VALUES row" "(2 rows)" \
    q "INSERT INTO rt VALUES (2, 'b', 20), (3, 'c', 30) RETURNING id;"
expect "INSERT RETURNING an expression with its alias" "next" \
    q "INSERT INTO rt VALUES (4, 'd', 40) RETURNING id + 1 AS next;"
expect "INSERT RETURNING the expression's value" "6" \
    v "INSERT INTO rt VALUES (5, 'e', 50) RETURNING id + 1 AS next;"
expect "INSERT RETURNING egid is the assigned id" "^[0-9][0-9]*$" \
    v "INSERT INTO rt VALUES (6, 'f', 60) RETURNING egid;"
expect "UPDATE RETURNING the row as rewritten" "1|A|11" \
    v "UPDATE rt SET v = upper(v), n = n + 1 WHERE id = 1 RETURNING id, v, n;"
expect "UPDATE RETURNING every matched row" "(3 rows)" \
    q "UPDATE rt SET n = 0 WHERE id > 3 RETURNING id;"
expect "DELETE RETURNING the row as it was" "2|b|20" \
    v "DELETE FROM rt WHERE id = 2 RETURNING *;"
expect "DELETE RETURNING removed it" "0" \
    v "SELECT count(*) FROM rt WHERE id = 2;"
expect "RETURNING an unknown column is 42703" "does not exist" \
    q "INSERT INTO rt VALUES (9, 'z', 0) RETURNING nope;"
expect "RETURNING 42703 wrote nothing" "0" \
    v "SELECT count(*) FROM rt WHERE id = 9;"
expect "ON CONFLICT DO UPDATE RETURNING the updated row" "1|A|99" \
    v "INSERT INTO rt VALUES (1, 'x', 99) ON CONFLICT (id) DO UPDATE
       SET n = EXCLUDED.n RETURNING *;"
expect "ON CONFLICT DO NOTHING RETURNING skips the row" "(0 rows)" \
    q "INSERT INTO rt VALUES (1, 'x', 0) ON CONFLICT (id) DO NOTHING
       RETURNING id;"
expect "ON CONFLICT DO NOTHING RETURNING keeps the new one" "7" \
    v "INSERT INTO rt VALUES (1, 'x', 0), (7, 'g', 70)
       ON CONFLICT (id) DO NOTHING RETURNING id;"
expect "RETURNING inside a block" "8" tx "BEGIN;
INSERT INTO rt VALUES (8, 'h', 80) RETURNING id;
COMMIT;"
expect "RETURNING in a block is visible after COMMIT" "h" \
    v "SELECT v FROM rt WHERE id = 8;"
expect "RETURNING an aggregate is refused" "RETURNING" \
    q "DELETE FROM rt RETURNING count(*);"
expect "DROP the RETURNING table" "DROP TABLE" q "DROP TABLE rt;"

# --- savepoints: ROLLBACK TO revives a failed block and drops its writes --
q "CREATE TABLE sp (id int4 PRIMARY KEY, v text);" >/dev/null
q "CREATE INDEX sp_v ON sp (v);" >/dev/null
expect "savepoint: duplicate key inside, rolled back to, block commits" \
    "COMMIT" tx "BEGIN;
INSERT INTO sp VALUES (1, 'a');
SAVEPOINT upsert;
INSERT INTO sp VALUES (1, 'dup');
ROLLBACK TO SAVEPOINT upsert;
INSERT INTO sp VALUES (2, 'b');
COMMIT;"
expect "savepoint: both inserts around the failure landed" "2" \
    v "SELECT count(*) FROM sp;"
expect "savepoint: the failed insert did not" "a" \
    v "SELECT v FROM sp WHERE id = 1;"
expect "savepoint: nested, ROLLBACK TO the outer drops both" "COMMIT" \
    tx "BEGIN;
SAVEPOINT a;
INSERT INTO sp VALUES (5, 'e');
SAVEPOINT b;
INSERT INTO sp VALUES (6, 'f');
ROLLBACK TO a;
COMMIT;"
expect "savepoint: nothing from the rewound part" "2" \
    v "SELECT count(*) FROM sp;"
expect "savepoint: an UPDATE before it stays, one after it goes" "COMMIT" \
    tx "BEGIN;
UPDATE sp SET v = 'z' WHERE id = 1;
SAVEPOINT s;
UPDATE sp SET v = 'y' WHERE id = 1;
ROLLBACK TO s;
COMMIT;"
expect "savepoint: the kept update, through the index" "1" \
    v "SELECT id FROM sp WHERE v = 'z';"
expect "savepoint: the dropped update's index entry is gone" "(0 rows)" \
    q "SELECT id FROM sp WHERE v = 'y';"
expect "savepoint: RELEASE keeps the writes" "COMMIT" tx "BEGIN;
SAVEPOINT s;
INSERT INTO sp VALUES (3, 'c');
RELEASE SAVEPOINT s;
COMMIT;"
expect "savepoint: released savepoint's insert landed" "c" \
    v "SELECT v FROM sp WHERE id = 3;"
expect "savepoint: ROLLBACK TO an unknown name" 'savepoint "nope" does not exist' \
    tx "BEGIN;
INSERT INTO sp VALUES (4, 'd');
ROLLBACK TO nope;
COMMIT;"
expect "savepoint: that error failed the block" "3" v "SELECT count(*) FROM sp;"
expect "savepoint: outside a block" "can only be used in transaction blocks" \
    q "SAVEPOINT s;"
expect "savepoint: a failed block without one stays failed" \
    "commands ignored until end" tx "BEGIN;
INSERT INTO sp VALUES (1, 'dup');
INSERT INTO sp VALUES (7, 'g');
COMMIT;"
expect "savepoint: plain failure rolled back" "3" v "SELECT count(*) FROM sp;"
expect "DROP the savepoint table" "DROP TABLE" q "DROP TABLE sp;"

# --- extended query protocol: psql 18's \bind, \parse, \bind_named --------
# `\bind` sends Parse (unnamed) + Bind + Describe(portal) + Execute + Sync,
# the shape pgbench -M extended uses; `\parse` + `\bind_named` is a named
# statement reused across Binds, pgbench -M prepared's shape. psql reads
# these from stdin one line at a time, so the helpers take a script.
x() { psql "$E" -X -tA <<<"$1"; }  # bare values
xq() { psql "$E" -X <<<"$1"; }     # tags and headers too
expect "CREATE table for the extended path" "CREATE TABLE" \
    q "CREATE TABLE pt (id int PRIMARY KEY, name text, n int8);"
expect "extended INSERT with params" "INSERT 0 2" \
    xq 'INSERT INTO pt VALUES ($1, $2, $3), ($4, $5, 0) \bind 1 a 10 2 b \g'
expect "extended SELECT with a param" "b" \
    x 'SELECT name FROM pt WHERE id = $1 \bind 2 \g'
expect "extended SELECT header comes from Describe" "name" \
    xq 'SELECT name FROM pt WHERE id = $1 \bind 2 \g'
expect "extended SELECT row count tag" "(1 row)" \
    xq 'SELECT name FROM pt WHERE id = $1 \bind 2 \g'
expect "extended count(*)" "2" x 'SELECT count(*) FROM pt \bind \g'
expect "extended SELECT 1" "1" x 'SELECT 1 \bind \g'
expect "extended SELECT 1 header comes from Describe" "?column?" \
    xq 'SELECT 1 \bind \g'
expect "extended SET" "SET" xq 'SET application_name = e2e \bind \g'
expect "extended UPDATE, param under an operator" "UPDATE 1" \
    xq 'UPDATE pt SET n = n + $1 WHERE name = $2 \bind 5 a \g'
expect "the UPDATE landed" "15" x 'SELECT n FROM pt WHERE id = $1 \bind 1 \g'
expect "extended UPDATE, param at the root" "UPDATE 1" \
    xq 'UPDATE pt SET name = $1 WHERE id = $2 \bind z 2 \g'
expect "highest index counts, lower ones unused" "z" \
    x 'SELECT name FROM pt WHERE id = $2 \bind 99 2 \g'
expect "extended DELETE with a param" "DELETE 1" \
    xq 'DELETE FROM pt WHERE id = $1 \bind 2 \g'
expect "the DELETE landed" "1" x 'SELECT count(*) FROM pt \bind \g'
expect "extended INSERT RETURNING header comes from Describe" "name" \
    xq 'INSERT INTO pt VALUES ($1, $2, 0) RETURNING name \bind 3 c \g'
expect "extended UPDATE RETURNING an expression" "c!" \
    x 'UPDATE pt SET n = $1 WHERE id = $2 RETURNING name || $3 \bind 1 3 ! \g'
expect "extended DELETE RETURNING with a param" "3" \
    x 'DELETE FROM pt WHERE id = $1 RETURNING id \bind 3 \g'
expect "too few params is 08P01" \
    "bind message supplies 0 parameters, but prepared statement requires 1" \
    x 'SELECT name FROM pt WHERE id = $1 \bind \g'
expect "too many params is 08P01" \
    "bind message supplies 2 parameters, but prepared statement requires 1" \
    x 'SELECT name FROM pt WHERE id = $1 \bind 1 2 \g'
expect "a param that does not parse for its column" "invalid value" \
    x 'SELECT name FROM pt WHERE id = $1 \bind abc \g'
expect "\$0 is rejected" "there is no parameter \$0" \
    x 'SELECT name FROM pt WHERE id = $0 \bind \g'
expect "syntax error over the extended path" "syntax error" \
    x 'SELEC 1 \bind \g'
expect "Describe of a missing table" "table does not exist" \
    x 'SELECT * FROM nope \bind \g'
expect "DDL over the extended path" "CREATE TABLE" \
    xq 'CREATE TABLE px (id int) \bind \g'
expect "DROP over the extended path" "DROP TABLE" \
    xq 'DROP TABLE px \bind \g'

# A named statement: parsed once, bound three times, closed, then gone.
named_out=$(x 'SELECT name FROM pt WHERE id = $1 \parse s1
\bind_named s1 1 \g
INSERT INTO pt VALUES (3, $1, 0) \bind c \g
\bind_named s1 3 \g
\bind_named s1 1 \g
\close_prepared s1
\bind_named s1 1 \g' 2>&1)
if [ "$(printf '%s\n' "$named_out" | grep -c '^a$')" = 2 ] &&
    grep -q '^c$' <<<"$named_out" &&
    grep -q 'unknown prepared statement' <<<"$named_out"; then
    ok "named statement: reused across Binds, then closed"
else
    bad "named statement — got: $named_out"
fi
# DEALLOCATE is the SQL spelling of Close: psycopg sends DEALLOCATE ALL
# after every DROP.
dealloc_out=$(x 'SELECT name FROM pt WHERE id = $1 \parse s2
\bind_named s2 1 \g
DEALLOCATE s2;
\bind_named s2 1 \g
SELECT name FROM pt WHERE id = $1 \parse s3
DEALLOCATE ALL;
\bind_named s3 1 \g
DEALLOCATE s3;' 2>&1)
if [ "$(printf '%s\n' "$dealloc_out" | grep -c '^a$')" = 1 ] &&
    [ "$(grep -c 'unknown prepared statement' <<<"$dealloc_out")" = 2 ] &&
    grep -q 'prepared statement does not exist' <<<"$dealloc_out"; then
    ok "DEALLOCATE name and ALL drop prepared statements"
else
    bad "DEALLOCATE — got: $dealloc_out"
fi

# A transaction block over the extended path: an error aborts it, the
# next statement is refused with 25P02 until ROLLBACK.
expect "extended BEGIN/COMMIT" "COMMIT" \
    xq 'BEGIN \bind \g
INSERT INTO pt VALUES ($1, $2, $3) \bind 4 d 0 \g
COMMIT \bind \g'
expect "the block committed" "d" \
    x 'SELECT name FROM pt WHERE id = $1 \bind 4 \g'
expect "error in an extended block aborts it" "current transaction is aborted" \
    x 'BEGIN \bind \g
SELECT * FROM nope \bind \g
SELECT name FROM pt WHERE id = $1 \bind 1 \g
ROLLBACK \bind \g'

# Pipeline: two Executes before one Sync. The first fails, so the
# second is dropped until the Sync; its row never comes back.
pipe_out=$(x '\startpipeline
SELECT * FROM nope \bind \sendpipeline
SELECT name FROM pt WHERE id = $1 \bind 1 \sendpipeline
\endpipeline' 2>&1)
if grep -q 'table does not exist' <<<"$pipe_out" &&
    ! grep -q '^a$' <<<"$pipe_out"; then
    ok "pipeline: an error drops the Executes after it until Sync"
else
    bad "pipeline — got: $pipe_out"
fi
expect "connection is usable after the pipeline" "a" \
    x 'SELECT name FROM pt WHERE id = $1 \bind 1 \g'
expect "DROP the extended-path table" "DROP TABLE" q "DROP TABLE pt;"

# --- no unexpected server-side errors --------------------------------------
if grep -qE '^[^ ]+ ERROR ' "$LOG"; then bad "server log has errors"; else ok "server log clean"; fi

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
