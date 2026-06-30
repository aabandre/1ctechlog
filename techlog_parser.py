#!/usr/bin/env python3
"""Streaming parser for 1C technological journal files.

The parser keeps only one journal entry in memory and therefore can be used on
very large logs.  It understands 1C tech-log records that start with a timestamp
header such as ``12:34.567890-0,DBPOSTGRS,...`` or
``20260630 12:34:56.123456-0,SDBL,...`` and treats all following lines as part
of the same record until the next header is found.
"""
from __future__ import annotations

import argparse
import fnmatch
import re
import sys
from collections import deque
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Deque, Iterator, Optional, Pattern, Sequence

HEADER_RE = re.compile(
    r"^(?P<time>(?:\d{8}\s+)?\d{1,2}:\d{2}(?::\d{2})?(?:\.\d+)?)-(?P<duration>\d+),(?P<event>[A-Z0-9_]+)(?:,|$)"
)
KEY_VALUE_RE = re.compile(r"(?:^|,)(?P<key>[A-Za-z][A-Za-z0-9_:]*)=(?P<value>\"(?:[^\"]|\"\")*\"|'(?:[^']|'')*'|[^,\r\n]*)", re.MULTILINE)
METRIC_PATTERNS = {
    "Planning Time": re.compile(r"Planning\s+Time\s*[:=]\s*([^,\"\r\n]+)", re.IGNORECASE),
    "Execution Time": re.compile(r"Execution\s+Time\s*[:=]\s*([^,\"\r\n]+)", re.IGNORECASE),
    "Buffers": re.compile(r"Buffers\s*[:=]\s*([^,\"\r\n]+)", re.IGNORECASE),
    "RowsAffected": re.compile(r"RowsAffected\s*=?\s*([^,\r\n]+)", re.IGNORECASE),
}
SQL_KEYS = ("Sql", "SQL", "sql", "SqlText", "SQLText", "Text")
PLAN_KEYS = ("planSQLText", "PlanSQLText", "Plan", "plan", "QueryPlan")
VERSION = "1.2.0"


@dataclass(frozen=True)
class Filters:
    session_id: Optional[str] = None
    usr: Optional[str] = None
    context: Optional[str] = None
    os_thread: Optional[str] = None
    connect_id: Optional[str] = None
    dbpid: Optional[str] = None
    event_types: frozenset[str] = frozenset()
    text: Optional[str] = None
    regex: Optional[Pattern[str]] = None
    object_name: Optional[str] = None


@dataclass
class Record:
    file: Path
    offset: int
    time: str
    event: str
    text: str


def _unquote(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] == '"':
        return value[1:-1].replace('""', '"')
    if len(value) >= 2 and value[0] == value[-1] == "'":
        return value[1:-1].replace("''", "'")
    return value


def iter_records(path: Path, encoding: str = "utf-8", errors: str = "replace") -> Iterator[Record]:
    """Yield records from *path* while holding only the current record in memory."""
    with path.open("rb") as fh:
        current_offset: Optional[int] = None
        current_header: Optional[re.Match[str]] = None
        chunks: list[str] = []

        while True:
            offset = fh.tell()
            raw = fh.readline()
            if not raw:
                break
            line = raw.decode(encoding, errors=errors)
            header = HEADER_RE.match(line)
            if header:
                if current_header is not None and current_offset is not None:
                    yield Record(path, current_offset, current_header.group("time"), current_header.group("event"), "".join(chunks))
                current_offset = offset
                current_header = header
                chunks = [line]
            elif current_header is not None:
                chunks.append(line)

        if current_header is not None and current_offset is not None:
            yield Record(path, current_offset, current_header.group("time"), current_header.group("event"), "".join(chunks))


def iter_files(paths: Sequence[Path], glob: str, since: Optional[timedelta]) -> Iterator[Path]:
    threshold = datetime.now().timestamp() - since.total_seconds() if since else None
    for base in paths:
        if base.is_file():
            candidates = [base]
        else:
            candidates = (p for p in base.rglob("*") if p.is_file())
        for path in candidates:
            if not fnmatch.fnmatch(path.name, glob):
                continue
            if threshold is not None and path.stat().st_mtime < threshold:
                continue
            yield path


def get_fields(text: str) -> dict[str, str]:
    return {m.group("key"): _unquote(m.group("value")) for m in KEY_VALUE_RE.finditer(text)}


def contains_field(fields: dict[str, str], keys: Sequence[str], needle: Optional[str]) -> bool:
    if needle is None:
        return True
    return any(fields.get(key) == needle for key in keys)


def matches(record: Record, filters: Filters) -> bool:
    if filters.event_types and record.event.upper() not in filters.event_types:
        return False
    fields = get_fields(record.text)
    checks = [
        (("SessionID", "SessionId", "sessionID"), filters.session_id),
        (("Usr", "User", "user"), filters.usr),
        (("Context", "context"), filters.context),
        (("OSThread", "OsThread", "osThread"), filters.os_thread),
        (("ConnectID", "ConnectId", "connectID", "t:connectID"), filters.connect_id),
        (("DBPID", "DbPid", "dbpid"), filters.dbpid),
    ]
    if any(not contains_field(fields, keys, value) for keys, value in checks):
        return False
    if filters.object_name:
        context = first_field(fields, ("Context", "context"))
        if filters.object_name not in context:
            return False
    if filters.text and filters.text not in record.text:
        return False
    if filters.regex and not filters.regex.search(record.text):
        return False
    return True


def first_field(fields: dict[str, str], keys: Sequence[str]) -> str:
    return next((fields[k] for k in keys if k in fields), "")


def metric(text: str, name: str) -> str:
    found = METRIC_PATTERNS[name].search(text)
    return found.group(1).strip() if found else ""


def record_summary(record: Record) -> str:
    fields = get_fields(record.text)
    parts = [
        f"file={record.file}",
        f"offset={record.offset}",
        f"time={record.time}",
        f"event={record.event}",
        f"Rows={fields.get('Rows') or '-'}",
        f"Context={first_field(fields, ('Context', 'context')) or '-'}",
        f"Planning Time={metric(record.text, 'Planning Time') or '-'}",
        f"Execution Time={metric(record.text, 'Execution Time') or '-'}",
        f"Buffers={metric(record.text, 'Buffers') or '-'}",
        f"RowsAffected={fields.get('RowsAffected') or metric(record.text, 'RowsAffected') or '-'}",
    ]
    sql = first_field(fields, SQL_KEYS)
    plan = first_field(fields, PLAN_KEYS)
    return "\n".join(parts) + "\nSQL:\n" + (sql or "-") + "\nPlan:\n" + (plan or "-")


def save_record(record: Record, out_dir: Path, index: int) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_event = re.sub(r"[^A-Za-z0-9_.-]+", "_", record.event)
    name = f"{index:06d}_{record.file.stem}_{record.offset}_{safe_event}.log"
    target = out_dir / name
    target.write_text(record.text, encoding="utf-8")
    return target


def parse_duration(value: Optional[str]) -> Optional[timedelta]:
    if not value:
        return None
    m = re.fullmatch(r"(\d+)([mh])", value.strip().lower())
    if not m:
        raise argparse.ArgumentTypeError("duration must look like 30m or 2h")
    amount = int(m.group(1))
    return timedelta(minutes=amount) if m.group(2) == "m" else timedelta(hours=amount)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stream-search 1C technological journal files.")
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    parser.add_argument("paths", nargs="+", type=Path, help="log files or directories")
    parser.add_argument("--glob", default="*.log", help="file name mask for directory scans (default: *.log)")
    parser.add_argument("--since", type=parse_duration, help="only files modified in the last N minutes/hours, e.g. 30m or 2h")
    parser.add_argument("--event", action="append", help="event type; may be repeated or comma-separated")
    parser.add_argument("--session-id")
    parser.add_argument("--usr")
    parser.add_argument("--context")
    parser.add_argument("--object", dest="object_name", help="substring to search inside Context, e.g. ЖурналДокументов.КадровыеДокументы")
    parser.add_argument("--os-thread")
    parser.add_argument("--connect-id")
    parser.add_argument("--dbpid")
    parser.add_argument("--text", help="substring to search in the complete record")
    parser.add_argument("--regex", help="regular expression to search in the complete record")
    parser.add_argument("--last", type=int, help="print only the last N matching records")
    parser.add_argument("--save-dir", type=Path, help="save each matching raw record to this directory")
    parser.add_argument("--output", type=Path, help="write the formatted search results to this file instead of stdout")
    parser.add_argument("--encoding", default="utf-8", help="input encoding (default: utf-8)")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    events = frozenset(e.strip().upper() for item in (args.event or []) for e in item.split(",") if e.strip())
    filters = Filters(args.session_id, args.usr, args.context, args.os_thread, args.connect_id, args.dbpid, events, args.text, re.compile(args.regex, re.DOTALL) if args.regex else None, args.object_name)
    buffer: Deque[Record] | None = deque(maxlen=args.last) if args.last else None
    count = 0
    if args.output and args.output.parent != Path("."):
        args.output.parent.mkdir(parents=True, exist_ok=True)
    output_context = args.output.open("w", encoding="utf-8") if args.output else nullcontext(sys.stdout)
    with output_context as output:
        for path in iter_files(args.paths, args.glob, args.since):
            for record in iter_records(path, args.encoding):
                if not matches(record, filters):
                    continue
                count += 1
                if args.save_dir:
                    save_record(record, args.save_dir, count)
                if buffer is not None:
                    buffer.append(record)
                else:
                    print(record_summary(record), end="\n\n", file=output)
        if buffer is not None:
            for record in buffer:
                print(record_summary(record), end="\n\n", file=output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
