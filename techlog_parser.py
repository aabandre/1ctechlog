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
import json
import re
import sys
import time
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Deque, Iterator, Optional, Pattern, Sequence

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
VERSION = "2.0.0"
DEFAULT_CONFIG = {
    "paths": ["."],
    "glob": "*.log",
    "since": "3600m",
    "event": ["DBPOSTGRS", "SDBL", "TLOCK", "EXCP"],
    "object": "ЖурналДокументов.КадровыеДокументы",
    "last": 20,
    "tail_reverse": True,
    "from_time": "00:00",
    "to_time": "23:59:59",
    "output": "results.txt",
    "save_dir": "found",
    "progress_interval": 5,
    "full_text": True,
    "format": "text",
    "group_sql": False,
    "lock_report": False,
}


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


def iter_records(
    path: Path,
    encoding: str = "utf-8",
    errors: str = "replace",
    progress: Optional[Callable[[Path, int], None]] = None,
) -> Iterator[Record]:
    """Yield records from *path* while holding only the current record in memory."""
    with path.open("rb") as fh:
        current_offset: Optional[int] = None
        current_header: Optional[re.Match[str]] = None
        chunks: list[str] = []
        line_count = 0

        while True:
            offset = fh.tell()
            raw = fh.readline()
            if not raw:
                break
            line_count += 1
            if progress is not None and line_count % 10000 == 0:
                progress(path, fh.tell())
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

        if progress is not None:
            progress(path, fh.tell())
        if current_header is not None and current_offset is not None:
            yield Record(path, current_offset, current_header.group("time"), current_header.group("event"), "".join(chunks))


def iter_lines_reverse(path: Path, block_size: int = 1024 * 1024) -> Iterator[tuple[int, bytes]]:
    """Yield ``(offset, line_bytes)`` from *path* starting at the end of the file."""
    with path.open("rb") as fh:
        fh.seek(0, 2)
        position = fh.tell()
        remainder = b""
        while position > 0:
            read_size = min(block_size, position)
            position -= read_size
            fh.seek(position)
            block = fh.read(read_size) + remainder
            lines = block.splitlines(keepends=True)
            if position > 0:
                remainder = lines[0]
                lines = lines[1:]
            else:
                remainder = b""
            offset = position + len(remainder)
            line_offsets: list[tuple[int, bytes]] = []
            for line in lines:
                line_offsets.append((offset, line))
                offset += len(line)
            yield from reversed(line_offsets)
        if remainder:
            yield 0, remainder


def iter_records_reverse(path: Path, encoding: str = "utf-8", errors: str = "replace") -> Iterator[Record]:
    """Yield records from *path* from newest to oldest without reading the whole file."""
    tail_lines: list[str] = []
    for offset, raw in iter_lines_reverse(path):
        line = raw.decode(encoding, errors=errors)
        header = HEADER_RE.match(line)
        if header:
            yield Record(path, offset, header.group("time"), header.group("event"), line + "".join(reversed(tail_lines)))
            tail_lines = []
        else:
            tail_lines.append(line)


def iter_files(paths: Sequence[Path], glob: str, since: Optional[timedelta], newest_first: bool = False) -> Iterator[Path]:
    threshold = datetime.now().timestamp() - since.total_seconds() if since else None
    found: list[Path] = []
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
            found.append(path)
    if newest_first:
        found.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    for path in found:
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
    if filters.object_name and filters.object_name not in record.text:
        return False
    if filters.text and filters.text not in record.text:
        return False
    if filters.regex and not filters.regex.search(record.text):
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
    return True


def first_field(fields: dict[str, str], keys: Sequence[str]) -> str:
    return next((fields[k] for k in keys if k in fields), "")


def metric(text: str, name: str) -> str:
    found = METRIC_PATTERNS[name].search(text)
    return found.group(1).strip() if found else ""


def parse_record_time(value: str) -> Optional[float]:
    match = re.search(r"(\d{1,2}):(\d{2})(?::(\d{2}))?(?:\.(\d+))?", value)
    if not match:
        return None
    hours = int(match.group(1))
    minutes = int(match.group(2))
    seconds = int(match.group(3) or 0)
    fraction = float("0." + match.group(4)) if match.group(4) else 0.0
    return hours * 3600 + minutes * 60 + seconds + fraction


def normalize_sql(sql: str) -> str:
    sql = re.sub(r"'[^']*'|\"[^\"]*\"", "?", sql)
    sql = re.sub(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b", "?", sql)
    sql = re.sub(r"\b\d+(?:\.\d+)?\b", "?", sql)
    sql = re.sub(r"\s+", " ", sql).strip()
    return sql


def format_lock_report(record: Record) -> str:
    fields = get_fields(record.text)
    return "\n".join([
        f"file={record.file}",
        f"offset={record.offset}",
        f"time={record.time}",
        f"event={record.event}",
        f"SessionID={fields.get('SessionID') or '-'}",
        f"Usr={fields.get('Usr') or '-'}",
        f"OSThread={fields.get('OSThread') or '-'}",
        f"ConnectID={fields.get('ConnectID') or fields.get('t:connectID') or '-'}",
        f"Regions={fields.get('Regions') or '-'}",
        f"Locks={fields.get('Locks') or '-'}",
        f"WaitConnections={fields.get('WaitConnections') or '-'}",
        f"Context={first_field(fields, ('Context', 'context')) or '-'}",
    ])


def record_to_dict(record: Record, full_text: bool = True) -> dict[str, Any]:
    fields = get_fields(record.text)
    sql = first_field(fields, SQL_KEYS)
    plan = first_field(fields, PLAN_KEYS)
    if not full_text:
        sql = (sql[:500] + "...") if len(sql) > 500 else sql
        plan = (plan[:500] + "...") if len(plan) > 500 else plan
    return {
        "file": str(record.file),
        "offset": record.offset,
        "time": record.time,
        "event": record.event,
        "rows": fields.get("Rows"),
        "context": first_field(fields, ("Context", "context")),
        "planning_time": metric(record.text, "Planning Time"),
        "execution_time": metric(record.text, "Execution Time"),
        "buffers": metric(record.text, "Buffers"),
        "rows_affected": fields.get("RowsAffected") or metric(record.text, "RowsAffected"),
        "sql": sql,
        "plan": plan,
    }


def record_summary(record: Record, full_text: bool = True) -> str:
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
    if not full_text:
        sql = (sql[:500] + "...") if len(sql) > 500 else sql
        plan = (plan[:500] + "...") if len(plan) > 500 else plan
    return "\n".join(parts) + "\nSQL:\n" + (sql or "-") + "\nPlan:\n" + (plan or "-")


def save_record(record: Record, out_dir: Path, index: int) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_event = re.sub(r"[^A-Za-z0-9_.-]+", "_", record.event)
    name = f"{index:06d}_{record.file.stem}_{record.offset}_{safe_event}.log"
    target = out_dir / name
    target.write_text(record.text, encoding="utf-8")
    return target


def parse_simple_config_value(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    value = value.strip()
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    if value.isdigit():
        return int(value)
    return value


def read_config(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        data = json.loads(text)
        if not isinstance(data, dict):
            raise ValueError("config JSON must contain an object")
        return data
    data: dict[str, Any] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" in line:
            key, value = line.split(":", 1)
        elif "=" in line:
            key, value = line.split("=", 1)
        else:
            continue
        value = value.strip()
        if value.startswith("[") and value.endswith("]"):
            data[key.strip()] = [item.strip().strip("'\"") for item in value[1:-1].split(",") if item.strip()]
        else:
            data[key.strip()] = parse_simple_config_value(value.strip("'\""))
    return data


def write_default_config(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(DEFAULT_CONFIG, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


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
    parser.add_argument("--config", type=Path, help="JSON or simple key:value config file")
    parser.add_argument("--write-default-config", type=Path, help="write a starter config file and exit")
    parser.add_argument("paths", nargs="*", type=Path, help="log files or directories")
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
    parser.add_argument("--tail-reverse", action="store_true", help="with --last, read newest files from the end and stop after N matches")
    parser.add_argument("--from-time", dest="from_time", help="record time lower bound, e.g. 00:30 or 00:30:00")
    parser.add_argument("--to-time", dest="to_time", help="record time upper bound, e.g. 01:00 or 01:00:00")
    parser.add_argument("--fast", action="store_true", help="prefer raw-text prefilters and shorter output unless --full-text is used")
    parser.add_argument("--full-text", action="store_true", default=True, help="include full SQL and plan text in output (default)")
    parser.add_argument("--no-full-text", dest="full_text", action="store_false", help="truncate SQL and plan text in output")
    parser.add_argument("--group-sql", action="store_true", help="group identical normalized SQL texts instead of printing every record")
    parser.add_argument("--lock-report", action="store_true", help="print a TLOCK-focused report")
    parser.add_argument("--save-dir", type=Path, help="save each matching raw record to this directory")
    parser.add_argument("--output", type=Path, help="write the formatted search results to this file instead of stdout")
    parser.add_argument("--encoding", default="utf-8", help="input encoding (default: utf-8)")
    parser.add_argument("--progress-interval", type=float, default=5.0, help="seconds between progress messages to stderr; use 0 to disable (default: 5)")
    parser.add_argument("--workers", type=int, default=1, help="parallel file workers for non-tail searches (default: 1)")
    parser.add_argument("--format", choices=("text", "jsonl"), default="text", help="output format (default: text)")
    return parser


def apply_config_defaults(parser: argparse.ArgumentParser, config: dict[str, Any]) -> None:
    defaults: dict[str, Any] = {}
    for key, value in config.items():
        dest = {"object": "object_name", "save_dir": "save_dir"}.get(key, key.replace("-", "_"))
        if dest in {"output", "save_dir", "config"} and value:
            value = Path(value)
        elif dest == "paths":
            value = [Path(item) for item in value]
        elif dest == "since" and isinstance(value, str):
            value = parse_duration(value)
        defaults[dest] = value
    parser.set_defaults(**defaults)


def selected_record_iterator(path: Path, args: argparse.Namespace, update_position: Callable[[Path, int], None]) -> Iterator[Record]:
    if args.tail_reverse and args.last:
        yield from iter_records_reverse(path, args.encoding)
    else:
        yield from iter_records(path, args.encoding, progress=update_position)


def time_matches(record: Record, from_seconds: Optional[float], to_seconds: Optional[float]) -> bool:
    record_seconds = parse_record_time(record.time)
    if record_seconds is None:
        return True
    if from_seconds is not None and record_seconds < from_seconds:
        return False
    if to_seconds is not None and record_seconds > to_seconds:
        return False
    return True


def main(argv: Optional[Sequence[str]] = None) -> int:
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--config", type=Path)
    pre_parser.add_argument("--write-default-config", type=Path)
    pre_args, _ = pre_parser.parse_known_args(argv)

    if pre_args.write_default_config:
        write_default_config(pre_args.write_default_config)
        print(f"Wrote starter config to {pre_args.write_default_config}")
        return 0

    parser = build_parser()
    if pre_args.config:
        apply_config_defaults(parser, read_config(pre_args.config))
    args = parser.parse_args(argv)
    if not args.paths:
        parser.error("paths are required unless they are provided by --config")

    events = frozenset(e.strip().upper() for item in (args.event or []) for e in str(item).split(",") if e.strip())
    filters = Filters(args.session_id, args.usr, args.context, args.os_thread, args.connect_id, args.dbpid, events, args.text, re.compile(args.regex, re.DOTALL) if args.regex else None, args.object_name or None)
    buffer: Deque[Record] | None = deque(maxlen=args.last) if args.last and not args.tail_reverse else None
    matched_count = 0
    record_count = 0
    file_count = 0
    bytes_done = 0
    group_counts: Counter[str] = Counter()
    group_examples: dict[str, dict[str, Any]] = {}
    from_seconds = parse_record_time(args.from_time) if args.from_time else None
    to_seconds = parse_record_time(args.to_time) if args.to_time else None
    start_time = time.monotonic()
    last_progress_time = start_time

    def log_progress(path: Path, position: int, *, force: bool = False) -> None:
        nonlocal last_progress_time
        if args.progress_interval <= 0:
            return
        now = time.monotonic()
        if not force and now - last_progress_time < args.progress_interval:
            return
        elapsed = max(now - start_time, 0.001)
        mib_done = (bytes_done + position) / (1024 * 1024)
        speed = mib_done / elapsed
        print(
            f"[progress] files={file_count} records={record_count} matched={matched_count} "
            f"read={mib_done:.1f} MiB speed={speed:.1f} MiB/s current={path} offset={position}",
            file=sys.stderr,
            flush=True,
        )
        last_progress_time = now

    def format_record(record: Record) -> str:
        if args.format == "jsonl":
            return json.dumps(record_to_dict(record, args.full_text), ensure_ascii=False)
        if args.lock_report:
            return format_lock_report(record)
        return record_summary(record, args.full_text)

    if args.output and args.output.parent != Path("."):
        args.output.parent.mkdir(parents=True, exist_ok=True)
    output_context = args.output.open("w", encoding="utf-8") if args.output else nullcontext(sys.stdout)

    def collect_file(path: Path) -> tuple[Path, list[str], int, int]:
        local_records = 0
        local_matches = 0
        rendered: list[str] = []
        for record in iter_records(path, args.encoding):
            local_records += 1
            if not time_matches(record, from_seconds, to_seconds):
                continue
            if not matches(record, filters):
                continue
            local_matches += 1
            rendered.append(format_record(record))
        return path, rendered, local_records, local_matches

    stop_early = False
    with output_context as output:
        if args.workers > 1 and not any((args.last, args.tail_reverse, args.group_sql, args.save_dir)):
            paths = list(iter_files(args.paths, args.glob, args.since))
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = [executor.submit(collect_file, path) for path in paths]
                for future in as_completed(futures):
                    path, rendered, local_records, local_matches = future.result()
                    file_count += 1
                    record_count += local_records
                    matched_count += local_matches
                    bytes_done += path.stat().st_size
                    log_progress(path, path.stat().st_size, force=(file_count == 1))
                    for text in rendered:
                        print(text, end="\n\n" if args.format == "text" else "\n", file=output)
            if args.progress_interval > 0:
                log_progress(Path("<done>"), 0, force=True)
            return 0
        if args.workers > 1:
            print("[warning] --workers > 1 is used only for non-tail searches without --last, --group-sql, or --save-dir; falling back to safe streaming mode", file=sys.stderr)
        for path in iter_files(args.paths, args.glob, args.since, newest_first=args.tail_reverse):
            file_count += 1
            log_progress(path, 0, force=(file_count == 1))
            last_position = 0

            def update_position(current_path: Path, position: int) -> None:
                nonlocal last_position
                last_position = position
                log_progress(current_path, position)

            for record in selected_record_iterator(path, args, update_position):
                record_count += 1
                if not time_matches(record, from_seconds, to_seconds):
                    continue
                if not matches(record, filters):
                    continue
                matched_count += 1
                if args.save_dir:
                    save_record(record, args.save_dir, matched_count)
                if args.group_sql:
                    fields = get_fields(record.text)
                    sql = first_field(fields, SQL_KEYS)
                    fingerprint = normalize_sql(sql) or "<no sql>"
                    group_counts[fingerprint] += 1
                    group_examples.setdefault(fingerprint, record_to_dict(record, args.full_text))
                elif buffer is not None:
                    buffer.append(record)
                else:
                    print(format_record(record), end="\n\n" if args.format == "text" else "\n", file=output)
                if args.tail_reverse and args.last and matched_count >= args.last:
                    stop_early = True
                    break
            bytes_done += last_position or path.stat().st_size
            if stop_early:
                break
        if args.group_sql:
            for fingerprint, count in group_counts.most_common():
                example = group_examples[fingerprint]
                if args.format == "jsonl":
                    print(json.dumps({"count": count, "fingerprint": fingerprint, "example": example}, ensure_ascii=False), file=output)
                else:
                    print(f"count={count}\nfingerprint={fingerprint}\nexample_context={example.get('context') or '-'}\nexample_sql:\n{example.get('sql') or '-'}", end="\n\n", file=output)
        elif buffer is not None:
            for record in buffer:
                print(format_record(record), end="\n\n" if args.format == "text" else "\n", file=output)
    if args.progress_interval > 0:
        log_progress(Path("<done>"), 0, force=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
