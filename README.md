# 1ctechlog

Streaming Python parser for 1C technological journal files. It reads records without loading the whole log into memory and is intended for large 1C tech logs.

## Quick start

Create a starter config and edit paths/object if needed:

```bash
python3 techlog_parser.py --write-default-config techlog_config.json
python3 techlog_parser.py --config techlog_config.json
```

Example direct run:

```bash
python3 techlog_parser.py /var/log/1c --event DBPOSTGRS,SDBL --object ЖурналДокументов.КадровыеДокументы --since 3600m --last 20 --tail-reverse --output results.txt --save-dir found
```

## Key features

- Streaming parsing with multiline SQL and `planSQLText` preserved as one record.
- Fast raw-text prefilters before full key/value parsing.
- Parallel file workers for broad non-tail searches with `--workers`.
- Search by `SessionID`, `Usr`, exact `Context`, object substring in `Context`, `OSThread`, `ConnectID`, `DBPID`, event type, plain text, or regex.
- File mtime filtering via `--since` and record-time filtering via `--from-time` / `--to-time`.
- `--tail-reverse` for `--last`: reads newest files from the end and stops after enough matches.
- Full SQL and full execution plan are included by default; use `--no-full-text` only when you intentionally want truncated output.
- `--group-sql` groups repeated SQL by normalized fingerprint.
- `--lock-report` prints a TLOCK-focused report with regions, locks, wait connections, user/session, and context.
- `--output` writes formatted results to a file; `--save-dir` saves each raw matched record separately.
- Progress logs go to stderr and show files, records, matches, bytes read, speed, current file, and offset.

## Useful options

```text
--config FILE          load JSON or simple key:value config
--write-default-config FILE
--session-id VALUE     filter by SessionID
--usr VALUE            filter by Usr
--context VALUE        filter by exact Context
--object VALUE         substring search inside Context/object name
--os-thread VALUE      filter by OSThread
--connect-id VALUE     filter by ConnectID
--dbpid VALUE          filter by DBPID
--event VALUE          event type; repeat or use comma-separated values
--since 30m|2h         scan only files modified in the last minutes or hours
--from-time HH:MM      lower bound inside record timestamps
--to-time HH:MM        upper bound inside record timestamps
--text VALUE           substring search in the complete record
--regex VALUE          regex search in the complete record
--last N               print only the last N matching records
--tail-reverse         with --last, read newest files from the end
--group-sql            group identical normalized SQL texts
--lock-report          print a lock-oriented report
--save-dir DIR         write each matching raw record to its own file
--output FILE          write formatted search results to this file instead of stdout
--format text|jsonl    output format
--encoding ENCODING    input encoding; default is utf-8
--progress-interval N  seconds between progress messages; 0 disables logs
--workers N            parallel workers for non-tail searches
```

## Performance tips

- Use `--since 60m` or another time window to skip old files by modification time.
- Use `--event DBPOSTGRS,SDBL` to discard other event types before expensive field parsing.
- Use `--object` or `--text` when possible; the parser first checks the raw record text and only then parses fields.
- Use `--workers N` for broad searches across many files when you do not need `--last`, `--tail-reverse`, `--group-sql`, or `--save-dir`.
- For “last N” searches, combine `--last N --tail-reverse` so the parser reads newest files from the end and stops early.
- Progress is printed to stderr by default every 5 seconds. Disable it with `--progress-interval 0`.

## Verify the script version

If `--object`, `--output`, or newer options are reported as unrecognized arguments, you are running an older copy of the script. Copy the current `techlog_parser.py` over that file and verify:

```bash
python3 techlog_parser.py --help | grep -E -- '--object|--output|--tail-reverse|--group-sql'
python3 techlog_parser.py --version
```
