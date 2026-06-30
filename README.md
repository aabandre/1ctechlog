# 1ctechlog

Streaming Python parser for 1C technological journal files. It reads files record by record and does not load the whole log into memory, so it can be used with very large journals, including 100 GB files.

## Features

- Reads journal files line by line.
- Detects record boundaries by 1C tech-log headers and keeps multiline SQL or `planSQLText` inside the same record.
- Filters by `SessionID`, `Usr`, `Context`, context object name, `OSThread`, `ConnectID`, `DBPID`, event type (`DBPOSTGRS`, `SDBL`, `EXCP`, `SCALL`, and others), substring, or regular expression.
- Can scan only files modified during the last N minutes or hours.
- Prints file name, byte offset, time, event type, `Rows`, full `Context`, SQL, execution plan, `Planning Time`, `Execution Time`, `Buffers`, and `RowsAffected`.
- Saves every matched record into a separate file.
- Can print only the last N matched records.

## Usage

```bash
python3 techlog_parser.py /var/log/1c --event DBPOSTGRS,SDBL --object ЖурналДокументов.КадровыеДокументы --since 30m --last 20 --output results.txt --save-dir found
```

Useful options:

```text
--session-id VALUE     filter by SessionID
--usr VALUE            filter by Usr
--context VALUE        filter by exact Context
--object VALUE         substring search inside Context/object name
--os-thread VALUE      filter by OSThread
--connect-id VALUE     filter by ConnectID
--dbpid VALUE          filter by DBPID
--event VALUE          event type; repeat or use comma-separated values
--since 30m|2h         scan only files modified in the last minutes or hours
--text VALUE           substring search in the complete record
--regex VALUE          regex search in the complete record
--last N               print only the last N matching records
--save-dir DIR         write each matching raw record to its own file
--output FILE          write formatted search results to this file instead of stdout
--glob MASK            file mask when scanning directories; default is *.log
--encoding ENCODING    input encoding; default is utf-8
```

## Verify the script version

If `--object` or `--output` is reported as an unrecognized argument, you are running an older copy of the script. Copy the current `techlog_parser.py` over that file (for example, over `Untitled-4.py`) and verify that help shows both options:

```bash
python3 techlog_parser.py --help | grep -E -- '--object|--output'
python3 techlog_parser.py --version
```
