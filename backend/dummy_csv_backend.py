#!/usr/bin/env python3

import csv
import io
import json
import sys


def main() -> int:
    payload = json.load(sys.stdin)
    content = payload.get("content", "")

    rows = list(csv.reader(io.StringIO(content)))
    columns = rows[0] if rows else []
    preview = rows[1:6] if len(rows) > 1 else []

    response = {
        "row_count": max(len(rows) - 1, 0) if columns else len(rows),
        "column_count": len(columns),
        "columns": columns,
        "preview": preview,
        "raw_preview": "\n".join(content.splitlines()[:8]),
    }

    json.dump(response, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
