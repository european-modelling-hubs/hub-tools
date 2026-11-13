"""Check the weekly ERVISS release; completion is a repository marker file."""
import argparse
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

URL = ("https://raw.githubusercontent.com/EU-ECDC/Respiratory_viruses_weekly_data/"
       "main/data/snapshots/{snapshot_date}_ILIARIRates.csv")
MARKER_DIR = Path(".github/data-storage/erviss-completed")


def get_snapshot_date(today=None):
    today = today or datetime.now(timezone.utc).date()
    return (today - timedelta(days=(today.weekday() - 4) % 7)).isoformat()


def check_release(hub_path, snapshot_date):
    marker = Path(hub_path) / MARKER_DIR / snapshot_date
    # stat distinguishes a missing file from inaccessible/corrupt marker storage.
    try:
        marker.stat()
    except FileNotFoundError:
        pass
    else:
        if not marker.is_file():
            raise OSError(f"Completion marker is not a file: {marker}")
        return "ALREADY_IMPORTED"
    url = URL.format(snapshot_date=snapshot_date)
    try:
        with urlopen(Request(url, method="HEAD"), timeout=30) as response:
            status = response.status
    except HTTPError as exc:
        if exc.code == 404:
            return "NOT_AVAILABLE"
        if exc.code not in (405, 501):
            raise
        # Some servers reject HEAD; stream GET without downloading the CSV.
        try:
            with urlopen(Request(url, headers={"Range": "bytes=0-0"}), timeout=30) as response:
                status = response.status
        except HTTPError as fallback:
            if fallback.code == 404:
                return "NOT_AVAILABLE"
            raise
    if status not in (200, 206):
        raise OSError(f"Unexpected HTTP status: {status}")
    return "AVAILABLE"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hub_path", default="./repo")
    args = parser.parse_args(argv)
    snapshot_date = get_snapshot_date()
    try:
        status = check_release(args.hub_path, snapshot_date)
    except (OSError, URLError, ValueError) as exc:
        print(f"ERVISS check failed: {exc}", file=sys.stderr)
        status = "ERROR"
    output = f"status={status}\nsnapshot_date={snapshot_date}\n"
    print(output, end="")
    if os.getenv("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as stream:
            stream.write(output)
    return 1 if status == "ERROR" else 0


if __name__ == "__main__":
    sys.exit(main())
