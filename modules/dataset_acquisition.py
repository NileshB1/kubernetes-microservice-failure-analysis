
#Dataset Acquisition - programmatic retrieval of the source data

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

from modules.shared_utils import setup_logging

logger = setup_logging("dataset_acquisition")

GITHUB_REPO = "IntelligentDDS/Nezha"
GITHUB_REF = "main"
GITHUB_TREE_API = f"https://api.github.com/repos/{GITHUB_REPO}/git/trees/{GITHUB_REF}?recursive=1"
RAW_CONTENT_BASE = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_REF}/"

# Nezha ships two measurement roots, and the project needs both:
#
#   construct_data  
#   rca_data
#
BASELINE_ROOT = "construct_data"
FAULT_ROOT = "rca_data"
SOURCE_ROOTS = (BASELINE_ROOT, FAULT_ROOT)

# Ground-truth fault injections, one JSON file per capture date
FAULT_LIST_SUFFIX = "-fault_list.json"

# The metric/ directory mixes two things: one per-pod resource file per pod
POD_METRIC_SUFFIX = "_metric.csv"

# Nezha covers two benchmark applications on two dates each
CAPTURE_NAMESPACES = {
    "2022-08-22": "hipstershop", "2022-08-23": "hipstershop",
    "2023-01-29": "trainticket",   "2023-01-30": "trainticket",
}

# Expected header of each source file kind. Acquisition fails loudly if the
# upstream layout changes
SOURCE_SCHEMAS: dict[str, list[str]] = {
    "trace": [
        "TraceID", "SpanID",  "ParentID", "PodName",
        "OperationName", "StartTimeUnixNano", "EndTimeUnixNano", "Duration",
    ],
    "log": [
        "Timestamp", "TimeUnixNano", "Node", "PodName",
        "Container", "TraceID", "SpanID", "Log",
    ],
    "metric": [
        "Time", "TimeStamp", "PodName", "CpuUsage(m)",
        "CpuUsageRate(%)", "MemoryUsage(Mi)", "MemoryUsageRate(%)",
    ],
}

HTTP_TIMEOUT_SEC = 120
MAX_ATTEMPTS = 3
RETRY_BACKOFF_SEC = 2.0


class AcquisitionError(RuntimeError):
    """Raised when the source dataset cannot be retrieved or is malformed"""


@dataclass
class SourceFile:
    """One file in the upstream dataset"""

    remote_path: str
    size_bytes: int

    @property
    def root(self) -> str:
        """construct_data (baseline) or rca_data (fault windows)."""
        return self.remote_path.split("/")[0]

    @property
    def kind(self) -> str:
        """trace, metric, or log - taken from the directory it sits in"""
        parts = self.remote_path.split("/")
        return parts[2] if len(parts) > 3 else "unknown"

    @property
    def capture_date(self) -> str:
        parts = self.remote_path.split("/")
        return parts[1] if len(parts) > 2 else "unknown"

    @property
    def local_path(self) -> str:
        """Mirror the upstream layout under the local data directory"""
        return self.remote_path


@dataclass
class AcquisitionReport:
    """What acquisition actually produced, for logging and validation"""

    files: list[str] = field(default_factory=list)
    total_bytes: int = 0
    row_counts: dict[str, int] = field(default_factory=dict)
    skipped: list[str] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return sum(self.row_counts.values())

    def summary(self) -> dict:
        return {
            "files": len(self.files), "total_bytes": self.total_bytes,
            "total_rows": self.total_rows, "rows_by_kind": self.row_counts,
            "skipped": len(self.skipped),
        }


# HTTP helpers

def _fetch(url: str, *, binary: bool = False) -> bytes | str:
    """
    GET a URL, retrying transient failures
    """
    # Only ever fetch over HTTPS. Without this, a manifest entry could in
    # principle steer the fetch at file:// and read the local filesystem.
    if not url.startswith("https://"):
        raise AcquisitionError(f"Refusing to fetch a non-HTTPS URL: {url!r}")

    last_error: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            # noqa: S310 on both lines - the https:// scheme is enforced above.
            request = urllib.request.Request(  # noqa: S310
                url, headers={"User-Agent": "k8s-microservice-failure-analysis"}
            )
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SEC) as response:  # noqa: S310
                payload = response.read()
            return payload if binary else payload.decode("utf-8", errors="replace")
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = exc
            if attempt == MAX_ATTEMPTS:
                break
            delay = RETRY_BACKOFF_SEC * attempt
            logger.warning(
                "Fetch attempt %d/%d failed for %s (%s); retrying in %.1fs",
                attempt, MAX_ATTEMPTS, url,
                exc.__class__.__name__, delay,
            )
            time.sleep(delay)

    raise AcquisitionError(f"Could not fetch {url} after {MAX_ATTEMPTS} attempts: {last_error}")


def _is_wanted(remote_path: str) -> bool:
    """Exclude the service-graph summaries that share the metric/ directory."""
    if "/metric/" in remote_path:
        return remote_path.endswith(POD_METRIC_SUFFIX)
    return True


def list_remote_files(
    kinds: tuple[str, ...] = ("trace", "metric", "log"),
    roots: tuple[str, ...] = SOURCE_ROOTS,
) -> list[SourceFile]:
    """
    Query the upstream file manifest through the GitHub API
    """
    logger.info("Querying dataset manifest: %s", GITHUB_TREE_API)
    payload = _fetch(GITHUB_TREE_API)

    try:
        tree = json.loads(payload)["tree"]
    except (json.JSONDecodeError, KeyError) as exc:
        raise AcquisitionError(f"Unexpected manifest format from GitHub: {exc}") from exc

    files = [
        SourceFile(remote_path=entry["path"], size_bytes=entry.get("size", 0))
        for entry in tree
        if entry.get("type") == "blob"
        and any(entry["path"].startswith(f"{root}/") for root in roots)
        and entry["path"].endswith(".csv")
        and any(f"/{kind}/" in entry["path"] for kind in kinds)
        and _is_wanted(entry["path"])
    ]

    if not files:
        raise AcquisitionError(
            f"Manifest contained no CSV files under {roots} for kinds {kinds}. "
            "  The upstream repository layout may have changed."
        )

    total_mb = sum(f.size_bytes for f in files) / 1024 / 1024
    logger.info("Manifest: %d files, %.1f MB across kinds %s", len(files), total_mb, kinds)
    return files


# Download

def download_fault_lists(data_dir: str, capture_dates: tuple[str, ...] | None = None) -> list[str]:
    """
    Fetch the ground-truth fault list for each capture date
    """
    root = Path(data_dir) / FAULT_ROOT
    dates = capture_dates or tuple(CAPTURE_NAMESPACES)
    written: list[str] = []

    for date in dates:
        remote = f"{FAULT_ROOT}/{date}/{date}{FAULT_LIST_SUFFIX}"
        destination = root / date / f"{date}{FAULT_LIST_SUFFIX}"
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            destination.write_bytes(_fetch(RAW_CONTENT_BASE + remote, binary=True))
            written.append(str(destination))
        except AcquisitionError:
            # Not every date is guaranteed to publish a fault list; the
            # pipeline can still run without ground-truth evaluation.
            logger.warning("No ground-truth fault list published for %s", date)

    logger.info("Ground-truth fault lists: %d date(s)", len(written))
    return written


def load_fault_labels(data_dir: str = "data"):
    """
    Read every downloaded fault list into flat records.
    """
    root = Path(data_dir) / FAULT_ROOT
    records: list[dict] = []
    if not root.is_dir():
        return records

    for path in sorted(root.glob(f"*/*{FAULT_LIST_SUFFIX}")):
        capture_date = path.parent.name
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read fault list %s: %s", path, exc)
            continue

        # The file is keyed by hour; each value is a list of injections
        groups = payload.values() if isinstance(payload, dict) else [payload]
        for group in groups:
            for entry in group if isinstance(group, list) else []:
                records.append(
                    { "capture_date": capture_date, "inject_time": entry.get("inject_time"),
                        "inject_timestamp": int(entry.get("inject_timestamp", 0) or 0),
                        "inject_pod": entry.get("inject_pod"),  "inject_type": entry.get("inject_type"),
                    }
                )

    logger.info("Loaded %d ground-truth fault injections", len(records))
    return records


def download_dataset(
    data_dir: str = "data",
    kinds: tuple[str, ...] = ("trace", "metric", "log"),
    capture_dates: tuple[str, ...] | None = None,
    roots: tuple[str, ...] = SOURCE_ROOTS, force: bool = False,
):
    """
    Download the source dataset into `data_dir`, mirroring upstream layout
    """
    root = Path(data_dir)
    root.mkdir(parents=True, exist_ok=True)

    files = list_remote_files(kinds, roots=roots)
    if capture_dates:
        files = [f for f in files if f.capture_date in capture_dates]
        if not files:
            raise AcquisitionError(f"No files matched capture dates {capture_dates}")

    report = AcquisitionReport()
    logger.info("Downloading %d files into %s ...", len(files), root)

    for index, source in enumerate(files, start=1):
        destination = root / source.local_path
        destination.parent.mkdir(parents=True, exist_ok=True)

        if destination.exists() and not force:
            report.files.append(str(destination))
            report.total_bytes += destination.stat().st_size
            report.skipped.append(str(destination))
            continue

        content = _fetch(RAW_CONTENT_BASE + source.remote_path, binary=True)
        destination.write_bytes(content)

        report.files.append(str(destination))
        report.total_bytes += len(content)

        if index % 20 == 0 or index == len(files):
            logger.info("  %d/%d files (%.1f MB)", index, len(files), report.total_bytes / 1024 / 1024)

    if report.skipped:
        logger.info(
            "  %d file(s) already present, left untouched (use --force to refresh)", len(report.skipped)
        )

    _count_rows(root, files, report)
    logger.info("Acquisition complete: %s", report.summary())
    return report


def _count_rows(root: Path, files: list[SourceFile], report: AcquisitionReport) -> None:
    """Record how many data rows landed, per file kind."""
    for source in files:
        path = root / source.local_path
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            # Subtract the header; a file with only a header contributes 0.
            rows = max(0, sum(1 for _ in handle) - 1)
        key = f"{source.root}/{source.kind}"
        report.row_counts[key] = report.row_counts.get(key, 0) + rows



# Validation

def validate_local_dataset(data_dir: str = "data", min_rows: int = 10_000) -> dict:
    """
    Check that the local dataset is present, well-formed, and large enough.
    """
    root = Path(data_dir) / BASELINE_ROOT
    if not root.is_dir():
        raise AcquisitionError(
            f"Source dataset not found at {root}. "
            "Run: python -m modules.dataset_acquisition --data-dir " + str(data_dir)
        )

    findings: dict[str, dict] = {}
    for kind, expected_columns in SOURCE_SCHEMAS.items():
        pattern = f"*/{kind}/*{POD_METRIC_SUFFIX}" if kind == "metric" else f"*/{kind}/*.csv"
        paths = sorted(root.glob(pattern))
        if not paths:
            findings[kind] = {"files": 0, "rows": 0, "problems": [f"no {kind} files found"]}
            continue

        rows = 0
        problems: list[str] = []
        for path in paths:
            with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
                reader = csv.reader(handle)
                header = next(reader, None)
                if header is None:
                    problems.append(f"{path.name}: empty file")
                    continue
                header = [h.strip().lstrip("") for h in header]
                missing = [c for c in expected_columns if c not in header]
                if missing:
                    problems.append(f"{path.name}: missing columns {missing}")
                rows += sum(1 for _ in reader)

        findings[kind] = {"files": len(paths), "rows": rows, "problems": problems}

    total_rows = sum(f["rows"] for f in findings.values())
    all_problems = [p for f in findings.values() for p in f["problems"]]

    for kind, finding in findings.items():
        logger.info(
            "  %-7s %3d files, %9s rows%s",  kind,
            finding["files"], f"{finding['rows']:,}",
            "  PROBLEMS" if finding["problems"] else "",
        )

    if all_problems:
        raise AcquisitionError(
            "Source dataset failed validation:\n" + "\n".join(f"  - {p}" for p in all_problems)
        )

    if total_rows < min_rows:
        raise AcquisitionError(f"Source dataset holds {total_rows:,} rows, below the {min_rows:,} required.")

    logger.info(
        "Dataset validated: %s rows across %d kinds",
        f"{total_rows:,}",  len([f for f in findings.values() if f["files"]]),
    )
    return {"total_rows": total_rows, "by_kind": findings}


def dataset_is_present(data_dir: str = "data") -> bool:
    """True when a previously-acquired dataset is on disk."""
    root = Path(data_dir) / BASELINE_ROOT
    return root.is_dir() and any(root.glob("*/trace/*.csv"))



# CLI
def main(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Acquire and validate the source dataset")
    parser.add_argument(
        "--data-dir", default=os.getenv("DATA_DIR", "data"),
        help="Local directory to download into (default: ./data)",
    )
    parser.add_argument(
        "--validate-only", action="store_true", help="Validate an existing local copy without downloading"
    )
    parser.add_argument("--force", action="store_true", help="Re-download files that already exist locally")
    parser.add_argument(
        "--dates",  nargs="*",
        default=None,  help=f"Restrict to capture dates (available: {', '.join(CAPTURE_NAMESPACES)})",
    )
    parser.add_argument(
        "--min-rows", type=int, default=10_000, help="Minimum total rows required to pass validation"
    )
    parser.add_argument(
        "--kinds",  nargs="*",
        default=["trace", "metric", "log"],
        help="File kinds to acquire (trace metric log)",
    )
    parser.add_argument(
        "--roots",  nargs="*",
        default=list(SOURCE_ROOTS),
        help=f"Measurement roots to acquire (default: {' '.join(SOURCE_ROOTS)})",
    )
    parser.add_argument(
        "--skip-fault-lists", action="store_true", help="Do not download the ground-truth fault lists"
    )
    args = parser.parse_args(argv)

    try:
        dates = tuple(args.dates) if args.dates else None
        if not args.validate_only:
            download_dataset(
                data_dir=args.data_dir,  kinds=tuple(args.kinds),
                capture_dates=dates, roots=tuple(args.roots), force=args.force
            )
            if not args.skip_fault_lists:
                download_fault_lists(args.data_dir, capture_dates=dates)
        validate_local_dataset(args.data_dir, min_rows=args.min_rows)
    except AcquisitionError as exc:
        logger.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
