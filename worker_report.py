#!/usr/bin/env python3
"""
Collect OpenShift worker node resource usage and sync to Google Sheets.

Run this script on each OCP cluster and specify the target worksheet name.
Each cluster writes to its own worksheet in the same spreadsheet.

Usage:
    python3 worker_report.py --sheet "surr sby"
    python3 worker_report.py --sheet "surr jkt"
"""

import argparse
import json
import re
import subprocess
import sys
from typing import Dict, List, Optional, Tuple

import gspread
from google.auth.exceptions import GoogleAuthError
from google.oauth2.service_account import Credentials
from gspread.exceptions import (
    APIError,
    GSpreadException,
    SpreadsheetNotFound,
    WorksheetNotFound,
)


# ---------------------------------------------------------------------------
# Google Sheets configuration
# ---------------------------------------------------------------------------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SERVICE_ACCOUNT_FILE = "/home/ADMINISTRATOR/ivtsvc/worker-report/service-account.json"
SPREADSHEET_ID = "1iU150fVgpwg9zbROho6UjB4p1kufCl7-hJUcnYLbfj8"

# ---------------------------------------------------------------------------
# Collection parameters
# ---------------------------------------------------------------------------
EXCLUDE_NS_PATTERN = r"openshift-.*|kube-system|default"
TOP_N = 10
TOTAL_COLUMNS = 13


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
class PodResource:
    """Container for a single pod's resource request/limit on a node."""

    def __init__(self, namespace: str, pod: str, request: float, limit: float) -> None:
        self.namespace = namespace
        self.pod = pod
        self.request = request
        self.limit = limit


# ---------------------------------------------------------------------------
# OC data collection
# ---------------------------------------------------------------------------
def run_command(command: List[str], description: str) -> str:
    """Run a shell command and return its stdout."""
    try:
        result = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
    except FileNotFoundError:
        print(
            "Command not found: {0}. Ensure it is installed and in PATH.".format(command[0]),
            file=sys.stderr,
        )
        sys.exit(1)
    except OSError as exc:
        print("Failed to execute {0}: {1}".format(description, exc), file=sys.stderr)
        sys.exit(1)

    if result.returncode != 0:
        error_message = result.stderr.strip() or "Unknown error"
        print("{0} failed: {1}".format(description, error_message), file=sys.stderr)
        sys.exit(1)

    return result.stdout


def collect_pods() -> List[dict]:
    """Collect all pod specs from the cluster."""
    print("Collecting pod resource specs...")
    output = run_command(["oc", "get", "pods", "-A", "-o", "json"], "oc get pods")
    try:
        return json.loads(output).get("items", [])
    except json.JSONDecodeError as exc:
        print("Failed to parse pod JSON: {0}".format(exc), file=sys.stderr)
        sys.exit(1)


def collect_worker_nodes() -> List[str]:
    """Collect worker node names from the cluster."""
    print("Collecting worker nodes...")
    output = run_command(["oc", "get", "nodes", "--no-headers"], "oc get nodes")
    nodes: List[str] = []
    for line in output.strip().splitlines():
        parts = line.split()
        if len(parts) >= 3 and ("worker" in parts[0] or "worker" in parts[2]):
            nodes.append(parts[0])
    return sorted(nodes)


def collect_prometheus_cpu() -> Dict[Tuple[str, str], float]:
    """Query Prometheus for real CPU usage per pod (cores)."""
    print("Collecting real CPU usage from Prometheus...")
    query = (
        "sum(rate(container_cpu_usage_seconds_total{"
        'namespace!~"openshift-.*|kube-system|default",'
        'container!="",'
        'image!="",'
        'pod!=""'
        "}[5m])) by (namespace,pod)"
    )
    return _query_prometheus(query, is_cpu=True)


def collect_prometheus_memory() -> Dict[Tuple[str, str], float]:
    """Query Prometheus for real memory usage per pod (MB)."""
    print("Collecting real memory usage from Prometheus...")
    query = (
        "sum(container_memory_working_set_bytes{"
        'namespace!~"openshift-.*|kube-system|default",'
        'container!="",'
        'image!="",'
        'pod!=""'
        "}) by (namespace,pod) / 1024 / 1024"
    )
    return _query_prometheus(query, is_cpu=False)


def _query_prometheus(query: str, is_cpu: bool) -> Dict[Tuple[str, str], float]:
    """Execute a PromQL instant query via the Prometheus pod."""
    try:
        result = subprocess.run(
            [
                "oc", "exec", "-n", "openshift-monitoring",
                "prometheus-k8s-0", "-c", "prometheus", "--",
                "promtool", "query", "instant", "http://localhost:9090",
                query,
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
    except (FileNotFoundError, OSError):
        print("Warning: Could not query Prometheus, real usage will show '-'", file=sys.stderr)
        return {}

    if result.returncode != 0:
        print("Warning: Prometheus query returned non-zero, real usage will show '-'", file=sys.stderr)
        return {}

    usage: Dict[Tuple[str, str], float] = {}
    ns_re = re.compile(r'namespace="([^"]+)"')
    pod_re = re.compile(r'pod="([^"]+)"')
    val_re = re.compile(r"=> ([0-9.]+)")

    for line in result.stdout.splitlines():
        ns_match = ns_re.search(line)
        pod_match = pod_re.search(line)
        val_match = val_re.search(line)
        if ns_match and pod_match and val_match:
            ns = ns_match.group(1)
            pod = pod_match.group(1)
            val = float(val_match.group(1))
            usage[(ns, pod)] = round(val, 3) if is_cpu else round(val)

    return usage


# ---------------------------------------------------------------------------
# Resource parsing helpers
# ---------------------------------------------------------------------------
def parse_cpu(value: Optional[str]) -> float:
    """Convert a Kubernetes CPU string to cores."""
    if not value:
        return 0.0
    if value.endswith("m"):
        return float(value[:-1]) / 1000
    if value.endswith("n"):
        return float(value[:-1]) / 1_000_000_000
    try:
        return float(value)
    except ValueError:
        return 0.0


def parse_memory_mb(value: Optional[str]) -> float:
    """Convert a Kubernetes memory string to megabytes."""
    if not value:
        return 0.0
    units: List[Tuple[str, float]] = [
        ("Ti", 1024 * 1024), ("Gi", 1024), ("Mi", 1), ("Ki", 1 / 1024),
        ("T", 1_000_000), ("G", 1000), ("M", 1), ("K", 1 / 1000),
    ]
    for suffix, factor in units:
        if value.endswith(suffix):
            try:
                return float(value[: -len(suffix)]) * factor
            except ValueError:
                return 0.0
    if value.endswith("m"):
        try:
            return float(value[:-1]) / 1000 / 1024 / 1024
        except ValueError:
            return 0.0
    try:
        return float(value) / 1024 / 1024
    except ValueError:
        return 0.0


# ---------------------------------------------------------------------------
# Data processing
# ---------------------------------------------------------------------------
def process_node_data(
    pods: List[dict],
    node: str,
    cpu_usage: Dict[Tuple[str, str], float],
    mem_usage: Dict[Tuple[str, str], float],
) -> Tuple[List[PodResource], List[PodResource]]:
    """Return the top-N CPU and memory consumers on a given worker node."""
    exclude_re = re.compile(EXCLUDE_NS_PATTERN)

    cpu_pods: List[PodResource] = []
    mem_pods: List[PodResource] = []

    for pod in pods:
        metadata = pod.get("metadata", {})
        spec = pod.get("spec", {})

        if spec.get("nodeName") != node:
            continue

        ns = metadata.get("namespace", "")
        if exclude_re.match(ns):
            continue

        pod_name = metadata.get("name", "")

        # Skip deployer pods (OpenShift DeploymentConfig deployers)
        if pod_name.endswith("-deploy"):
            continue

        containers = spec.get("containers", [])

        total_cpu_req = 0.0
        total_cpu_lim = 0.0
        total_mem_req = 0.0
        total_mem_lim = 0.0

        for container in containers:
            resources = container.get("resources", {})
            requests = resources.get("requests", {})
            limits = resources.get("limits", {})

            total_cpu_req += parse_cpu(requests.get("cpu", "0"))
            total_cpu_lim += parse_cpu(limits.get("cpu", "0"))
            total_mem_req += parse_memory_mb(requests.get("memory", "0"))
            total_mem_lim += parse_memory_mb(limits.get("memory", "0"))

        if total_cpu_req > 0:
            cpu_pods.append(PodResource(ns, pod_name, total_cpu_req, total_cpu_lim))
        if total_mem_req > 0:
            mem_pods.append(PodResource(ns, pod_name, total_mem_req, total_mem_lim))

    cpu_pods.sort(key=lambda p: p.request, reverse=True)
    mem_pods.sort(key=lambda p: p.request, reverse=True)

    return cpu_pods[:TOP_N], mem_pods[:TOP_N]


# ---------------------------------------------------------------------------
# Sheet data builder — data-only (no headers / formatting)
# ---------------------------------------------------------------------------
def _col_letter(index: int) -> str:
    """Convert a 0-based column index to a Sheets column letter (A, B, … Z, AA …)."""
    result = ""
    while True:
        result = chr(ord("A") + index % 26) + result
        index = index // 26 - 1
        if index < 0:
            break
    return result


def find_node_rows(
    worksheet: gspread.Worksheet,
    worker_nodes: List[str],
) -> Dict[str, int]:
    """Read column B from the sheet and return {node_name: 1-based_row} for each node header."""
    try:
        col_b = worksheet.col_values(2)  # column B, 1-indexed
    except (APIError, GSpreadException) as exc:
        print("Failed to read worksheet column B: {0}".format(exc), file=sys.stderr)
        sys.exit(1)

    node_set = set(worker_nodes)
    node_rows: Dict[str, int] = {}
    for idx, value in enumerate(col_b):
        stripped = value.strip()
        if stripped in node_set:
            node_rows[stripped] = idx + 1  # 1-based row number
    return node_rows


def build_range_updates(
    worker_nodes: List[str],
    node_rows: Dict[str, int],
    pods: List[dict],
    cpu_usage: Dict[Tuple[str, str], float],
    mem_usage: Dict[Tuple[str, str], float],
) -> List[dict]:
    """Build a list of {range, values} dicts for batch_update.

    Only touches the data columns:
      CPU:    B-F  (Namespace, Pod, Request, Limit, Real Usage)
      Memory: H-L  (Namespace, Pod, Request, Limit, Real Usage)

    Column G is a separator. Headers and formatting are untouched.
    """
    updates: List[dict] = []

    for node in worker_nodes:
        if node not in node_rows:
            print("Warning: node '{0}' not found in sheet, skipping.".format(node),
                  file=sys.stderr)
            continue

        header_row = node_rows[node]
        data_start = header_row + 2  # skip node header row + column header row

        top_cpu, top_mem = process_node_data(pods, node, cpu_usage, mem_usage)

        # --- CPU data (B:F) ---
        cpu_rows: List[List[str]] = []
        for i in range(TOP_N):
            if i < len(top_cpu):
                p = top_cpu[i]
                usage_val = cpu_usage.get((p.namespace, p.pod))
                real_str = "{0:.3f}".format(usage_val) if usage_val is not None else "-"
                cpu_rows.append([
                    p.namespace,
                    p.pod,
                    "{0:.3g}".format(p.request),
                    "{0:.3g}".format(p.limit),
                    real_str,
                ])
            else:
                cpu_rows.append(["", "", "", "", ""])

        cpu_range = "B{0}:F{1}".format(data_start, data_start + TOP_N - 1)
        updates.append({"range": cpu_range, "values": cpu_rows})

        # --- Memory data (K:O) ---
        mem_rows: List[List[str]] = []
        for i in range(TOP_N):
            if i < len(top_mem):
                p = top_mem[i]
                usage_val = mem_usage.get((p.namespace, p.pod))
                real_str = "{0:.0f}".format(usage_val) if usage_val is not None else "-"
                mem_rows.append([
                    p.namespace,
                    p.pod,
                    "{0:.0f}".format(p.request),
                    "{0:.0f}".format(p.limit),
                    real_str,
                ])
            else:
                mem_rows.append(["", "", "", "", ""])

        mem_range = "K{0}:O{1}".format(data_start, data_start + TOP_N - 1)
        updates.append({"range": mem_range, "values": mem_rows})

    return updates


# ---------------------------------------------------------------------------
# Google Sheets sync
# ---------------------------------------------------------------------------
def get_gspread_client() -> gspread.Client:
    """Authenticate with Google using a service account."""
    try:
        credentials = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES,
        )
        return gspread.authorize(credentials)
    except FileNotFoundError:
        print(
            "Service account file not found: {0}".format(SERVICE_ACCOUNT_FILE),
            file=sys.stderr,
        )
        sys.exit(1)
    except (GoogleAuthError, ValueError) as exc:
        print("Google authentication failed: {0}".format(exc), file=sys.stderr)
        sys.exit(1)


def get_or_create_worksheet(
    client: gspread.Client,
    worksheet_name: str,
) -> gspread.Worksheet:
    """Open an existing worksheet or create one if it does not exist."""
    try:
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
    except SpreadsheetNotFound:
        print(
            "Spreadsheet not found or not shared with service account: {0}".format(
                SPREADSHEET_ID
            ),
            file=sys.stderr,
        )
        sys.exit(1)
    except (APIError, GSpreadException) as exc:
        print("Failed to open spreadsheet: {0}".format(exc), file=sys.stderr)
        sys.exit(1)

    try:
        return spreadsheet.worksheet(worksheet_name)
    except WorksheetNotFound:
        print("Worksheet '{0}' not found, creating...".format(worksheet_name))
        try:
            return spreadsheet.add_worksheet(
                title=worksheet_name, rows=500, cols=TOTAL_COLUMNS,
            )
        except (APIError, GSpreadException) as exc:
            print("Failed to create worksheet: {0}".format(exc), file=sys.stderr)
            sys.exit(1)


def sync_data_only(
    worksheet: gspread.Worksheet,
    updates: List[dict],
) -> None:
    """Write only the data cells without touching headers, formatting, or other columns."""
    if not updates:
        print("No data to sync.")
        return
    try:
        worksheet.batch_update(updates, value_input_option="RAW")
    except (APIError, GSpreadException) as exc:
        print("Failed to sync data to sheet: {0}".format(exc), file=sys.stderr)
        sys.exit(1)


def clear_data_row_highlights(
    worksheet: gspread.Worksheet,
    node_rows: Dict[str, int],
) -> None:
    """Set white background on all data rows to clear any old highlighting.

    Uses the dynamic node positions from the sheet rather than fixed offsets.
    """
    requests: List[dict] = []

    for node, header_row in node_rows.items():
        # header_row is 1-based; Sheets API uses 0-based row indices
        data_start = header_row + 1  # skip node header + column header (0-based)
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": data_start,
                    "endRowIndex": data_start + TOP_N,
                    "startColumnIndex": 0,
                    "endColumnIndex": TOTAL_COLUMNS,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {
                            "red": 1.0,
                            "green": 1.0,
                            "blue": 1.0,
                        }
                    }
                },
                "fields": "userEnteredFormat.backgroundColor",
            }
        })

    if not requests:
        return

    try:
        worksheet.spreadsheet.batch_update({"requests": requests})
    except (APIError, GSpreadException) as exc:
        print("Warning: Could not clear highlights: {0}".format(exc), file=sys.stderr)
# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
def print_summary(worksheet_name: str, node_count: int, pod_count: int) -> None:
    """Print a short run summary."""
    print("=" * 60)
    print("Worksheet   : {0}".format(worksheet_name))
    print("Worker Nodes: {0}".format(node_count))
    print("Total Pods  : {0}".format(pod_count))
    print("Top N       : {0} per node".format(TOP_N))
    print("Sheet ID    : {0}".format(SPREADSHEET_ID))
    print("=" * 60)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    """Run the full collection and sync process."""
    parser = argparse.ArgumentParser(
        description="Collect OpenShift worker resource usage and sync to Google Sheets.",
    )
    parser.add_argument(
        "--sheet",
        required=True,
        help="Target worksheet name in Google Sheets (e.g. 'surr sby', 'surr jkt')",
    )
    args = parser.parse_args()

    worksheet_name = args.sheet

    print("Worksheet: {0}".format(worksheet_name))
    print()

    pods = collect_pods()
    worker_nodes = collect_worker_nodes()
    cpu_usage = collect_prometheus_cpu()
    mem_usage = collect_prometheus_memory()

    print("Found {0} worker nodes, {1} pods".format(len(worker_nodes), len(pods)))
    print()

    print("Syncing to Google Sheets...")
    client = get_gspread_client()
    worksheet = get_or_create_worksheet(client, worksheet_name)

    print("Scanning sheet for node positions...")
    node_rows = find_node_rows(worksheet, worker_nodes)
    matched = [n for n in worker_nodes if n in node_rows]
    missing = [n for n in worker_nodes if n not in node_rows]
    if missing:
        print("Warning: nodes not found in sheet (skipped): {0}".format(
            ", ".join(missing)), file=sys.stderr)
    if not matched:
        print("Error: none of the worker nodes were found in the sheet.", file=sys.stderr)
        sys.exit(1)
    print("Matched {0}/{1} nodes in sheet.".format(len(matched), len(worker_nodes)))

    updates = build_range_updates(
        worker_nodes, node_rows, pods, cpu_usage, mem_usage,
    )
    sync_data_only(worksheet, updates)
    clear_data_row_highlights(worksheet, node_rows)

    print_summary(worksheet_name, len(worker_nodes), len(pods))
    print("Done.")


if __name__ == "__main__":
    main()
