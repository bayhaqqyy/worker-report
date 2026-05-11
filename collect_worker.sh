#!/bin/bash
#
# Standalone script to collect OpenShift worker node resource usage
# and generate a CSV report.
#
# This script runs directly on a host with 'oc' access to the cluster.
# For automated sync to Google Sheets, use worker_report.py instead.
#
# Usage:
#   ./collect_worker.sh
#
# Output:
#   Worker_Resource_Request_Analysis_OpenShift.csv
#

OUT_FILE="Worker_Resource_Request_Analysis_OpenShift.csv"
EXCLUDE_NS='openshift-.*|kube-system|default'

rm -f "$OUT_FILE"

echo "Collecting pod resource spec..."
oc get pods -A -o json > /tmp/ocp_pods.json

echo "Collecting worker nodes..."
oc get nodes --no-headers | awk '$1 ~ /worker/ || $3 ~ /worker/ {print $1}' > /tmp/ocp_worker_nodes.txt

echo "Collecting real CPU usage from Prometheus..."
oc exec -n openshift-monitoring prometheus-k8s-0 -c prometheus -- \
promtool query instant http://localhost:9090 '
sum(rate(container_cpu_usage_seconds_total{
  namespace!~"openshift-.*|kube-system|default",
  container!="",
  image!="",
  pod!=""
}[5m])) by (namespace,pod)
' 2>/dev/null | awk '
{
  ns=""; pod=""; val=""
  if (match($0, /namespace="[^"]+"/)) {
    ns=substr($0, RSTART+11, RLENGTH-12)
  }
  if (match($0, /pod="[^"]+"/)) {
    pod=substr($0, RSTART+5, RLENGTH-6)
  }
  if (match($0, /=> [0-9.]+/)) {
    val=substr($0, RSTART+3, RLENGTH-3)
  }
  if (ns != "" && pod != "" && val != "") {
    printf "%s\t%s\t%.3f\n", ns, pod, val
  }
}
' > /tmp/ocp_prom_cpu_usage.tsv

echo "Collecting real memory usage from Prometheus..."
oc exec -n openshift-monitoring prometheus-k8s-0 -c prometheus -- \
promtool query instant http://localhost:9090 '
sum(container_memory_working_set_bytes{
  namespace!~"openshift-.*|kube-system|default",
  container!="",
  image!="",
  pod!=""
}) by (namespace,pod) / 1024 / 1024
' 2>/dev/null | awk '
{
  ns=""; pod=""; val=""
  if (match($0, /namespace="[^"]+"/)) {
    ns=substr($0, RSTART+11, RLENGTH-12)
  }
  if (match($0, /pod="[^"]+"/)) {
    pod=substr($0, RSTART+5, RLENGTH-6)
  }
  if (match($0, /=> [0-9.]+/)) {
    val=substr($0, RSTART+3, RLENGTH-3)
  }
  if (ns != "" && pod != "" && val != "") {
    printf "%s\t%s\t%.0f\n", ns, pod, val
  }
}
' > /tmp/ocp_prom_mem_usage.tsv

cpu_usage_core() {
  local ns="$1"
  local pod="$2"

  awk -v ns="$ns" -v pod="$pod" '
  $1 == ns && $2 == pod {
    print $3
    found=1
  }
  END {
    if (!found) print "-"
  }
  ' /tmp/ocp_prom_cpu_usage.tsv
}

mem_usage_mb() {
  local ns="$1"
  local pod="$2"

  awk -v ns="$ns" -v pod="$pod" '
  $1 == ns && $2 == pod {
    print $3
    found=1
  }
  END {
    if (!found) print "-"
  }
  ' /tmp/ocp_prom_mem_usage.tsv
}

echo "Worker Resource Request Analysis - OpenShift" > "$OUT_FILE"
echo "" >> "$OUT_FILE"

while read -r NODE; do
  [ -z "$NODE" ] && continue

  echo "Processing node: $NODE"

  CPU_FILE="/tmp/cpu_${NODE}.tsv"
  MEM_FILE="/tmp/mem_${NODE}.tsv"

  jq -r --arg node "$NODE" --arg exclude "$EXCLUDE_NS" '
    def cpu_to_core:
      if . == null or . == "" then 0
      elif test("m$") then (sub("m$";"") | tonumber) / 1000
      elif test("n$") then (sub("n$";"") | tonumber) / 1000000000
      else tonumber
      end;

    .items[]
    | select(.spec.nodeName == $node)
    | select(.metadata.namespace | test($exclude) | not)
    | select(.metadata.name | endswith("-deploy") | not)
    | {
        ns: .metadata.namespace,
        pod: .metadata.name,
        cpu_request: (
          [.spec.containers[].resources.requests.cpu // "0"]
          | map(cpu_to_core)
          | add
        ),
        cpu_limit: (
          [.spec.containers[].resources.limits.cpu // "0"]
          | map(cpu_to_core)
          | add
        )
      }
    | select(.cpu_request > 0)
    | [.ns, .pod, .cpu_request, .cpu_limit]
    | @tsv
  ' /tmp/ocp_pods.json | sort -k3,3nr | head -10 > "$CPU_FILE"

  jq -r --arg node "$NODE" --arg exclude "$EXCLUDE_NS" '
    def mem_to_mb:
      if . == null or . == "" then 0
      elif test("Ki$") then (sub("Ki$";"") | tonumber) / 1024
      elif test("Mi$") then (sub("Mi$";"") | tonumber)
      elif test("Gi$") then (sub("Gi$";"") | tonumber) * 1024
      elif test("Ti$") then (sub("Ti$";"") | tonumber) * 1024 * 1024
      elif test("K$") then (sub("K$";"") | tonumber) / 1000
      elif test("M$") then (sub("M$";"") | tonumber)
      elif test("G$") then (sub("G$";"") | tonumber) * 1000
      elif test("T$") then (sub("T$";"") | tonumber) * 1000 * 1000
      elif test("m$") then (sub("m$";"") | tonumber) / 1000 / 1024 / 1024
      else tonumber / 1024 / 1024
      end;

    .items[]
    | select(.spec.nodeName == $node)
    | select(.metadata.namespace | test($exclude) | not)
    | select(.metadata.name | endswith("-deploy") | not)
    | {
        ns: .metadata.namespace,
        pod: .metadata.name,
        mem_request: (
          [.spec.containers[].resources.requests.memory // "0"]
          | map(mem_to_mb)
          | add
        ),
        mem_limit: (
          [.spec.containers[].resources.limits.memory // "0"]
          | map(mem_to_mb)
          | add
        )
      }
    | select(.mem_request > 0)
    | [.ns, .pod, .mem_request, .mem_limit]
    | @tsv
  ' /tmp/ocp_pods.json | sort -k3,3nr | head -10 > "$MEM_FILE"

  echo ",$NODE,,,,,,$NODE,,,," >> "$OUT_FILE"
  echo ",Namespace,Pod,CPU Request ( Core ),CPU Limit ( Core ),Real CPU Usage,,Namespace,Pod,Memory Request ( MB ),Memory Limit ( MB ),Real Memory Usage" >> "$OUT_FILE"

  for i in $(seq 1 10); do
    CPU_LINE=$(sed -n "${i}p" "$CPU_FILE")
    MEM_LINE=$(sed -n "${i}p" "$MEM_FILE")

    CPU_NS=""
    CPU_POD=""
    CPU_REQ=""
    CPU_LIM=""
    CPU_USAGE=""

    MEM_NS=""
    MEM_POD=""
    MEM_REQ=""
    MEM_LIM=""
    MEM_USAGE=""

    if [ -n "$CPU_LINE" ]; then
      CPU_NS=$(echo "$CPU_LINE" | awk -F'\t' '{print $1}')
      CPU_POD=$(echo "$CPU_LINE" | awk -F'\t' '{print $2}')
      CPU_REQ=$(echo "$CPU_LINE" | awk -F'\t' '{printf "%.3g", $3}')
      CPU_LIM=$(echo "$CPU_LINE" | awk -F'\t' '{printf "%.3g", $4}')
      CPU_USAGE=$(cpu_usage_core "$CPU_NS" "$CPU_POD")
    fi

    if [ -n "$MEM_LINE" ]; then
      MEM_NS=$(echo "$MEM_LINE" | awk -F'\t' '{print $1}')
      MEM_POD=$(echo "$MEM_LINE" | awk -F'\t' '{print $2}')
      MEM_REQ=$(echo "$MEM_LINE" | awk -F'\t' '{printf "%.0f", $3}')
      MEM_LIM=$(echo "$MEM_LINE" | awk -F'\t' '{printf "%.0f", $4}')
      MEM_USAGE=$(mem_usage_mb "$MEM_NS" "$MEM_POD")
    fi

    echo ",$CPU_NS,$CPU_POD,$CPU_REQ,$CPU_LIM,$CPU_USAGE,,$MEM_NS,$MEM_POD,$MEM_REQ,$MEM_LIM,$MEM_USAGE" >> "$OUT_FILE"
  done

  echo "" >> "$OUT_FILE"

done < /tmp/ocp_worker_nodes.txt

echo "Done. CSV created: $OUT_FILE"
