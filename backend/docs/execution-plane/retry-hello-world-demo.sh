#!/usr/bin/env bash
# Retry an existing hello-world demo execution and poll until done.
#
#   ./retry-hello-world-demo.sh <execution-id>
#
# Environment: same as run-hello-world-demo.sh (BASE, ADMIN_PASSWORD, …).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=kind-demo-common.sh
source "${SCRIPT_DIR}/kind-demo-common.sh"

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <execution-id>" >&2
  exit 1
fi

EXEC_ID="$1"
TOKEN="$(kind_demo_login)"
EXEC="$(curl -sk -X POST "${API}/executions/${EXEC_ID}/retry" \
  -H "Authorization: Bearer ${TOKEN}" | jq -r .id)"
if [[ -z "${EXEC}" || "${EXEC}" == "null" ]]; then
  echo "Error: retry failed for execution ${EXEC_ID}" >&2
  exit 1
fi
echo "execution: ${EXEC}"

kind_demo_poll_execution "${TOKEN}" "${EXEC}"
