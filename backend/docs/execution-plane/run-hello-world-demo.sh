#!/usr/bin/env bash
# Create the hello-world-demo workflow, start an execution, and poll until done.
# Run from anywhere. Requires curl, jq, and a running Syntara API (Step 7).
#
#   ./run-hello-world-demo.sh
#
# Environment:
#   BASE                       API origin (default https://localhost:8000)
#   ADMIN_PASSWORD             optional; otherwise reads backend/.secrets/admin-password
#   APP_ADMIN_PASSWORD_PATH    optional override for the password file

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=kind-demo-common.sh
source "${SCRIPT_DIR}/kind-demo-common.sh"

DEMO_JSON="${SCRIPT_DIR}/hello-world-demo.json"

TOKEN="$(kind_demo_login)"
AUTH=("Authorization: Bearer ${TOKEN}")

PROJ="$(curl -sk "${API}/projects" -H "${AUTH[0]}" \
  | jq -r '.resources | map(select(.is_default == true)) | .[0].id // empty')"
if [[ -z "${PROJ}" ]]; then
  PROJ="$(curl -sk "${API}/projects" -H "${AUTH[0]}" | jq -r '.resources[0].id')"
fi
if [[ -z "${PROJ}" || "${PROJ}" == "null" ]]; then
  echo "Error: no project found; run db-seed first" >&2
  exit 1
fi

BODY="$(jq --arg pid "${PROJ}" '. + {project_id: $pid}' "${DEMO_JSON}")"
WF_RESP="$(curl -sk -X POST "${API}/workflows" \
  -H "${AUTH[0]}" \
  -H "Content-Type: application/json" \
  -d "${BODY}")"
WF="$(echo "${WF_RESP}" | jq -r .id)"
if [[ -z "${WF}" || "${WF}" == "null" ]]; then
  WF="$(curl -sk "${API}/workflows" -H "${AUTH[0]}" \
    | jq -r --arg name "hello-world-demo" '.resources[] | select(.name == $name) | .id' | head -n 1)"
fi
if [[ -z "${WF}" || "${WF}" == "null" ]]; then
  echo "Error: could not create or find workflow hello-world-demo" >&2
  echo "${WF_RESP}" | jq . >&2 || echo "${WF_RESP}" >&2
  exit 1
fi
echo "workflow: ${WF}"

EXEC="$(curl -sk -X POST "${API}/executions" \
  -H "${AUTH[0]}" \
  -H "Content-Type: application/json" \
  -d "{\"workflow_id\":\"${WF}\",\"trigger_node_id\":\"trigger\",\"input_data\":{}}" \
  | jq -r .id)"
if [[ -z "${EXEC}" || "${EXEC}" == "null" ]]; then
  echo "Error: failed to create execution" >&2
  exit 1
fi
echo "execution: ${EXEC}"

kind_demo_poll_execution "${TOKEN}" "${EXEC}"
