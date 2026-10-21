import type { ExecutionPlaneAPI } from '@syntara/contracts'

type ExecutionTarget = ExecutionPlaneAPI.components['schemas']['ExecutionTargetFacadeRead']
type Placement = ExecutionTarget['placement']

/** Kubernetes execution namespace, or null when placement has none (e.g. RHEL). */
export function getExecutionNamespace(placement: Placement | undefined): string | null {
  if (placement?.type === 'kubernetes' && placement.namespace) {
    return placement.namespace
  }
  return null
}

/**
 * Integration id for linking Cluster to Integration detail.
 * Prefers the AO sync label when present; otherwise the facade cluster_id.
 */
export function getClusterIntegrationId(target: Pick<ExecutionTarget, 'cluster_id' | 'labels'>): string | undefined {
  const fromLabel = target.labels?.integration_id?.trim()
  if (fromLabel) return fromLabel
  const clusterId = target.cluster_id?.trim()
  return clusterId || undefined
}
