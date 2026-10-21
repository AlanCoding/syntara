import { describe, expect, it } from 'vitest'

import { getClusterIntegrationId, getExecutionNamespace } from './executionTargetDetailUtils'

describe('getExecutionNamespace', () => {
  it('returns kubernetes namespace', () => {
    expect(
      getExecutionNamespace({
        type: 'kubernetes',
        namespace: 'syntara-workers',
      })
    ).toBe('syntara-workers')
  })

  it('returns null for RHEL placement', () => {
    expect(getExecutionNamespace({ type: 'rhel' })).toBeNull()
  })

  it('returns null when kubernetes namespace is empty', () => {
    expect(getExecutionNamespace({ type: 'kubernetes', namespace: '' })).toBeNull()
  })

  it('returns null when placement is undefined', () => {
    expect(getExecutionNamespace(undefined)).toBeNull()
  })
})

describe('getClusterIntegrationId', () => {
  it('prefers labels.integration_id over cluster_id', () => {
    expect(
      getClusterIntegrationId({
        cluster_id: 'cluster-1',
        labels: { integration_id: 'integration-1' },
      })
    ).toBe('integration-1')
  })

  it('falls back to cluster_id when integration_id label is absent', () => {
    expect(
      getClusterIntegrationId({
        cluster_id: 'cluster-1',
        labels: { region: 'us-west' },
      })
    ).toBe('cluster-1')
  })

  it('ignores blank integration_id labels', () => {
    expect(
      getClusterIntegrationId({
        cluster_id: 'cluster-1',
        labels: { integration_id: '   ' },
      })
    ).toBe('cluster-1')
  })
})
