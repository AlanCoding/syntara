import { useParams } from '@tanstack/react-router'

import { breadcrumbsExecutionTargetDetailEarlyShell } from '../../app/breadcrumbBuilders'
import { SynPage, SynPageBody } from '../../components/layout/SynPage'
import { SynPageHeader } from '../../components/layout/SynPageHeader'
import { SynPanel } from '../../components/layout/SynPanel'
import { SynPageTitle } from '../../components/SynPageTitle'
import { useDocLink } from '../../utils/docs/useDocLink'

export default function ExecutionTargetDetail() {
  const { executionTargetId }: { executionTargetId?: string } = useParams({ strict: false })
  const docLink = useDocLink('executionPlane')

  return (
    <SynPage>
      <SynPageTitle segments={['Execution target', 'Execution Plane']} />
      <SynPageHeader
        title="Execution target"
        breadcrumbs={breadcrumbsExecutionTargetDetailEarlyShell()}
        docLink={docLink}
      />
      <SynPageBody>
        <SynPanel isFullHeight>
          {executionTargetId ? `Execution target ${executionTargetId}` : 'No execution target ID provided'}
        </SynPanel>
      </SynPageBody>
    </SynPage>
  )
}
