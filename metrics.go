package pool

import "time"

type PoolMetrtics interface {
	ReportResources(stats ResourcePoolStat)
	ReportWait(wt time.Duration)
}
