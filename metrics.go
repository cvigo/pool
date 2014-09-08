package pool

import "time"

type PoolMetrics interface {
	ReportResources(stats ResourcePoolStat)
	ReportWait(wt time.Duration)
}
