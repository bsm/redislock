package redislock

import "sync/atomic"

type Status struct {
	Success int64
	Failed  int64
	Error   int64
	Cancel  int64
}

type Stats struct {
	Obtain       Status
	Release      Status
	Refresh      Status
	Backoff      int64
	Watchdog     int64
	WatchdogDone int64
	WatchdogTick int64
}

var (
	stats Stats
)

func GetStats() Stats {
	s := Stats{
		Obtain: Status{
			Success: atomic.LoadInt64(&stats.Obtain.Success),
			Failed:  atomic.LoadInt64(&stats.Obtain.Failed),
			Error:   atomic.LoadInt64(&stats.Obtain.Error),
			Cancel:  atomic.LoadInt64(&stats.Obtain.Cancel),
		},
		Release: Status{
			Success: atomic.LoadInt64(&stats.Release.Success),
			Failed:  atomic.LoadInt64(&stats.Release.Failed),
			Error:   atomic.LoadInt64(&stats.Release.Error),
			Cancel:  atomic.LoadInt64(&stats.Release.Cancel),
		},
		Refresh: Status{
			Success: atomic.LoadInt64(&stats.Refresh.Success),
			Failed:  atomic.LoadInt64(&stats.Refresh.Failed),
			Error:   atomic.LoadInt64(&stats.Refresh.Error),
			Cancel:  atomic.LoadInt64(&stats.Refresh.Cancel),
		},
		Backoff:      atomic.LoadInt64(&stats.Backoff),
		Watchdog:     atomic.LoadInt64(&stats.Watchdog),
		WatchdogDone: atomic.LoadInt64(&stats.WatchdogDone),
		WatchdogTick: atomic.LoadInt64(&stats.WatchdogTick),
	}
	return s
}
