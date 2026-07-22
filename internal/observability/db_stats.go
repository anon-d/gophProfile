package observability

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StartDBStatsCollector запускает периодический экспорт статистики пула PostgreSQL.
func StartDBStatsCollector(
	parent context.Context,
	pool *pgxpool.Pool,
	service string,
	interval time.Duration,
) context.CancelFunc {
	ctx, cancel := context.WithCancel(parent)
	if pool == nil {
		return cancel
	}
	if interval <= 0 {
		interval = 10 * time.Second
	}

	collect := func() {
		stats := pool.Stat()
		SetDBConnections(service, "total", float64(stats.TotalConns()))
		SetDBConnections(service, "idle", float64(stats.IdleConns()))
		SetDBConnections(service, "acquired", float64(stats.AcquiredConns()))
		SetDBConnections(service, "constructing", float64(stats.ConstructingConns()))
	}

	collect()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				collect()
			}
		}
	}()

	return cancel
}
