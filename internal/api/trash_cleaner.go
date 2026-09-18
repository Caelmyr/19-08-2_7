// Package api 回收站自动清理：定时物理删除超过保留期限的回收站文档。
package api

import (
	"context"
	"log"
	"time"
)

// defaultPurgeInterval 自动清理的扫描间隔
const defaultPurgeInterval = 10 * time.Minute

// StartTrashCleaner 启动回收站自动清理协程。
// 启动时立即扫描一次，之后每隔 purgeInterval 清理一次 expires_at 到期的文档。
func (s *Server) StartTrashCleaner(ctx context.Context) {
	interval := defaultPurgeInterval
	if s.TrashTTL > 0 && s.TrashTTL < interval {
		// 保留期很短（测试场景）时，扫描间隔不应长于保留期
		interval = s.TrashTTL / 2
		if interval <= 0 {
			interval = time.Second
		}
	}

	runPurge := func() {
		n, err := s.Store.PurgeExpired()
		if err != nil {
			log.Printf("[Trash] purge expired documents failed: %v", err)
			return
		}
		if n > 0 {
			log.Printf("[Trash] auto-purged %d expired document(s)", n)
		}
	}

	log.Printf("[Trash] auto-cleaner started, retention=%s, scan interval=%s", s.TrashTTL, interval)

	runPurge()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[Trash] auto-cleaner stopped")
			return
		case <-ticker.C:
			runPurge()
		}
	}
}
