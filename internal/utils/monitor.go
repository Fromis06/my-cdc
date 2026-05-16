package utils

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // [Thêm] Import gói pprof để tự động đăng ký các route /debug/pprof
	"runtime"
	"time"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
)

// StartAdaptiveMonitor khởi động một goroutine để theo dõi hiệu năng hệ thống
// và tự động điều chỉnh các tham số cấu hình (auto-tuning).
func StartAdaptiveMonitor(cfg *config.AppConfig, counts *models.EventsCount, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Bật PPROF HTTP Server ngầm
	go func() {
		port := cfg.Monitor.HttpPort
		if port == 0 {
			port = 8080 // Mặc định nếu chưa cấu hình
		}
		log.Printf("MONITOR: Bật PPROF tại http://localhost:%d/debug/pprof/", port)
		// Khởi chạy HTTP Server cho pprof
		log.Println(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
	}()

	var lastInsert, lastUpdate, lastDelete int64
	log.Printf("MONITOR: Đã khởi động (Chu kỳ: %v)", interval)

	for range ticker.C {
		currentInsert := counts.InsertCount.Load()
		currentUpdate := counts.UpdateCount.Load()
		currentDelete := counts.DeleteCount.Load()

		deltaInsert := currentInsert - lastInsert
		deltaUpdate := currentUpdate - lastUpdate
		deltaDelete := currentDelete - lastDelete

		totalDelta := deltaInsert + deltaUpdate + deltaDelete
		eps := float64(totalDelta) / interval.Seconds()

		lastInsert = currentInsert
		lastUpdate = currentUpdate
		lastDelete = currentDelete

		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		allocMB := m.Alloc / 1024 / 1024
		Sys := m.Sys / 1024 / 1024

		log.Printf("MONITOR: EPS=%.0f, RAM(Go)=%dMB, RAM(Sys)=%dMB, Tổng Events=%d", eps, allocMB, Sys, currentInsert+currentUpdate+currentDelete)

		// Logic tự động điều chỉnh (Auto-Tuner):
		// Chỉ cần thay đổi giá trị trong `cfg`, các thành phần khác sẽ tự động nhận.
		if eps > 10000 {
			// Khi lưu lượng cao (bão): Tăng kích thước lô, giảm thời gian chờ để xả nhanh hơn.
			cfg.Batch.BatchMaxSize.Store(50000)
			cfg.Batch.BatchTimeout.Store(500)
			cfg.DataProcessing.DataProcessingWorkerCount.Store(16)
		} else if eps > 0 {
			// Khi lưu lượng bình thường: Dùng kích thước lô nhỏ hơn, chờ lâu hơn để gom đủ.
			cfg.Batch.BatchMaxSize.Store(5000)
			cfg.Batch.BatchTimeout.Store(200)
			cfg.DataProcessing.DataProcessingWorkerCount.Store(10)
		}
	}
}
