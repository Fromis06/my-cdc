package postgres

import (
	"my-cdc/internal/capture"
	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/sinks"
)

// init tự động đăng ký PostgreSQL Provider vào Capture Registry khi package này được import.
func init() {
	capture.Register("postgres", func(cfg *config.AppConfig, multiSink *sinks.MultiSink, eventsCount *models.EventsCount) capture.Listener {
		return NewListener(cfg, multiSink, eventsCount)
	})
}
