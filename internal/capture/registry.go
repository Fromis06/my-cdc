package capture

import (
	"context"
	"fmt"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/sinks"
)

// [Pattern: Driver Factory] Listener định nghĩa hợp đồng chung cho các nguồn Capture.
type Listener interface {
	Start(ctx context.Context, url string, state *models.GlobalState) error
}

// Factory là hàm khởi tạo sinh ra một Listener cụ thể.
type Factory func(cfg *config.AppConfig, multiSink *sinks.MultiSink, eventsCount *models.EventsCount) Listener

var registry = make(map[string]Factory)

// Register đăng ký một Provider mới vào hệ thống.
func Register(name string, factory Factory) {
	registry[name] = factory
}

// CreateListener khởi tạo Listener dựa vào tên Provider cấu hình.
func CreateListener(name string, cfg *config.AppConfig, multiSink *sinks.MultiSink, eventsCount *models.EventsCount) (Listener, error) {
	if factory, ok := registry[name]; ok {
		return factory(cfg, multiSink, eventsCount), nil
	}
	return nil, fmt.Errorf("không tìm thấy provider được hỗ trợ: %s", name)
}
