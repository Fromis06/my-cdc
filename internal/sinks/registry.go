package sinks

import (
	"context"
	"fmt"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
)

// [Pattern: Driver Factory] AppenderFactory sinh ra đích đến và gắn trực tiếp vào MultiSink.
type AppenderFactory func(ctx context.Context, consumerName string, cfg *config.AppConfig, consumerURL string, state *models.GlobalState, multiSink *MultiSink) error

var registry = make(map[string]AppenderFactory)

// Register đăng ký một loại Consumer mới (Postgres, MySQL, Kafka,...).
func Register(name string, factory AppenderFactory) {
	registry[name] = factory
}

// BuildAndAddPipeline khởi tạo pipeline dựa trên type và thêm thẳng vào MultiSink.
func BuildAndAddPipeline(ctx context.Context, sinkType string, consumerName string, cfg *config.AppConfig, consumerURL string, state *models.GlobalState, multiSink *MultiSink) error {
	if factory, exists := registry[sinkType]; exists {
		return factory(ctx, consumerName, cfg, consumerURL, state, multiSink)
	}
	return fmt.Errorf("không hỗ trợ consumer type: %s", sinkType)
}
