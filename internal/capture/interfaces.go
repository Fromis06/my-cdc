package capture

import (
	"context"

	"my-cdc/internal/models"
	"my-cdc/internal/sinks"
)

// SourceCapture là giao diện chung cho mọi loại Database Nguồn
type SourceCapture interface {
	Start(ctx context.Context, sourceURL string, targetSink sinks.Pipeline, eventsCount *models.EventsCount) error
	Stop() error
}