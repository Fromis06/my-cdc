package sinks

import (
	"log"
	"my-cdc/internal/pb"
)

// [Pattern: Broadcast / Pub-Sub] MultiSink sao chép và phát sóng túi sự kiện tới nhiều đích.
type MultiSink struct {
	pipelines []Pipeline
}

func NewMultiSink() *MultiSink {
	return &MultiSink{pipelines: make([]Pipeline, 0)}
}

// AddPipeline thêm một pipeline con vào danh sách để nhận dữ liệu.
func (m *MultiSink) AddPipeline(p Pipeline) {
	m.pipelines = append(m.pipelines, p)
}

// Start khởi động tất cả các pipeline con.
func (m *MultiSink) Start() error {
	for _, p := range m.pipelines {
		if err := p.Start(); err != nil {
			log.Printf("SINK: Lỗi khi khởi động pipeline con: %v", err)
		}
	}
	return nil
}

// WriteBatch gửi một túi sự kiện đến tất cả các pipeline con đã đăng ký.
func (m *MultiSink) WriteBatch(events []*pb.ChangeEvent) error {
	for _, p := range m.pipelines {
		// Lỗi từ một pipeline không nên chặn các pipeline khác.
		_ = p.WriteBatch(events)
	}
	return nil
}

// Stop dừng tất cả các pipeline con một cách an toàn.
func (m *MultiSink) Stop() error {
	for _, p := range m.pipelines {
		// Lỗi từ một pipeline không nên chặn các pipeline khác.
		_ = p.Stop()
	}
	return nil
}
