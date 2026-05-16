package models

import (
	"sync"
	"sync/atomic"
)

// [Pattern: Thread-Safe State] GlobalState quản lý mốc checkpoint độc lập cho từng Sink một cách an toàn.
type GlobalState struct {
	checkpoints sync.Map // Key: Tên Sink (string), Value: LSN (*atomic.Uint64).
}

// NewGlobalState khởi tạo một GlobalState mới.
func NewGlobalState() *GlobalState {
	return &GlobalState{}
}

// InitSink khởi tạo mốc checkpoint ban đầu cho một Sink cụ thể.
func (g *GlobalState) InitSink(sinkName string, initialVal uint64) {
	val := &atomic.Uint64{}
	val.Store(initialVal)
	g.checkpoints.Store(sinkName, val)
}

// RemoveSink xóa mốc checkpoint của một Sink khỏi hệ thống.
// Dùng khi một Sink bị lỗi và cần ngắt bỏ mà không làm ảnh hưởng đến tiến trình chung.
func (g *GlobalState) RemoveSink(sinkName string) {
	g.checkpoints.Delete(sinkName)
}

// UpdateCheckpoint cập nhật mốc checkpoint cho một Sink cụ thể.
func (g *GlobalState) UpdateCheckpoint(sinkName string, val uint64) {
	actual, ok := g.checkpoints.Load(sinkName)
	if !ok {
		return
	}
	atomicVal := actual.(*atomic.Uint64)
	for {
		current := atomicVal.Load()
		if val <= current {
			return
		}
		if atomicVal.CompareAndSwap(current, val) {
			return
		}
	}
}

// GetMinCheckpoint trả về mốc checkpoint nhỏ nhất trong tất cả các Sinks.
func (g *GlobalState) GetMinCheckpoint() uint64 {
	var min uint64 = 0
	first := true

	g.checkpoints.Range(func(key, value any) bool {
		val := value.(*atomic.Uint64).Load()
		if first {
			min = val
			first = false
		} else if val < min {
			min = val
		}
		return true
	})

	return min
}
