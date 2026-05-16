package models

import "sync/atomic"

// EventsCount chứa các biến đếm số liệu cho mục đích theo dõi (monitoring).
// Sử dụng atomic.Int64 để đảm bảo an toàn khi truy cập từ nhiều goroutine
// mà không cần dùng tới mutex, giúp tăng hiệu năng.
type EventsCount struct {
	InsertCount atomic.Int64 // Tổng số sự kiện INSERT đã được xử lý.
	UpdateCount atomic.Int64 // Tổng số sự kiện UPDATE đã được xử lý.
	DeleteCount atomic.Int64 // Tổng số sự kiện DELETE đã được xử lý.
}
