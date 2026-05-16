package models

import ( "sync"

		"my-cdc/internal/pb"
)

// [Pattern: Object Pool] ChangeEventBagPool tái sử dụng bộ nhớ slice để giảm tải cho Garbage Collector.
var ChangeEventBagPool sync.Pool

// QueryPool tái sử dụng slice chứa chuỗi SQL.
var QueryPool = sync.Pool{
	New: func() any {
		return make([]string, 0, 5000) // Cấp phát sẵn capacity
	},
}

// ArgsPool tái sử dụng slice chứa mảng tham số.
var ArgsPool = sync.Pool{
	New: func() any {
		return make([][]any, 0, 5000)
	},
}

// InitBagPool khởi tạo hàm New cho pool với sức chứa (capacity) lấy từ cấu hình.
func InitBagPool(capacity int) {
	ChangeEventBagPool.New = func() any {
		return make([]*pb.ChangeEvent, 0, capacity)
	}
}
