package utils

import (
	"log"
	"time"
)

// DoWithRetry thực hiện một hành động (operation) và tự động thử lại nếu nó thất bại.
// Hàm sử dụng chiến lược "Exponential Backoff": thời gian chờ giữa các lần thử lại
// sẽ tăng gấp đôi, cho đến khi đạt đến một giới hạn tối đa (maxDelay).
func DoWithRetry(maxRetries int, baseDelay time.Duration, maxDelay time.Duration, operation func() error) error {
	var err error
	backoff := baseDelay

	// Vòng lặp tính cả lần chạy đầu tiên (nên maxRetries + 1)
	for i := 1; i <= maxRetries+1; i++ {
		err = operation()
		if err == nil {
			return nil // Thành công
		}

		if i <= maxRetries {
			log.Printf("RETRY: Thao tác thất bại (lần %d/%d). Thử lại sau %v. Lỗi: %v", i, maxRetries, backoff, err)
			time.Sleep(backoff)

			// Tăng gấp đôi thời gian chờ cho lần thử lại tiếp theo.
			backoff *= 2
			if backoff > maxDelay {
				backoff = maxDelay // Không cho phép thời gian chờ vượt quá giới hạn.
			}
		}
	}

	log.Printf("RETRY: Bỏ cuộc sau %d lần thử. Lỗi cuối cùng: %v", maxRetries, err)
	return err
}
