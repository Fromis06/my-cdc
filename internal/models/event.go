package models

import (
	"encoding/json"
	"my-cdc/internal/pb"
)

// ParseSourceType chuyển đổi tên chuỗi thành mã định danh nguồn của protobuf.
func ParseSourceType(source string) pb.SourceType {
	switch source {
	case "postgres":
		return pb.SourceType_SOURCE_POSTGRES
	case "mysql":
		return pb.SourceType_SOURCE_MYSQL
	case "sqlserver":
		return pb.SourceType_SOURCE_SQLSERVER
	default:
		return pb.SourceType_SOURCE_UNKNOWN
	}
}

// CheckpointFileData chứa thông tin mốc checkpoint để mã hóa JSON lưu xuống đĩa.
type CheckpointFileData struct {
	InstanceName   string         `json:"instance_name"`   // Tên định danh của instance nguồn.
	SourceType     pb.SourceType  `json:"source_type"`     // Loại định dạng file lưu theo DB nguồn
	CheckpointData *pb.Checkpoint `json:"checkpoint_data"` // Dữ liệu tọa độ chi tiết.
	UpdatedAt      int64          `json:"updated_at"`      // Dấu thời gian cập nhật cuối cùng.
}

// BuildChangeEvent tạo ra đối tượng pb.ChangeEvent và marshal map dữ liệu thô sang mảng byte.
func BuildChangeEvent(sourceType pb.SourceType, action pb.Action, schema, table string, keyNames []string, before, after map[string]any, offset *pb.Checkpoint) *pb.ChangeEvent {
	var beforeBytes, afterBytes []byte
	if before != nil {
		beforeBytes, _ = json.Marshal(before)
	}
	if after != nil {
		afterBytes, _ = json.Marshal(after)
	}

	return &pb.ChangeEvent{
		SourceType: sourceType,
		Action:     action,
		Schema:     schema,
		Table:      table,
		KeyNames:   keyNames,
		Before:     beforeBytes,
		After:      afterBytes,
		Offset:     offset,
	}
}
