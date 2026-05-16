package postgres

import (
	"context"
	"log"
	"strings"
	"time"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/sinks"
	"my-cdc/internal/utils"
	"my-cdc/internal/pb"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Listener struct {
	Config    *config.AppConfig
	Processor *Processor
}

func NewListener(cfg *config.AppConfig, targetSink sinks.Pipeline, counts *models.EventsCount) *Listener {
	return &Listener{
		Config:    cfg,
		Processor: NewProcessor(cfg, targetSink, counts),
	}
}

func (l *Listener) Start(ctx context.Context, sourceURL string, globalState *models.GlobalState) error {
	log.Println("CAPTURE: Đang kết nối đến database nguồn...")

	var conn *pgconn.PgConn
	err := utils.DoWithRetry(
		l.Config.Retry.MaxRetries,
		time.Duration(l.Config.Retry.BaseDelayMs)*time.Millisecond,
		time.Duration(l.Config.Retry.MaxDelayTimeMs)*time.Millisecond,
		func() error {
			var connErr error
			conn, connErr = pgconn.Connect(ctx, sourceURL)
			return connErr
		},
	)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return err
	}
	log.Printf("CAPTURE: Kết nối thành công. System LSN hiện tại: %s", sysident.XLogPos)

	slotName := "cdc_test_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}

	// Lấy mốc Checkpoint đã load từ đĩa (nếu có) để yêu cầu Postgres bắt đầu từ chính xác điểm này
	startLSN := globalState.GetMinCheckpoint()
	if startLSN > 0 {
		log.Printf("CAPTURE: Yêu cầu Postgres bắt đầu gửi dữ liệu từ LSN %d", startLSN)
	}

	err = pglogrepl.StartReplication(ctx, conn, slotName, pglogrepl.LSN(startLSN), pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names 'cdc_pub'"},
	})
	if err != nil {
		return err
	}

	log.Println("CAPTURE: Bắt đầu lắng nghe các thay đổi từ PostgreSQL...")

	feedbackInterval := time.Duration(l.Config.Capture.FeedbackInterval.Load()) * time.Second
	ticker := time.NewTicker(feedbackInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done(): // Nhận tín hiệu dừng từ context chính
			return nil

		case <-ticker.C:
			// Định kỳ gửi StandbyStatusUpdate để báo cho Postgres biết LSN đã xử lý,
			// giúp Postgres dọn dẹp file WAL và tránh đầy đĩa.
			confirmedLSN := globalState.GetMinCheckpoint()

			if confirmedLSN > 0 {
				errUpdate := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: pglogrepl.LSN(confirmedLSN),
					WALFlushPosition: pglogrepl.LSN(confirmedLSN),
					WALApplyPosition: pglogrepl.LSN(confirmedLSN),
				})

				if errUpdate == nil {
					// Đồng thời, lưu checkpoint này xuống đĩa một cách định kỳ.
					// Điều này phòng trường hợp app bị tắt đột ngột (kill -9) mà không qua graceful shutdown.
					ckptData := models.CheckpointFileData{
						InstanceName: l.Config.Provider.Source.Name,
						SourceType:   pb.SourceType_SOURCE_POSTGRES,
						CheckpointData: &pb.Checkpoint{
							Offset: &pb.Checkpoint_Lsn{Lsn: confirmedLSN},
						},
					}
					errSave := utils.SaveProviderCheckpoint(l.Config.SaveDestination, ckptData)
					if errSave != nil {
						log.Printf("CAPTURE: Cảnh báo - Lỗi lưu checkpoint định kỳ xuống đĩa: %v", errSave)
					}
				} else {
					log.Printf("CAPTURE: Cảnh báo - Lỗi gửi StandbyStatusUpdate: %v", errUpdate)
				}
			}
		default:
			ctxTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
			msg, err := conn.ReceiveMessage(ctxTimeout)
			cancel()
			if err != nil {
				// Nếu là lỗi timeout, bỏ qua và tiếp tục vòng lặp.
				if pgconn.Timeout(err) {
					continue
				}
				continue
			}

			if cd, ok := msg.(*pgproto3.CopyData); ok {
				switch cd.Data[0] {
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, _ := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
					// Postgres yêu cầu phản hồi để kiểm tra kết nối còn sống không.
					if pkm.ReplyRequested {
						confirmedLSN := globalState.GetMinCheckpoint()
						if confirmedLSN == 0 {
							confirmedLSN = uint64(pkm.ServerWALEnd)
						}
						pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
							WALWritePosition: pglogrepl.LSN(confirmedLSN),
							WALFlushPosition: pglogrepl.LSN(confirmedLSN),
							WALApplyPosition: pglogrepl.LSN(confirmedLSN),
						})
					}
				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
					// Đây là gói tin chứa dữ liệu thay đổi (INSERT, UPDATE, DELETE...).
					if err != nil {
						continue
					}
					currentLSN := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
					l.Processor.ProcessRawBytes(xld.WALData, currentLSN)
				}
			}
		}
	}
}
