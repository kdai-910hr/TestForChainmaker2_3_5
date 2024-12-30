package tbft

import (
	"testing"
	"time"

	"chainmaker.org/chainmaker/logger/v2"
	tbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/tbft"
)

func Test_timeScheduler_Stop(t *testing.T) {
	type fields struct {
		logger   *logger.CMLogger
		id       string
		timer    *time.Timer
		bufferC  chan tbftpb.TimeoutInfo
		timeoutC chan tbftpb.TimeoutInfo
		stopC    chan struct{}
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{"test",
			fields{nil, "1", nil, nil, nil, make(chan struct{})}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := &timeScheduler{
				logger:   tt.fields.logger,
				id:       tt.fields.id,
				timer:    tt.fields.timer,
				bufferC:  tt.fields.bufferC,
				timeoutC: tt.fields.timeoutC,
				stopC:    tt.fields.stopC,
			}
			ts.Stop()
		})
	}
}
