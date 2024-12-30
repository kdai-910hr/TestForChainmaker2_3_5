package tbft

import (
	"testing"
	"time"
)

func Test_heightMetrics_String(t *testing.T) {
	var tt, _ = time.ParseInLocation("2006-01-02 15:04:05", "2017-05-11 14:06:06", time.Local)
	type fields struct {
		height             uint64
		enterNewHeightTime time.Time
		rounds             map[int32]*roundMetrics
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{name: "test",
			fields: fields{1, tt, nil},
			want:   "{\"Height\":1,\"EnterNewHeightTime\":\"2017-05-11 14:06:06 +0800 CST\",\"Rounds\":{}}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &heightMetrics{
				height:             tt.fields.height,
				enterNewHeightTime: tt.fields.enterNewHeightTime,
				rounds:             tt.fields.rounds,
			}
			if got := h.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_heightMetrics_AppendPersistStateDuration(t *testing.T) {
	var tt, _ = time.ParseInLocation("2006-01-02 15:04:05", "2017-05-11 14:06:06", time.Local)
	persistStateDurations := make(map[string][]time.Duration)
	t1 := []time.Duration{time.Millisecond}
	persistStateDurations["1"] = t1
	rs := make(map[int32]*roundMetrics)
	rm := roundMetrics{1,
		tt,
		tt,
		tt,
		tt,
		tt,
		persistStateDurations}
	rs[1] = &rm
	type fields struct {
		height             uint64
		enterNewHeightTime time.Time
		rounds             map[int32]*roundMetrics
	}
	type args struct {
		round int32
		step  string
		d     time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"test",
			fields{1, tt, rs},
			args{2, "1", time.Millisecond}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &heightMetrics{
				height:             tt.fields.height,
				enterNewHeightTime: tt.fields.enterNewHeightTime,
				rounds:             tt.fields.rounds,
			}
			h.AppendPersistStateDuration(tt.args.round, tt.args.step, tt.args.d)
		})
	}
}
