/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tbft

import (
	"time"

	tbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/tbft"
	"chainmaker.org/chainmaker/protocol/v2"
)

var (
	defaultTimeSchedulerBufferSize = 10
)

// timeScheduler is used by consensus for shecdule timeout events.
// Outdated timeouts will be ignored in processing.
type timeScheduler struct {
	logger   protocol.Logger
	id       string
	timer    *time.Timer
	bufferC  chan tbftpb.TimeoutInfo
	timeoutC chan tbftpb.TimeoutInfo
	stopC    chan struct{}
}

// NewTimeSheduler returns a new timeScheduler
func newTimeSheduler(logger protocol.Logger, id string) *timeScheduler {
	ts := &timeScheduler{
		logger:   logger,
		id:       id,
		timer:    time.NewTimer(0),
		bufferC:  make(chan tbftpb.TimeoutInfo, defaultTimeSchedulerBufferSize),
		timeoutC: make(chan tbftpb.TimeoutInfo, defaultTimeSchedulerBufferSize),
		stopC:    make(chan struct{}),
	}
	if !ts.timer.Stop() {
		<-ts.timer.C
	}

	return ts
}

// Start starts the timeScheduler
func (ts *timeScheduler) Start() {
	go ts.handle()
}

// Stop stops the timeScheduler
func (ts *timeScheduler) Stop() {
	close(ts.stopC)
}

// stopTimer stop timer of timeScheduler
func (ts *timeScheduler) stopTimer() {
	ts.timer.Stop()
}

// AddTimeoutInfo add a timeoutInfo event to timeScheduler
func (ts *timeScheduler) AddTimeoutInfo(ti tbftpb.TimeoutInfo) {
	ts.logger.Debugf("len(ts.bufferC): %d", len(ts.bufferC))
	ts.bufferC <- ti
}

// GetTimeoutC returns timeoutC for consuming
func (ts *timeScheduler) GetTimeoutC() <-chan tbftpb.TimeoutInfo {
	return ts.timeoutC
}

func (ts *timeScheduler) handle() {
	ts.logger.Debugf("[%s] start handle timeout", ts.id)
	defer ts.logger.Debugf("[%s] stop handle timeout", ts.id)
	var ti tbftpb.TimeoutInfo // lastest timeout event had been seen
	for {
		select {
		case t := <-ts.bufferC:
			ts.logger.Debugf("[%s] %v receive timeoutInfo: %v, ts.bufferC len: %d", ts.id, ti, t, len(ts.bufferC))

			// ignore outdated timeouts
			if t.Height < ti.Height {
				continue
			} else if t.Height == ti.Height {
				if t.Round < ti.Round {
					continue
				} else if t.Round == ti.Round && t.Step <= ti.Step {
					continue
				}
			}

			// stop timer first
			ts.stopTimer()

			// update with new timeout
			ti = t
			ts.timer.Reset(time.Duration(ti.Duration))
			ts.logger.Debugf("[%s] schedule %v", ts.id, ti)

		case <-ts.timer.C: // timeout
			ts.logger.Debugf("[%s] %v timeout", ts.id, ti)
			ts.timeoutC <- ti
		case <-ts.stopC:
			return
		}
	}
}
