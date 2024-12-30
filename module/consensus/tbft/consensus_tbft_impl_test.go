/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tbft

import (
	"crypto/rand"
	"crypto/sha256"
	"math"
	"testing"
	"time"

	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/consensus-utils/v2/testframework"
	"chainmaker.org/chainmaker/logger/v2"
	commonpb "chainmaker.org/chainmaker/pb-go/v2/common"
	"github.com/golang/mock/gomock"

	tbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/tbft"

	configpb "chainmaker.org/chainmaker/pb-go/v2/config"

	consensuspb "chainmaker.org/chainmaker/pb-go/v2/consensus"

	"chainmaker.org/chainmaker/consensus-utils/v2/consistent_service"
	"chainmaker.org/chainmaker/protocol/v2/mock"

	"github.com/stretchr/testify/require"
)

//
//  TestConsensusTBFTImpl_Type
//  @Description: TestConsensusTBFTImpl_Type
//  @param t
//
func TestConsensusTBFTImpl_Type(t *testing.T) {
	tbftImpl.RLock()
	defer tbftImpl.RUnlock()
	typeInt := tbftImpl.Type()
	require.Equal(t, typeInt, int8(TypeLocalTBFTState))
}

//
//  TestConsensusTBFTImpl_Data
//  @Description: TestConsensusTBFTImpl_Data
//  @param t
//
func TestConsensusTBFTImpl_Data(t *testing.T) {
	tbftImpl.RLock()
	defer tbftImpl.RUnlock()
	date := tbftImpl.Data()
	require.Equal(t, date, tbftImpl)
}

//
//  TestConsensusTBFTImpl_Update
//  @Description: TestConsensusTBFTImpl_Update
//  @param t
//
func TestConsensusTBFTImpl_Update(t *testing.T) {
	tbftImpl.Lock()
	defer tbftImpl.Unlock()
	status := make(map[int8]consistent_service.Status)
	s := &RemoteState{Id: "node1", Height: 1, Round: 1, Step: 1}
	status[s.Type()] = s
	tbftImpl.Update(s)

}

//
//  TestConsensusTBFTImpl_InitExtendHandler
//  @Description: TestConsensusTBFTImpl_InitExtendHandler
//  @param t
//
func TestConsensusTBFTImpl_InitExtendHandler(t *testing.T) {
	tbftImpl.Lock()
	defer tbftImpl.Unlock()
	handler := &mock.MockConsensusExtendHandler{}
	tbftImpl.InitExtendHandler(handler)
}

//
//  TestConsensusTBFTImpl_Verify
//  @Description: TestConsensusTBFTImpl_Verify
//  @param t
//
func TestConsensusTBFTImpl_Verify(t *testing.T) {
	tbftImpl.Lock()
	defer tbftImpl.Unlock()
	chainConfig := &configpb.ChainConfig{
		Consensus: &configpb.ConsensusConfig{
			Type: consensuspb.ConsensusType_TBFT,
			Nodes: []*configpb.OrgConfig{
				{
					OrgId:  org1Id,
					NodeId: []string{org1NodeId},
				},
				{
					OrgId:  org2Id,
					NodeId: []string{org2NodeId},
				},
				{
					OrgId:  org3Id,
					NodeId: []string{org3NodeId},
				},
				{
					OrgId:  org4Id,
					NodeId: []string{org4NodeId},
				},
			},
		},
	}
	err := tbftImpl.Verify(consensuspb.ConsensusType_SOLO, chainConfig)
	require.NotNil(t, err)
}

//
//  TestConsensusTBFTImpl_ExtractProposeTimeout
//  @Description: TestConsensusTBFTImpl_ExtractProposeTimeout
//  @param t
//
func TestConsensusTBFTImpl_ExtractProposeTimeout(t *testing.T) {
	tbftImpl.Lock()
	defer tbftImpl.Unlock()

	_, err := tbftImpl.extractProposeTimeout("100")
	require.NotNil(t, err)

	_, err = tbftImpl.extractProposeTimeout((time.Millisecond * 100).String())
	require.Nil(t, err)
}

//
//  TestConsensusTBFTImpl_ExtractProposeTimeoutDelta
//  @Description: TestConsensusTBFTImpl_ExtractProposeTimeoutDelta
//  @param t
//
func TestConsensusTBFTImpl_ExtractProposeTimeoutDelta(t *testing.T) {
	tbftImpl.Lock()
	defer tbftImpl.Unlock()

	_, err := tbftImpl.extractProposeTimeoutDelta("100")
	require.NotNil(t, err)

	_, err = tbftImpl.extractProposeTimeoutDelta((time.Millisecond * 100).String())
	require.Nil(t, err)
}

//
//  TestConsensusTBFTImpl_ExtractBlocksPerProposer
//  @Description: TestConsensusTBFTImpl_ExtractBlocksPerProposer
//  @param t
//
func TestConsensusTBFTImpl_ExtractBlocksPerProposer(t *testing.T) {
	tbftImpl.Lock()
	defer tbftImpl.Unlock()

	_, err := tbftImpl.extractBlocksPerProposer("100")
	require.Nil(t, err)

	_, err = tbftImpl.extractBlocksPerProposer((time.Millisecond * 100).String())
	require.NotNil(t, err)

	_, err = tbftImpl.extractBlocksPerProposer("-1")
	require.NotNil(t, err)
}

//
//  extractTimeoutProposeOptimal
//  @Description: extractTimeoutProposeOptimal
//  @param t
//
func TestConsensusTBFTImpl_ExtractextractTimeoutProposeOptimal(t *testing.T) {
	tbftImpl.Lock()
	defer tbftImpl.Unlock()

	_, err := tbftImpl.extractTimeoutProposeOptimal("100")
	require.NotNil(t, err)

	_, err = tbftImpl.extractTimeoutProposeOptimal((time.Millisecond * 100).String())
	require.Nil(t, err)
}

//
//  TestConsensusTBFTImpl_ExtractTimeoutProposeOptimal
//  @Description: TestConsensusTBFTImpl_ExtractTimeoutProposeOptimal
//  @param t
//
func TestConsensusTBFTImpl_ExtractTimeoutProposeOptimal(t *testing.T) {
	tbftImpl.Lock()
	defer tbftImpl.Unlock()

	_, err := tbftImpl.extractProposeOptimal("true")
	require.Nil(t, err)

	_, err = tbftImpl.extractProposeOptimal("1")
	require.Nil(t, err)

	_, err = tbftImpl.extractProposeOptimal("t")
	require.Nil(t, err)

	_, err = tbftImpl.extractProposeOptimal("false")
	require.Nil(t, err)

	_, err = tbftImpl.extractProposeOptimal("0")
	require.Nil(t, err)

	_, err = tbftImpl.extractProposeOptimal("f")
	require.Nil(t, err)

	_, err = tbftImpl.extractProposeOptimal("yes")
	require.NotNil(t, err)

	_, err = tbftImpl.extractProposeOptimal("-1")
	require.NotNil(t, err)

	_, err = tbftImpl.extractProposeOptimal("a")
	require.NotNil(t, err)

}

//
//  TestConsensusTBFTImpl_handleTimeout
//  @Description: TestConsensusTBFTImpl_handleTimeout
//  @param t
//
func TestConsensusTBFTImpl_getProposeTimeout(t *testing.T) {
	tbftImpl.Lock()
	defer tbftImpl.Unlock()

	tbftImpl.TimeoutPropose = 30 * time.Second
	tbftImpl.TimeoutProposeDelta = 1 * time.Second
	tbftImpl.ProposeOptimal = false
	tbftImpl.ProposeOptimalTimer = time.NewTimer(0 * time.Second)
	tbftImpl.ProposeOptimalTimer.Stop()
	id := "validator0"
	tbftImpl.Id = id
	timeout := tbftImpl.getProposeTimeout(id, 100, 100, 0)
	require.Equal(t, 30*time.Second, timeout)

	timeout = tbftImpl.getProposeTimeout(id, 97, 100, 0)
	require.Equal(t, 10*time.Second, timeout)

	timeout = tbftImpl.getProposeTimeout(id, 80, 100, 0)
	require.Equal(t, 2*time.Second, timeout)

	tbftImpl.ProposeOptimal = true
	timeout = tbftImpl.getProposeTimeout(id, 101, 100, 0)
	require.Equal(t, 30*time.Second, timeout)

	validator := "validator1"
	tbftImpl.validatorSet.validatorsBeatTime[validator] = time.Now().UnixNano() / 1e6
	timeout = tbftImpl.getProposeTimeout(validator, 100, 100, 0)
	require.Equal(t, 30*time.Second, timeout)

	timeout = tbftImpl.getProposeTimeout(validator, 99, 100, 0)
	require.Equal(t, 30*time.Second, timeout)

	timeout = tbftImpl.getProposeTimeout(validator, 98, 100, 0)
	require.Equal(t, 30*time.Second, timeout)

	timeout = tbftImpl.getProposeTimeout(validator, 97, 100, 0)
	require.Equal(t, 0*time.Second, timeout)

	tbftImpl.validatorSet.validatorsBeatTime[validator] = time.Now().UnixNano()/1e6 - TimeDisconnet*2
	timeout = tbftImpl.getProposeTimeout(validator, 100, 100, 0)
	require.Equal(t, 0*time.Second, timeout)

	timeout = tbftImpl.getProposeTimeout(validator, 0, 0, 0)
	require.Equal(t, 30*time.Second, timeout)

	timeout = tbftImpl.getProposeTimeout(validator, 0, 1, 0)
	require.Equal(t, 30*time.Second, timeout)

	timeout = tbftImpl.getProposeTimeout(validator, 1, 3, 0)
	require.Equal(t, 0*time.Second, timeout)

}

//
//  TestConsensusTBFTImpl_handleTimeout
//  @Description: TestConsensusTBFTImpl_handleTimeout
//  @param t
//
func TestConsensusTBFTImpl_handleTimeout(t *testing.T) {

	timeoutInfo := &tbftpb.TimeoutInfo{Duration: 1, Height: 1, Round: 1, Step: -1}
	tbftImpl.handleTimeout(*timeoutInfo)

	timeoutInfo = &tbftpb.TimeoutInfo{Duration: 1, Height: 1, Round: 1, Step: tbftpb.Step_PREVOTE}
	tbftImpl.handleTimeout(*timeoutInfo)

	timeoutInfo = &tbftpb.TimeoutInfo{Duration: 1, Height: 1, Round: 1, Step: tbftpb.Step_PRECOMMIT}
	tbftImpl.handleTimeout(*timeoutInfo)

	timeoutInfo = &tbftpb.TimeoutInfo{Duration: 1, Height: 1, Round: 1, Step: tbftpb.Step_COMMIT}
	tbftImpl.handleTimeout(*timeoutInfo)
}

//
//  TestConsensusTBFTImpl_handleProposeOptimalTimeout
//  @Description: TestConsensusTBFTImpl_handleProposeOptimalTimeout
//  @param t
//
func TestConsensusTBFTImpl_handleProposeOptimalTimeout(t *testing.T) {

	tbftImpl.ProposeOptimalTimer = time.NewTimer(0 * time.Second)
	tbftImpl.ProposeOptimalTimer.Stop()
	tbftImpl.validatorSet.blocksPerProposer = 1

	tbftImpl.Step = tbftpb.Step_PREVOTE
	tbftImpl.handleProposeOptimalTimeout()

	tbftImpl.Step = tbftpb.Step_PROPOSE
	tbftImpl.validatorSet.validatorsBeatTime["validator0"] = time.Now().UnixNano() / 1e6
	tbftImpl.validatorSet.validatorsBeatTime["validator1"] = time.Now().UnixNano() / 1e6
	tbftImpl.handleProposeOptimalTimeout()
}

//
//  TestConsensusTBFTImpl_PrevoteTimeout
//  @Description: TestConsensusTBFTImpl_PrevoteTimeout
//  @param t
//
func TestConsensusTBFTImpl_PrevoteTimeout(t *testing.T) {
	t1 := time.Duration(
		TimeoutPrevote.Nanoseconds()+TimeoutPrevoteDelta.Nanoseconds()*int64(0),
	) * time.Nanosecond
	t2 := tbftImpl.PrevoteTimeout(0)
	require.Equal(t, t1, t2)
}

//
//  TestConsensusTBFTImpl_PrecommitTimeout
//  @Description: TestConsensusTBFTImpl_PrecommitTimeout
//  @param t
//
func TestConsensusTBFTImpl_PrecommitTimeout(t *testing.T) {
	t1 := time.Duration(
		TimeoutPrecommit.Nanoseconds()+TimeoutPrecommitDelta.Nanoseconds()*int64(0),
	) * time.Nanosecond
	t2 := tbftImpl.PrecommitTimeout(0)
	require.Equal(t, t1, t2)
}

//
//  TestConsensusTBFTImpl_CommitTimeout
//  @Description: TestConsensusTBFTImpl_CommitTimeout
//  @param t
//
func TestConsensusTBFTImpl_CommitTimeout(t *testing.T) {
	t1 := time.Duration(TimeoutCommit.Nanoseconds()*int64(0)) * time.Nanosecond
	t2 := tbftImpl.CommitTimeout(0)
	require.Equal(t, t1, t2)
}

//
// TestConsensusTBFTImpl_ToProto
// @Description: TestConsensusTBFTImpl_ToProto
// @param t
//
func TestConsensusTBFTImpl_ToProto(t *testing.T) {
	tbftImpl.RLock()
	defer tbftImpl.RUnlock()
	consensusState := tbftImpl.ToProto()
	require.NotNil(t, consensusState)
}

//
// TestConsensusTBFTImpl_ToGossipStateProto
// @Description: TestConsensusTBFTImpl_ToGossipStateProto
// @param t
//
func TestConsensusTBFTImpl_ToGossipStateProto(t *testing.T) {
	tbftImpl.RLock()
	defer tbftImpl.RUnlock()
	gssipState := tbftImpl.ToGossipStateProto()
	require.NotNil(t, gssipState)
}

//
// TestConsensusTBFTImpl_EnterPrecommitFromReplayWal
// @Description: TestConsensusTBFTImpl_EnterPrecommitFromReplayWal
// @param t
//
func TestConsensusTBFTImpl_EnterPrecommitFromReplayWal(t *testing.T) {
	tbftImpl.RLock()
	defer tbftImpl.RUnlock()
	err := tbftImpl.enterPrecommitFromReplayWal(nil)
	require.NotNil(t, err)
}

//
// TestConsensusTBFTImpl_GetValidators
// @Description: TestConsensusTBFTImpl_GetValidators
// @param t
//
func TestConsensusTBFTImpl_GetValidators(t *testing.T) {
	tbftImpl.RLock()
	defer tbftImpl.RUnlock()
	tbftImpl.state = stop
	validators, err := tbftImpl.GetValidators()
	require.NotNil(t, err)
	require.Nil(t, validators)
	tbftImpl.state = start
	validators, err = tbftImpl.GetValidators()
	require.Nil(t, err)
	require.NotNil(t, validators)
}

//
// TestConsensusTBFTImpl_GetLastHeight
// @Description: TestConsensusTBFTImpl_GetLastHeight
// @param t
//
func TestConsensusTBFTImpl_GetLastHeight(t *testing.T) {
	tbftImpl.RLock()
	defer tbftImpl.RUnlock()
	tbftImpl.state = stop
	height := tbftImpl.GetLastHeight()
	require.True(t, height == math.MaxUint64)
	tbftImpl.state = start
	height = tbftImpl.GetLastHeight()
	require.False(t, height == math.MaxUint64)
}

//
// TestConsensusTBFTImpl_GetConsensusStateJSON
// @Description: TestConsensusTBFTImpl_GetConsensusStateJSON
// @param t
//
func TestConsensusTBFTImpl_GetConsensusStateJSON(t *testing.T) {
	tbftImpl.RLock()
	defer tbftImpl.RUnlock()
	tbftImpl.state = stop
	stat, err := tbftImpl.GetConsensusStateJSON()
	require.Nil(t, stat)
	require.NotNil(t, err)
	tbftImpl.state = start
	stat, err = tbftImpl.GetConsensusStateJSON()
	require.NotNil(t, stat)
	require.Nil(t, err)
}

func TestConsensusTBFTImpl_getValidatorSet(t *testing.T) {
	vs := tbftImpl.getValidatorSet()
	require.NotNil(t, vs)
}

func TestConsensusTBFTImpl_onQuit(t *testing.T) {
	tbftImpl.OnQuit()
}

//
//  TestProcVerifyBlockWithRwSets
//  @Description: TestProcVerifyBlockWithRwSets
//  @param t
//
func TestProcVerifyBlockWithRwSets(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tbftImpl.RLock()
	defer tbftImpl.RUnlock()
	var blockHeight uint64 = 10
	blockHash := sha256.Sum256(nil)
	_, _ = rand.Read(blockHash[:])
	block := &commonpb.Block{
		Header: &commonpb.BlockHeader{
			BlockHeight: blockHeight,
			BlockHash:   blockHash[:],
		},
		AdditionalData: &commonpb.AdditionalData{
			ExtraData: map[string][]byte{
				TBFTAddtionalDataKey: nil,
			},
		},
	}

	var votes []*tbftpb.Vote
	for _, id := range tbftImpl.validatorSet.Validators {
		vote := NewVote(tbftpb.VoteType_VOTE_PREVOTE, id, blockHeight, 0, blockHash[:])
		votes = append(votes, vote)
	}

	proposal := &tbftpb.Proposal{
		Voter:    "node1",
		Height:   1,
		Round:    1,
		PolRound: 1,
		Block:    block,
		Qc:       votes,
	}

	tbftProposal := NewTBFTProposal(proposal, true)
	oldLen := len(tbftProposal.Bytes)
	tbftProposal.Marshal()
	require.Equal(t, oldLen, len(tbftProposal.Bytes))
	core := testframework.NewCoreEngineForTest(&testframework.TestNodeConfig{}, cmLogger)
	ac := mock.NewMockAccessControlProvider(ctrl)
	ac.EXPECT().CreatePrincipal(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
	ac.EXPECT().VerifyMsgPrincipal(gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)
	tbftImpl.ac = ac
	tbftImpl.blockVerifier = core.GetBlockVerifier()
	err := tbftImpl.procVerifyBlockWithRwSets(proposal)
	require.Nil(t, err)
}

//
//  TestProcRoundQc
//  @Description: TestProcRoundQc
//  @param t
//
func TestProcRoundQc(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tbftImpl.RLock()
	defer tbftImpl.RUnlock()

	tbftImpl.heightRoundVoteSet = newHeightRoundVoteSet(
		cmLogger, 1, 1, tbftImpl.validatorSet)

	blockHash := sha256.Sum256(nil)
	_, _ = rand.Read(blockHash[:])
	for _, id := range tbftImpl.validatorSet.Validators {
		vote := NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, id, 1, 1, blockHash[:])
		_, _ = tbftImpl.heightRoundVoteSet.addVote(vote)
	}

	voteSet := NewVoteSet(cmLogger, tbftpb.VoteType_VOTE_PRECOMMIT, 1, 2, tbftImpl.validatorSet)

	for _, id := range tbftImpl.validatorSet.Validators {
		vote := NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, id, 1, 2, blockHash[:])
		_, _ = voteSet.AddVote(vote, false)
	}

	roundqc := &tbftpb.RoundQC{
		Height: 1,
		Round:  2,
		Qc:     voteSet.ToProto(),
	}

	ac := mock.NewMockAccessControlProvider(ctrl)
	ac.EXPECT().CreatePrincipal(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
	ac.EXPECT().VerifyMsgPrincipal(gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)
	tbftImpl.ac = ac
	tbftImpl.procRoundQC(roundqc)
}

//
//  TestProcRoundQcByPC
//  @Description: TestProcRoundQcByPC
//  @param t
//
func TestProcRoundQcByPC(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tbftImpl.RLock()
	defer tbftImpl.RUnlock()

	tbftImpl.metrics = newHeightMetrics(1)
	tbftImpl.heightRoundVoteSet = newHeightRoundVoteSet(
		cmLogger, 1, 1, tbftImpl.validatorSet)

	blockHash := sha256.Sum256(nil)
	_, _ = rand.Read(blockHash[:])

	voteSet := NewVoteSet(cmLogger, tbftpb.VoteType_VOTE_PREVOTE, 1, 2, tbftImpl.validatorSet)

	for _, id := range tbftImpl.validatorSet.Validators {
		vote := NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, id, 1, 2, blockHash[:])
		_, _ = voteSet.AddVote(vote, false)
	}

	roundqc := &tbftpb.RoundQC{
		Height: 1,
		Round:  2,
		Qc:     voteSet.ToProto(),
	}

	ac := mock.NewMockAccessControlProvider(ctrl)
	ac.EXPECT().CreatePrincipal(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
	ac.EXPECT().VerifyMsgPrincipal(gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)
	tbftImpl.ac = ac
	tbftImpl.procRoundQC(roundqc)
}

//
//  TestDelInvalidTxs
//  @Description: TestDelInvalidTxs
//  @param t
//
func TestDelInvalidTxs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tbftImpl.RLock()
	defer tbftImpl.RUnlock()

	//blockHash := sha256.Sum256(nil)
	//_, _ = rand.Read(blockHash[:])

	voteSet := NewVoteSet(cmLogger, tbftpb.VoteType_VOTE_PREVOTE, 1, 2, tbftImpl.validatorSet)

	for _, id := range tbftImpl.validatorSet.Validators {
		vote := NewVote(tbftpb.VoteType_VOTE_PREVOTE, id, 1, 2, nilHash)
		vote.InvalidTxs = []string{"hash1", "hash2"}
		_, _ = voteSet.AddVote(vote, true)
	}

	tbftImpl.delInvalidTxs(voteSet, nilHash)
}

func TestTBFTConsensus_OnQuit(t *testing.T) {
	type fields struct {
		log      *logger.CMLogger
		msgBus   msgbus.MessageBus
		messageC chan interface{}
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{"test1",
			fields{logger.GetLogger(chainid), tbftImpl.msgbus, make(chan interface{})},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &ConsensusTBFTImpl{
				logger: tt.fields.log,
				msgbus: tt.fields.msgBus,
			}
			m.OnQuit()
		})
	}
}

//
//  TestProcVote
//  @Description: TestProcVote
//  @param t
//
func TestProcVote(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tbftImpl.RLock()
	defer tbftImpl.RUnlock()
	valSet := &validatorSet{
		logger:             tLogger,
		Validators:         nodes,
		validatorsHeight:   make(map[string]uint64),
		validatorsBeatTime: make(map[string]int64),
	}

	tbftImpl.validatorSet = valSet

	tbftImpl.metrics = newHeightMetrics(1)
	tbftImpl.heightRoundVoteSet = newHeightRoundVoteSet(
		cmLogger, 10, 1, tbftImpl.validatorSet)
	tbftImpl.Id = tbftImpl.validatorSet.Validators[3]
	tbftImpl.Height = 10
	tbftImpl.Round = 1

	// procPrevote
	blockHash := sha256.Sum256(nil)
	_, _ = rand.Read(blockHash[:])
	vote := NewVote(tbftpb.VoteType_VOTE_PREVOTE, tbftImpl.validatorSet.Validators[0], 10, 1, blockHash[:])
	_, _ = tbftImpl.heightRoundVoteSet.addVote(vote)
	vote = NewVote(tbftpb.VoteType_VOTE_PREVOTE, tbftImpl.validatorSet.Validators[1], 10, 1, blockHash[:])
	_, _ = tbftImpl.heightRoundVoteSet.addVote(vote)
	vote = NewVote(tbftpb.VoteType_VOTE_PREVOTE, tbftImpl.validatorSet.Validators[2], 10, 1, blockHash[:])
	_, _ = tbftImpl.heightRoundVoteSet.addVote(vote)
	tbftImpl.Step = tbftpb.Step_PRECOMMIT

	require.NotNil(t, tbftImpl.heightRoundVoteSet.getRoundVoteSet(1).Prevotes.Maj23)
	vote = NewVote(tbftpb.VoteType_VOTE_PREVOTE, tbftImpl.Id, 10, 1, blockHash[:])
	tbftImpl.procPrevote(vote)

	// procPrecomimit
	vote = NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, tbftImpl.validatorSet.Validators[0], 10, 1, blockHash[:])
	_, _ = tbftImpl.heightRoundVoteSet.addVote(vote)
	vote = NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, tbftImpl.validatorSet.Validators[1], 10, 1, blockHash[:])
	_, _ = tbftImpl.heightRoundVoteSet.addVote(vote)
	vote = NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, tbftImpl.validatorSet.Validators[2], 10, 1, blockHash[:])
	_, _ = tbftImpl.heightRoundVoteSet.addVote(vote)
	tbftImpl.Step = tbftpb.Step_COMMIT

	require.NotNil(t, tbftImpl.heightRoundVoteSet.getRoundVoteSet(1).Precommits.Maj23)
	vote = NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, tbftImpl.Id, 10, 1, blockHash[:])
	tbftImpl.procPrecommit(vote)
}

//
//  TestProcLocalVote
//  @Description: TestProcLocalVote
//  @param t
//
func TestProcLocalVote(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tbftImpl.RLock()
	defer tbftImpl.RUnlock()
	valSet := &validatorSet{
		logger:             tLogger,
		Validators:         nodes,
		validatorsHeight:   make(map[string]uint64),
		validatorsBeatTime: make(map[string]int64),
	}

	tbftImpl.validatorSet = valSet

	tbftImpl.metrics = newHeightMetrics(1)
	tbftImpl.heightRoundVoteSet = newHeightRoundVoteSet(
		cmLogger, 10, 1, tbftImpl.validatorSet)
	tbftImpl.Id = tbftImpl.validatorSet.Validators[3]
	tbftImpl.Height = 10
	tbftImpl.Round = 1

	blockHash := sha256.Sum256(nil)
	_, _ = rand.Read(blockHash[:])
	vote := NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, tbftImpl.validatorSet.Validators[0], 10, 1, blockHash[:])
	_, _ = tbftImpl.heightRoundVoteSet.addVote(vote)
	vote = NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, tbftImpl.validatorSet.Validators[1], 10, 1, blockHash[:])
	_, _ = tbftImpl.heightRoundVoteSet.addVote(vote)
	vote = NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, tbftImpl.validatorSet.Validators[2], 10, 1, blockHash[:])
	_, _ = tbftImpl.heightRoundVoteSet.addVote(vote)
	tbftImpl.Step = tbftpb.Step_COMMIT

	vote = NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, tbftImpl.validatorSet.Validators[0], 10, 1, blockHash[:])
	tbftImpl.procLocalVote(vote)

	vote = NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, tbftImpl.Id, 10, 1, blockHash[:])
	tbftImpl.procLocalVote(vote)
	vote = NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, tbftImpl.Id, 10, 1, blockHash[1:])
	tbftImpl.procLocalVote(vote)
}
