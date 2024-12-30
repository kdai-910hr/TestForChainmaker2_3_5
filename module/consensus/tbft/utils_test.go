/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tbft

import (
	"crypto/rand"
	"crypto/sha256"
	"reflect"
	"testing"

	"chainmaker.org/chainmaker/logger/v2"
	commonpb "chainmaker.org/chainmaker/pb-go/v2/common"
	configpb "chainmaker.org/chainmaker/pb-go/v2/config"
	consensuspb "chainmaker.org/chainmaker/pb-go/v2/consensus"
	tbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/tbft"
	"chainmaker.org/chainmaker/protocol/v2/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	chainId    = "test"
	org1Id     = "wx-org1"
	org2Id     = "wx-org2"
	org3Id     = "wx-org3"
	org4Id     = "wx-org4"
	org1NodeId = "QmQZn3pZCcuEf34FSvucqkvVJEvfzpNjQTk17HS6CYMR35"
	org2NodeId = "QmeRZz3AjhzydkzpiuuSAtmqt8mU8XcRH2hynQN4tLgYg6"
	org3NodeId = "QmTSMcqwp4X6oPP5WrNpsMpotQMSGcxVshkGLJUhCrqGbu"
	org4NodeId = "QmUryDgjNoxfMXHdDRFZ5Pe55R1vxTPA3ZgCteHze2ET27"
)

var cmLogger *logger.CMLogger

//
//  init
//  @Description: init logger
//
func init() {
	// logger, _ := zap.NewDevelopment(zap.AddCaller())
	// clog = logger.Sugar()
	cmLogger = logger.GetLogger(chainId)
}

//
//  TestGetValidatorListFromConfig
//  @Description: TestGetValidatorListFromConfig
//  @param t
//
func TestGetValidatorListFromConfig(t *testing.T) {
	type args struct {
		chainConfig *configpb.ChainConfig
	}
	tests := []struct {
		name           string
		args           args
		wantValidators []string
		wantErr        bool
	}{
		{
			"one org with one node id",
			args{
				chainConfig: &configpb.ChainConfig{
					Consensus: &configpb.ConsensusConfig{
						Nodes: []*configpb.OrgConfig{
							{
								OrgId:  org1Id,
								NodeId: []string{org1NodeId},
							},
						},
					},
				},
			},
			[]string{org1NodeId},
			false,
		},
		{
			"two org, each with one node id",
			args{
				chainConfig: &configpb.ChainConfig{
					Consensus: &configpb.ConsensusConfig{
						Nodes: []*configpb.OrgConfig{
							{
								OrgId:  org1Id,
								NodeId: []string{org1NodeId},
							},
							{
								OrgId:  org2Id,
								NodeId: []string{org2NodeId},
							},
						},
					},
				},
			},
			[]string{org1NodeId, org2NodeId},
			false,
		},
		{
			"two org, each with two node ids",
			args{
				chainConfig: &configpb.ChainConfig{
					Consensus: &configpb.ConsensusConfig{
						Nodes: []*configpb.OrgConfig{
							{
								OrgId: org1Id,
								NodeId: []string{
									org1NodeId,
									org3NodeId,
								},
							},
							{
								OrgId: org2Id,
								NodeId: []string{
									org2NodeId,
									org4NodeId,
								},
							},
						},
					},
				},
			},
			[]string{
				org1NodeId,
				"QmTSMcqwp4X6oPP5WrNpsMpotQMSGcxVshkGLJUhCrqGbu",
				org2NodeId,
				"QmUryDgjNoxfMXHdDRFZ5Pe55R1vxTPA3ZgCteHze2ET27",
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotValidators, err := GetValidatorListFromConfig(tt.args.chainConfig)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetValidatorListFromConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotValidators, tt.wantValidators) {
				t.Errorf("GetValidatorListFromConfig() = %v, want %v", gotValidators, tt.wantValidators)
			}
		})
	}
}

//
//  TestVerifyBlockSignaturesOneNodeSuccess
//  @Description: TestVerifyBlockSignaturesOneNodeSuccess
//  @param t
//
func TestVerifyBlockSignaturesOneNodeSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := mock.NewMockChainConf(ctrl)
	chainConf.EXPECT().GetChainConfigFromFuture(gomock.Any()).AnyTimes().Return(&configpb.ChainConfig{
		Consensus: &configpb.ConsensusConfig{
			Type: consensuspb.ConsensusType_TBFT,
			Nodes: []*configpb.OrgConfig{
				{
					OrgId:  org1Id,
					NodeId: []string{org1NodeId},
				},
			},
		},
	}, nil)

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

	chainConfig, _ := chainConf.GetChainConfigFromFuture(blockHeight)
	validators, _ := GetValidatorListFromConfig(chainConfig)
	validatorSet := newValidatorSet(cmLogger, validators, 1)
	voteSet := NewVoteSet(cmLogger, tbftpb.VoteType_VOTE_PRECOMMIT, blockHeight, 0, validatorSet)
	vote := NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, org1NodeId, blockHeight, 0, blockHash[:])
	added, err := voteSet.AddVote(vote, false)
	require.Nil(t, err)
	require.True(t, added)
	qc := mustMarshal(voteSet.ToProto())
	block.AdditionalData.ExtraData[TBFTAddtionalDataKey] = qc

	ac := mock.NewMockAccessControlProvider(ctrl)
	ac.EXPECT().CreatePrincipal(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
	ac.EXPECT().VerifyMsgPrincipal(gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)

	if err := VerifyBlockSignatures(chainConf, ac, block, nil, GetValidatorList); err != nil {
		t.Errorf("VerifyBlockSignatures() error = %v, wantErr %v", err, nil)
	}
}

//
//  TestVerifyBlockSignaturesOneNodeFail
//  @Description: TestVerifyBlockSignaturesOneNodeFail
//  @param t
//
func TestVerifyBlockSignaturesOneNodeFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := mock.NewMockChainConf(ctrl)
	chainConf.EXPECT().GetChainConfigFromFuture(gomock.Any()).AnyTimes().Return(&configpb.ChainConfig{
		Consensus: &configpb.ConsensusConfig{
			Type: consensuspb.ConsensusType_TBFT,
			Nodes: []*configpb.OrgConfig{
				{
					OrgId:  org1Id,
					NodeId: []string{org1NodeId},
				},
			},
		},
	}, nil)

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

	// chainConfig, _ := chainConf.GetChainConfigFromFuture(blockHeight)
	// validators, _ := GetValidatorListFromConfig(chainConfig)
	// validatorSet := newValidatorSet(validators)
	// voteSet := NewVoteSet(tbftpb.VoteType_VOTE_PRECOMMIT, blockHeight, 0, validatorSet)
	// vote := NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, org1Id, blockHeight, 0, blockHash[:])
	// voteSet.AddVote(vote)
	// qc := mustMarshal(voteSet.ToProto())
	// block.AdditionalData.ExtraData[TBFTAddtionalDataKey] = qc

	ac := mock.NewMockAccessControlProvider(ctrl)
	ac.EXPECT().CreatePrincipal(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
	ac.EXPECT().VerifyMsgPrincipal(gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)

	if err := VerifyBlockSignatures(chainConf, ac, block, nil, GetValidatorList); err == nil {
		t.Errorf("VerifyBlockSignatures() error = %v, but expecte error", err)
	}
}

//
//  TestVerifyBlockSignaturesFourNodeSuccess
//  @Description: TestVerifyBlockSignaturesFourNodeSuccess
//  @param t
//
func TestVerifyBlockSignaturesFourNodeSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := mock.NewMockChainConf(ctrl)
	chainConf.EXPECT().GetChainConfigFromFuture(gomock.Any()).AnyTimes().Return(&configpb.ChainConfig{
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
	}, nil)

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

	chainConfig, _ := chainConf.GetChainConfigFromFuture(blockHeight)
	validators, _ := GetValidatorListFromConfig(chainConfig)
	validatorSet := newValidatorSet(cmLogger, validators, 1)
	voteSet := NewVoteSet(cmLogger, tbftpb.VoteType_VOTE_PRECOMMIT, blockHeight, 0, validatorSet)

	nodes := []string{
		org1NodeId,
		org2NodeId,
		"QmTSMcqwp4X6oPP5WrNpsMpotQMSGcxVshkGLJUhCrqGbu",
		"QmUryDgjNoxfMXHdDRFZ5Pe55R1vxTPA3ZgCteHze2ET27",
	}
	for _, id := range nodes {
		vote := NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, id, blockHeight, 0, blockHash[:])
		_, _ = voteSet.AddVote(vote, false)
	}
	qc := mustMarshal(voteSet.ToProto())
	block.AdditionalData.ExtraData[TBFTAddtionalDataKey] = qc

	ac := mock.NewMockAccessControlProvider(ctrl)
	ac.EXPECT().CreatePrincipal(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
	ac.EXPECT().VerifyMsgPrincipal(gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)
	if err := VerifyBlockSignatures(chainConf, ac, block, nil, GetValidatorList); err != nil {
		t.Errorf("VerifyBlockSignatures() error = %v, wantErr %v", err, nil)
	}
}

//
//  TestVerifyBlockSignaturesFourNodeFail
//  @Description: TestVerifyBlockSignaturesFourNodeFail
//  @param t
//
func TestVerifyBlockSignaturesFourNodeFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := mock.NewMockChainConf(ctrl)
	chainConf.EXPECT().GetChainConfigFromFuture(gomock.Any()).AnyTimes().Return(&configpb.ChainConfig{
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
	}, nil)

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

	chainConfig, _ := chainConf.GetChainConfigFromFuture(blockHeight)
	validators, _ := GetValidatorListFromConfig(chainConfig)
	validatorSet := newValidatorSet(cmLogger, validators, 1)
	voteSet := NewVoteSet(cmLogger, tbftpb.VoteType_VOTE_PRECOMMIT, blockHeight, 0, validatorSet)

	nodes := []string{
		org1NodeId,
		// org2Id,
		// "QmTSMcqwp4X6oPP5WrNpsMpotQMSGcxVshkGLJUhCrqGbu",
		// "QmUryDgjNoxfMXHdDRFZ5Pe55R1vxTPA3ZgCteHze2ET27",
	}
	for _, id := range nodes {
		vote := NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, id, blockHeight, 0, blockHash[:])
		_, _ = voteSet.AddVote(vote, false)
	}
	qc := mustMarshal(voteSet.ToProto())
	block.AdditionalData.ExtraData[TBFTAddtionalDataKey] = qc

	ac := mock.NewMockAccessControlProvider(ctrl)
	ac.EXPECT().CreatePrincipal(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
	ac.EXPECT().VerifyMsgPrincipal(gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)
	if err := VerifyBlockSignatures(chainConf, ac, block, nil, GetValidatorList); err == nil {
		t.Errorf("VerifyBlockSignatures() error = %v, but expecte error", err)
	}
}

//
//  TestVerifyRoundQcSuccess
//  @Description: TestVerifyRoundQcSuccess
//  @param t
//
func TestVerifyRoundQcSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := mock.NewMockChainConf(ctrl)
	chainConf.EXPECT().GetChainConfigFromFuture(gomock.Any()).AnyTimes().Return(&configpb.ChainConfig{
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
	}, nil)

	var blockHeight uint64 = 1
	blockHash := nilHash

	chainConfig, _ := chainConf.GetChainConfigFromFuture(blockHeight)
	validators, _ := GetValidatorListFromConfig(chainConfig)
	validatorSet := newValidatorSet(cmLogger, validators, 1)
	voteSet := NewVoteSet(cmLogger, tbftpb.VoteType_VOTE_PRECOMMIT, blockHeight, 0, validatorSet)

	nodes := []string{
		org1NodeId,
		org2NodeId,
		"QmTSMcqwp4X6oPP5WrNpsMpotQMSGcxVshkGLJUhCrqGbu",
		"QmUryDgjNoxfMXHdDRFZ5Pe55R1vxTPA3ZgCteHze2ET27",
	}
	for _, id := range nodes {
		vote := NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, id, blockHeight, 0, blockHash)
		_, _ = voteSet.AddVote(vote, false)
	}

	roundqc := &tbftpb.RoundQC{
		Qc: voteSet.ToProto(),
	}

	//tLogger := newMockLogger()
	ac := mock.NewMockAccessControlProvider(ctrl)
	ac.EXPECT().CreatePrincipal(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
	ac.EXPECT().VerifyMsgPrincipal(gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)
	if err := VerifyRoundQc(tLogger, ac, validatorSet, roundqc, 0); err != nil {
		t.Errorf("VerifyRoundQc() error = %v, wantErr %v", err, nil)
	}
}

//
//  TestVerifyRoundQcWithoutMaj32
//  @Description: TestVerifyRoundQcWithoutMaj32
//  @param t
//
func TestVerifyRoundQcWithoutMaj32(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := mock.NewMockChainConf(ctrl)
	chainConf.EXPECT().GetChainConfigFromFuture(gomock.Any()).AnyTimes().Return(&configpb.ChainConfig{
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
	}, nil)

	var blockHeight uint64 = 10
	blockHash := nilHash

	chainConfig, _ := chainConf.GetChainConfigFromFuture(blockHeight)
	validators, _ := GetValidatorListFromConfig(chainConfig)
	validatorSet := newValidatorSet(cmLogger, validators, 1)
	voteSet := NewVoteSet(cmLogger, tbftpb.VoteType_VOTE_PRECOMMIT, blockHeight, 0, validatorSet)

	// precommits without majority
	nodes := []string{
		org1NodeId,
		org2NodeId,
	}
	for _, id := range nodes {
		vote := NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, id, blockHeight, 0, blockHash[:])
		_, _ = voteSet.AddVote(vote, false)
	}

	roundqc := &tbftpb.RoundQC{
		Qc: voteSet.ToProto(),
	}

	//tLogger := newMockLogger()
	ac := mock.NewMockAccessControlProvider(ctrl)
	ac.EXPECT().CreatePrincipal(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
	ac.EXPECT().VerifyMsgPrincipal(gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)
	if err := VerifyRoundQc(tLogger, ac, validatorSet, roundqc, 0); err == nil {
		t.Errorf("VerifyRoundQc() error = %v, but expecte error", err)
	}
}

//
//  TestVerifyRoundQcErrorMaj32
//  @Description: TestVerifyRoundQcErrorMaj32
//  @param t
//
func TestVerifyRoundQcErrorMaj32(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := mock.NewMockChainConf(ctrl)
	chainConf.EXPECT().GetChainConfigFromFuture(gomock.Any()).AnyTimes().Return(&configpb.ChainConfig{
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
	}, nil)

	// create a maj32 but not a nilhash
	var blockHeight uint64 = 10
	blockHash := sha256.Sum256(nil)
	_, _ = rand.Read(blockHash[:])

	chainConfig, _ := chainConf.GetChainConfigFromFuture(blockHeight)
	validators, _ := GetValidatorListFromConfig(chainConfig)
	validatorSet := newValidatorSet(cmLogger, validators, 1)
	voteSet := NewVoteSet(cmLogger, tbftpb.VoteType_VOTE_PRECOMMIT, blockHeight, 0, validatorSet)

	// precommits with majority and not a nilhash
	nodes := []string{
		org1NodeId,
		org2NodeId,
		"QmTSMcqwp4X6oPP5WrNpsMpotQMSGcxVshkGLJUhCrqGbu",
		"QmUryDgjNoxfMXHdDRFZ5Pe55R1vxTPA3ZgCteHze2ET27",
	}
	for _, id := range nodes {
		vote := NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, id, blockHeight, 0, blockHash[:])
		_, _ = voteSet.AddVote(vote, false)
	}

	roundqc := &tbftpb.RoundQC{
		Qc: voteSet.ToProto(),
	}

	//tLogger := newMockLogger()
	ac := mock.NewMockAccessControlProvider(ctrl)
	ac.EXPECT().CreatePrincipal(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
	ac.EXPECT().VerifyMsgPrincipal(gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)

	if err := VerifyRoundQc(tLogger, ac, validatorSet, roundqc, 0); err != nil {
		t.Errorf("VerifyRoundQc() error = %v, but expecte error", err)
	}
}

//
//  TestVerifyQcFromVotesSuccess
//  @Description: TestVerifyQcFromVotesSuccess
//  @param t
//
func TestVerifyQcFromVotesSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := mock.NewMockChainConf(ctrl)
	chainConf.EXPECT().GetChainConfigFromFuture(gomock.Any()).AnyTimes().Return(&configpb.ChainConfig{
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
	}, nil)

	var blockHeight uint64 = 1
	blockHash := nilHash

	chainConfig, _ := chainConf.GetChainConfigFromFuture(blockHeight)
	validators, _ := GetValidatorListFromConfig(chainConfig)
	validatorSet := newValidatorSet(cmLogger, validators, 1)

	nodes := []string{
		org1NodeId,
		org2NodeId,
		"QmTSMcqwp4X6oPP5WrNpsMpotQMSGcxVshkGLJUhCrqGbu",
		"QmUryDgjNoxfMXHdDRFZ5Pe55R1vxTPA3ZgCteHze2ET27",
	}

	var votes []*tbftpb.Vote
	for _, id := range nodes {
		vote := NewVote(tbftpb.VoteType_VOTE_PREVOTE, id, blockHeight, 0, blockHash)
		votes = append(votes, vote)
	}

	//tLogger := logger.GetLogger(chainid)
	ac := mock.NewMockAccessControlProvider(ctrl)
	ac.EXPECT().CreatePrincipal(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
	ac.EXPECT().VerifyMsgPrincipal(gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)
	if _, err := VerifyQcFromVotes(tLogger, votes, ac, validatorSet, tbftpb.VoteType_VOTE_PREVOTE, 0); err != nil {
		t.Errorf("VerifyQcFromVotes() error = %v, wantErr %v", err, nil)
	}
}
