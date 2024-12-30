/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tbft

import (
	"testing"

	commonpb "chainmaker.org/chainmaker/pb-go/v2/common"
	configpb "chainmaker.org/chainmaker/pb-go/v2/config"
	consensuspb "chainmaker.org/chainmaker/pb-go/v2/consensus"
	tbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/tbft"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/protocol/v2/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var cstLogger protocol.Logger

//
//  init
//  @Description: init logger
//
func init() {
	// logger, _ := zap.NewDevelopment(zap.AddCaller())
	// clog = logger.Sugar()
	cstLogger = newMockLogger()
}

//
//  TestConsensusFutureMsgCache
//  @Description: TestConsensusFutureMsgCache
//  @param t
//
func TestConsensusFutureMsgCache(t *testing.T) {
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

	chainConfig, _ := chainConf.GetChainConfigFromFuture(blockHeight)
	validators, _ := GetValidatorListFromConfig(chainConfig)
	validatorSet := newValidatorSet(cmLogger, validators, 1)

	// new future msgs cache
	// size: 10 , init height 0
	cfc := newConsensusFutureMsgCache(cmLogger, 10, 0)
	var proposalHeight uint64 = 1
	var proposaRound int32
	blockHash := []byte("H1R0")
	block := &commonpb.Block{
		Header: &commonpb.BlockHeader{
			BlockHeight: proposalHeight,
			BlockHash:   blockHash[:],
		},
		AdditionalData: &commonpb.AdditionalData{
			ExtraData: map[string][]byte{
				TBFTAddtionalDataKey: nil,
			},
		},
	}

	proposal := &tbftpb.Proposal{
		Voter:  org1NodeId,
		Height: proposalHeight,
		Round:  proposaRound,
		Block:  block,
	}
	// add a future proposal
	cfc.addFutureProposal(validatorSet, proposal)

	// get a future proposal
	cacheProposal := cfc.getConsensusFutureProposal(proposalHeight, proposaRound)
	// the proposal is expected to be got from the cache
	require.Equal(t, cacheProposal.Height, proposalHeight)
	require.Equal(t, cacheProposal.Round, proposaRound)
	require.Equal(t, cacheProposal.Block.Hash(), blockHash)

	var voteHeight uint64 = 2
	var voteRound int32
	voteHash := []byte("H2R0")
	prevote := NewVote(tbftpb.VoteType_VOTE_PREVOTE, org1NodeId, voteHeight, voteRound, voteHash[:])
	precommit := NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, org1NodeId, voteHeight, voteRound, voteHash[:])

	// add future votes
	cfc.addFutureVote(validatorSet, prevote)
	cfc.addFutureVote(validatorSet, precommit)

	// get future votes
	cacheVs := cfc.getConsensusFutureVote(voteHeight, voteRound)
	// the votes is expected to be got from the cache
	require.Equal(t, cacheVs.Prevotes.Votes[org1NodeId].Hash, voteHash)
	require.Equal(t, cacheVs.Precommits.Votes[org1NodeId].Hash, voteHash)

	// update height of consensus
	var voteHeight2 uint64 = 2
	cfc.updateConsensusHeight(voteHeight2)
	// add votes of height 11
	var voteHeight11 uint64 = 11
	var voteRound11 int32 = 1
	voteHash11 := []byte("H11R1")
	prevote11 := NewVote(tbftpb.VoteType_VOTE_PREVOTE, org1NodeId, voteHeight11, voteRound11, voteHash11[:])
	precommit11 := NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, org1NodeId, voteHeight11, voteRound11, voteHash11[:])
	cfc.addFutureVote(validatorSet, prevote11)
	cfc.addFutureVote(validatorSet, precommit11)

	// update height
	// test gc
	var updateHeight uint64 = 10
	cfc.updateConsensusHeight(updateHeight)

	// cached msgs should be gc before 10 height
	if cacheProposal = cfc.getConsensusFutureProposal(proposalHeight, proposaRound); cacheProposal != nil {
		t.Errorf("getConsensusFutureProposal cacheProposal = %v, want nil", cacheProposal)
	}
	if cacheVs = cfc.getConsensusFutureVote(voteHeight, voteRound); cacheVs != nil {
		t.Errorf("getConsensusFutureVote cacheVs = %v, want nil", cacheVs)
	}

	// get votes of height 11
	cacheVs = cfc.getConsensusFutureVote(voteHeight11, voteRound11)
	// the votes is expected to be got from the cache
	require.Equal(t, cacheVs.Prevotes.Votes[org1NodeId].Hash, voteHash11)
	require.Equal(t, cacheVs.Precommits.Votes[org1NodeId].Hash, voteHash11)
}
