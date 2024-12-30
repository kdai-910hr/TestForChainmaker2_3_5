/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tbft

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"path"
	"strconv"
	"sync"
	"time"

	consensus_utils "chainmaker.org/chainmaker/consensus-utils/v2"
	"chainmaker.org/chainmaker/consensus-utils/v2/wal_service"
	"chainmaker.org/chainmaker/localconf/v2"
	"chainmaker.org/chainmaker/logger/v2"
	"chainmaker.org/chainmaker/lws"
	"chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/config"
	"chainmaker.org/chainmaker/pb-go/v2/consensus"
	tbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/tbft"
	"chainmaker.org/chainmaker/protocol/v2"
	"github.com/gogo/protobuf/proto"
)

// GetValidatorList get Validator List From Config
// @param chainConfig
// @param store Not currently in use
// @return validators
// @return err
//
func GetValidatorList(chainConfig *config.ChainConfig, store protocol.BlockchainStore) (validators []string,
	err error) {
	if chainConfig.Consensus.Type == consensus.ConsensusType_TBFT {
		return GetValidatorListFromConfig(chainConfig)
	}
	return nil, fmt.Errorf("unknown consensus type: %s", chainConfig.Consensus.Type)
}

// GetValidatorListFromConfig get Validator List From Config
// @param chainConfig
// @return validators
// @return err
//
func GetValidatorListFromConfig(chainConfig *config.ChainConfig) (validators []string, err error) {
	nodes := chainConfig.Consensus.Nodes
	for _, node := range nodes {
		//for _, nid := range node.NodeId {
		//	validators = append(validators, nid)
		//}
		validators = append(validators, node.NodeId...)
	}
	return validators, nil
}

// VerifyBlockSignatures verifies whether the signatures in block
// is qulified with the consensus algorithm. It should return nil
// error when verify successfully, and return corresponding error
// when failed.
func VerifyBlockSignatures(chainConf protocol.ChainConf,
	ac protocol.AccessControlProvider, block *common.Block, store protocol.BlockchainStore,
	validatorListFunc consensus_utils.ValidatorListFunc) error {

	if block == nil || block.Header == nil || block.AdditionalData == nil || block.AdditionalData.ExtraData == nil {
		return fmt.Errorf("invalid block")
	}
	blockVoteSet, ok := block.AdditionalData.ExtraData[TBFTAddtionalDataKey]
	if !ok {
		return fmt.Errorf("block.AdditionalData.ExtraData[TBFTAddtionalDataKey] not exist")
	}

	voteSetProto := new(tbftpb.VoteSet)
	if err := proto.Unmarshal(blockVoteSet, voteSetProto); err != nil {
		return err
	}

	height := block.Header.BlockHeight
	chainConfig, err := chainConf.GetChainConfigFromFuture(height)
	if err != nil {
		return err
	}

	validators, err := validatorListFunc(chainConfig, store)
	if err != nil {
		return err
	}

	logger := logger.GetLoggerByChain(logger.MODULE_CONSENSUS, chainConfig.ChainId)
	validatorSet := newValidatorSet(logger, validators, DefaultBlocksPerProposer)
	voteSet := NewVoteSetFromProto(logger, voteSetProto, validatorSet)
	hash, ok := voteSet.twoThirdsMajority()
	if !ok {
		return fmt.Errorf("voteSet without majority")
	}

	if !bytes.Equal(hash, block.Header.BlockHash) {
		return fmt.Errorf("unmatch QC: %x to block hash: %v", hash, block.Header.BlockHash)
	}

	hashStr := base64.StdEncoding.EncodeToString(hash)
	blockVotes := voteSet.VotesByBlock[hashStr]
	// blockVotes should contain valid vote only, otherwise the block is invalid
	err = verifyVotes(blockVotes.Votes, ac, block.GetHeader().GetBlockVersion())
	if err != nil {
		clog.Debugf("VerifyBlockSignatures block (%d-%x-%v) failed",
			block.Header.BlockHeight, block.Header.BlockHash, err)
		return err
	}

	clog.Debugf("VerifyBlockSignatures block (%d-%x) success",
		block.Header.BlockHeight, block.Header.BlockHash)
	return nil
}

// VerifyRoundQc verifies whether the signatures in roundQC
// verify that the Qc is nil hash and the maj32 of the voteSet
// error when verify successfully, and return corresponding error
// when failed.
func VerifyRoundQc(logger protocol.Logger, ac protocol.AccessControlProvider,
	validators *validatorSet, roundQC *tbftpb.RoundQC, blockVersion uint32) error {
	voteSet := NewVoteSetFromProto(logger, roundQC.Qc, validators)
	hash, ok := voteSet.twoThirdsMajority()
	// we need a QC
	if !ok || roundQC.Qc.Type != voteSet.Type {
		return fmt.Errorf("qc without majority, ok = %v, type = %v, hash = %x", ok, roundQC.Qc.Type, hash)
	}

	hashStr := base64.StdEncoding.EncodeToString(hash)
	blockVotes := voteSet.VotesByBlock[hashStr]
	//
	err := verifyVotes(blockVotes.Votes, ac, blockVersion)
	if err != nil {
		clog.Debugf("verify round qc signatures failed, %v", err)
		return err
	}

	clog.Debugf("verify round qc signatures success")
	return nil
}

// VerifyQcFromVotes verifies whether the signatures in votes
// verify that the maj32 of the votes
// error when verify successfully, and return corresponding error
// when failed.
func VerifyQcFromVotes(logger protocol.Logger, vs []*tbftpb.Vote, ac protocol.AccessControlProvider,
	validators *validatorSet, voteType tbftpb.VoteType, blockVersion uint32) (*VoteSet, error) {
	if vs == nil || len(vs) <= 0 || voteType != vs[0].Type {
		logger.Warnf("invalid []*tbftpb.Vote, [%v]", vs)
		return nil, fmt.Errorf("invalid []*tbftpb.Vote")
	}
	voteSet := NewVoteSet(logger, vs[0].Type, vs[0].Height, vs[0].Round, validators)
	for _, v := range vs {
		_, err := voteSet.AddVote(v, false)
		if err != nil {
			return nil, err
		}
	}

	hash, ok := voteSet.twoThirdsMajority()
	// we need a QC
	if !ok {
		return nil, fmt.Errorf("votes qc without majority, ok = %v, type = %v, hash = %x", ok, voteSet.Type, hash)
	}

	hashStr := base64.StdEncoding.EncodeToString(hash)
	blockVotes := voteSet.VotesByBlock[hashStr]
	//
	err := verifyVotes(blockVotes.Votes, ac, blockVersion)
	if err != nil {
		clog.Debugf("verify vote signatures failed, %v", err)
		return nil, err
	}

	clog.Debugf("verify votes success")
	return voteSet, nil
}
func verifyVotes(votes map[string]*tbftpb.Vote, ac protocol.AccessControlProvider, blockVersion uint32) error {
	if votes == nil {
		return errors.New("invalid votes")
	}

	wg := sync.WaitGroup{}
	// no lock is needed to protect retErr, get any error code
	var retErr error
	for _, value := range votes {
		wg.Add(1)
		go func(v *tbftpb.Vote) {
			var err error
			defer func() {
				wg.Done()
				if err != nil {
					retErr = err
				}
			}()

			vote, ok := proto.Clone(v).(*tbftpb.Vote)
			if !ok {
				err = fmt.Errorf("interface transfer to *tbftpb.Vote failed")
				return
			}
			vote.Endorsement = nil
			message := mustMarshal(vote)

			principal, err := ac.CreatePrincipal(
				protocol.ResourceNameConsensusNode,
				[]*common.EndorsementEntry{v.Endorsement},
				message,
			)
			if err != nil {
				clog.Infof("verify signatures vote(%s) error: %v", v.Voter, err)
				return
			}

			result, err := ac.VerifyMsgPrincipal(principal, blockVersion)
			if err != nil {
				clog.Infof("verify signatures vote(%s) error: %v", v.Voter, err)
				return
			}

			if !result {
				clog.Infof("verify signatures vote(%s) because result: %v", v.Voter, result)
				err = fmt.Errorf("verifyVote result: %v", result)
				return
			}
		}(value)
	}

	wg.Wait()

	return retErr
}

// InitLWS initialize LWS
// @param config Consensus Config
// @param chainId
// @param nodeId
// @return lwsInstance
// @return walWriteMode
// @return err
//
func InitLWS(config *config.ConsensusConfig, chainId, nodeId string) (lwsInstance *lws.Lws,
	walWriteMode wal_service.WalWriteMode, err error) {
	for _, v := range config.ExtConfig {
		if v.Key == wal_service.WALWriteModeKey {
			val, conv_err := strconv.Atoi(v.Value)
			if conv_err != nil {
				return nil, wal_service.NonWalWrite, err
			}
			walWriteMode = wal_service.WalWriteMode(val)
		}
	}

	waldir := path.Join(localconf.ChainMakerConfig.GetStorePath(), chainId,
		fmt.Sprintf("%s_%s", wal_service.WalDir, nodeId))
	// the max size of wal file is 64M
	// the max number of wal files is 3
	lwsInstance, err = lws.Open(waldir, lws.WithSegmentSize(1<<26), lws.WithFileLimitForPurge(3),
		lws.WithWriteFlag(lws.WF_SYNCFLUSH, 0))
	if err != nil {
		return nil, wal_service.NonWalWrite, err
	}
	return
}

// CurrentTime return current time
func CurrentTime() time.Time {
	return time.Now()
}
