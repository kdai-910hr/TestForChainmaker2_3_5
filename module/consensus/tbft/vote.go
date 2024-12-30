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
	"strings"

	"chainmaker.org/chainmaker/pb-go/v2/common"
	consensuspb "chainmaker.org/chainmaker/pb-go/v2/consensus"
	tbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/tbft"
	"chainmaker.org/chainmaker/protocol/v2"
)

//const (
//	nilVoteStr = "nil vote"
//)

var (
	// ErrVoteNil implements the error of nil vote
	ErrVoteNil = errors.New("nil vote")
	// ErrUnexceptedStep implements the error of unexpected step in tbft
	ErrUnexceptedStep = errors.New("unexpected step")
	// ErrInvalidValidator implements the error of nil invalid validator
	ErrInvalidValidator = errors.New("invalid validator")
	// ErrVoteForDifferentHash implements the error of invalid hash
	ErrVoteForDifferentHash = errors.New("vote for different hash")
)

// NewProposal create a new Proposal
func NewProposal(voter string, height uint64, round int32, polRound int32, block *common.Block) *tbftpb.Proposal {
	return &tbftpb.Proposal{
		Voter:    voter,
		Height:   height,
		Round:    round,
		PolRound: polRound,
		Block:    block,
	}
}

// CopyProposalWithBlockHeader create a new Proposal instance for sign and verify
func CopyProposalWithBlockHeader(p *tbftpb.Proposal) *tbftpb.Proposal {
	if p == nil {
		return nil
	}
	return &tbftpb.Proposal{
		Voter:    p.Voter,
		Height:   p.Height,
		Round:    p.Round,
		PolRound: p.PolRound,
		Block: &common.Block{
			Header: p.Block.Header,
		},
	}
}

// NewProposalBlock create a new ProposalBlock
func NewProposalBlock(block *common.Block, txsRwSet map[string]*common.TxRWSet) *consensuspb.ProposalBlock {
	return &consensuspb.ProposalBlock{
		Block:    block,
		TxsRwSet: txsRwSet,
	}
}

// CopyBlock generates a new block with a old block, internally using the same pointer
func CopyBlock(block *common.Block) *common.Block {
	return &common.Block{
		Header:         block.Header,
		Dag:            block.Dag,
		Txs:            block.Txs,
		// FYZ
		Method: 		block.Method,
		AdditionalData: block.AdditionalData,
	}
}

// NewVote create a new Vote instance
func NewVote(typ tbftpb.VoteType, voter string, height uint64, round int32, hash []byte) *tbftpb.Vote {
	return &tbftpb.Vote{
		Type:   typ,
		Voter:  voter,
		Height: height,
		Round:  round,
		Hash:   hash,
	}
}

// BlockVotes traces the vote from different voter
type BlockVotes struct {
	Votes map[string]*tbftpb.Vote
	Sum   uint64
}

// NewBlockVotes creates a new BlockVotes instance
func NewBlockVotes() *BlockVotes {
	return &BlockVotes{
		Votes: make(map[string]*tbftpb.Vote),
	}
}

// ToProto serializes the BlockVotes instance
func (bv *BlockVotes) ToProto() *tbftpb.BlockVotes {
	if bv == nil {
		return nil
	}

	bvProto := &tbftpb.BlockVotes{
		Votes: make(map[string]*tbftpb.Vote),
		Sum:   bv.Sum,
	}

	for k, v := range bv.Votes {
		bvProto.Votes[k] = v
	}

	return bvProto
}

func (bv *BlockVotes) addVote(vote *tbftpb.Vote) {
	bv.Votes[vote.Voter] = vote
	bv.Sum++
}

// VoteSet wraps tbftpb.VoteSet and validatorSet
type VoteSet struct {
	logger       protocol.Logger
	Type         tbftpb.VoteType
	Height       uint64
	Round        int32
	Sum          uint64
	Maj23        []byte
	Votes        map[string]*tbftpb.Vote
	VotesByBlock map[string]*BlockVotes
	invalidTx    map[string]int32
	needToDelTxs []string
	validators   *validatorSet
}

// NewVoteSet creates a new VoteSet instance
func NewVoteSet(logger protocol.Logger, voteType tbftpb.VoteType, height uint64, round int32,
	validators *validatorSet) *VoteSet {
	return &VoteSet{
		logger:       logger,
		Type:         voteType,
		Height:       height,
		Round:        round,
		Votes:        make(map[string]*tbftpb.Vote),
		VotesByBlock: make(map[string]*BlockVotes),
		invalidTx:    make(map[string]int32),
		validators:   validators,
	}
}

// NewVoteSetFromProto creates a new VoteSet instance from pb
func NewVoteSetFromProto(logger protocol.Logger, vsProto *tbftpb.VoteSet, validators *validatorSet) *VoteSet {
	vs := NewVoteSet(logger, vsProto.Type, vsProto.Height, vsProto.Round, validators)

	for _, v := range vsProto.Votes {
		added, err := vs.AddVote(v, false)
		if !added || err != nil {
			logger.Errorf("validators: %s, vote: %s", validators, v)
		}
	}

	return vs
}

// ToProto serializes the VoteSet instance
func (vs *VoteSet) ToProto() *tbftpb.VoteSet {
	if vs == nil {
		return nil
	}

	vsProto := &tbftpb.VoteSet{
		Type:         vs.Type,
		Height:       vs.Height,
		Round:        vs.Round,
		Sum:          vs.Sum,
		Maj23:        vs.Maj23,
		Votes:        make(map[string]*tbftpb.Vote),
		VotesByBlock: make(map[string]*tbftpb.BlockVotes),
	}

	for k, v := range vs.Votes {
		vsProto.Votes[k] = v
	}

	for k, v := range vs.VotesByBlock {
		vsProto.VotesByBlock[k] = v.ToProto()
	}

	return vsProto
}

func (vs *VoteSet) String() string {
	if vs == nil {
		return ""
	}
	var builder strings.Builder
	fmt.Fprintf(&builder, "{Type: %s, Height: %d, Round: %d, Votes: [",
		vs.Type, vs.Height, vs.Round)
	for k := range vs.Votes {
		fmt.Fprintf(&builder, " %s,", k)
	}
	fmt.Fprintf(&builder, "]}")
	return builder.String()
}

// Size returns the size of the VoteSet
func (vs *VoteSet) Size() int32 {
	if vs == nil {
		return 0
	}
	return vs.validators.Size()
}

func (vs *VoteSet) checkVoteMatched(vote *tbftpb.Vote) bool {
	if vote.Type != vs.Type ||
		vote.Height != vs.Height ||
		vote.Round != vs.Round {
		return false
	}
	return true
}

// AddVote adds a vote to the VoteSet
func (vs *VoteSet) AddVote(vote *tbftpb.Vote, countInvalidTx bool) (added bool, err error) {
	if vs == nil {
		// This should not happen
		vs.logger.Errorf("AddVote on nil VoteSet")
		panic("AddVote on nil VoteSet")
	}

	if vote == nil {
		vs.logger.Errorf("%v add nil vote error", vs)
		return false, fmt.Errorf("%w %v", ErrVoteNil, vote)
	}

	if !vs.checkVoteMatched(vote) {
		vs.logger.Infof("expect %s/%d/%d, got %s/%d/%d",
			vs.Type, vs.Height, vs.Round, vote.Type, vote.Height, vote.Round)
		return false, fmt.Errorf("%w expect %s/%d/%d, got %s/%d/%d",
			ErrUnexceptedStep, vs.Type, vs.Height, vs.Round,
			vote.Type, vote.Height, vote.Round)
	}

	if !vs.validators.HasValidator(vote.Voter) {
		return false, fmt.Errorf("%w %s", ErrInvalidValidator, vote.Voter)
	}

	if v, ok := vs.Votes[vote.Voter]; ok {
		if bytes.Equal(vote.Hash, v.Hash) {
			return false, nil
		}
		return false, fmt.Errorf("%w existing: %v, new: %v",
			ErrVoteForDifferentHash, v.Hash, vote.Hash)
	}

	vs.Votes[vote.Voter] = vote
	vs.Sum++

	hashStr := base64.StdEncoding.EncodeToString(vote.Hash)
	votesByBlock, ok := vs.VotesByBlock[hashStr]
	if !ok {
		votesByBlock = NewBlockVotes()
		vs.VotesByBlock[hashStr] = votesByBlock
	}

	oldSum := votesByBlock.Sum
	quorum := uint64(vs.validators.Size()*2/3 + 1)

	votesByBlock.addVote(vote)
	vs.logger.Debugf("VoteSet(%s/%d/%d) AddVote %s(%s/%d/%d/%x) "+
		"oldSum: %d, quorum: %d, sum: %d",
		vs.Type, vs.Height, vs.Round, vote.Voter, vote.Type, vote.Height, vote.Round,
		vote.Hash, oldSum, quorum, votesByBlock.Sum)

	if oldSum < quorum && quorum <= votesByBlock.Sum && vs.Maj23 == nil {
		//vs.logger.Infof("VoteSet(%s/%d/%d) AddVote reach majority %x",
		//	vs.Type, vs.Height, vs.Round, vote.Hash)
		vs.Maj23 = vote.Hash

		for k, v := range votesByBlock.Votes {
			vs.Votes[k] = v
		}
	}

	// add invalid txs from prevote
	// todo : maybe there's a more elegant implementation here
	if vote.Type == tbftpb.VoteType_VOTE_PREVOTE && countInvalidTx {
		for _, tx := range vote.InvalidTxs {
			vs.invalidTx[tx]++
			if vs.invalidTx[tx] >= vs.validators.Size()*1/3+1 {
				vs.needToDelTxs = append(vs.needToDelTxs, tx)
				// avoid adding duplicate tx
				vs.invalidTx[tx] -= vs.validators.Size()
				vs.logger.Debugf("AddVote add invalidTxs = %s", tx)
			}
		}
	}
	return true, nil
}

// AddVoteForConsistent adds a vote to the VoteSet
func (vs *VoteSet) AddVoteForConsistent(vote *tbftpb.Vote) (added bool, err error) {
	if vs == nil {
		// This should not happen
		vs.logger.Errorf("AddVote on nil VoteSet")
		panic("AddVote on nil VoteSet")
	}

	if vote == nil {
		vs.logger.Errorf("%v add nil vote error", vs)
		return false, fmt.Errorf("%w %v", ErrVoteNil, vote)
	}

	if !vs.checkVoteMatched(vote) {
		vs.logger.Infof("expect %s/%d/%d, got %s/%d/%d",
			vs.Type, vs.Height, vs.Round, vote.Type, vote.Height, vote.Round)
		return false, fmt.Errorf("%w expect %s/%d/%d, got %s/%d/%d",
			ErrUnexceptedStep, vs.Type, vs.Height, vs.Round,
			vote.Type, vote.Height, vote.Round)
	}

	if v, ok := vs.Votes[vote.Voter]; ok {
		if bytes.Equal(vote.Hash, v.Hash) {
			return false, nil
		}
		return false, fmt.Errorf("%w existing: %v, new: %v",
			ErrVoteForDifferentHash, v.Hash, vote.Hash)
	}

	vs.Votes[vote.Voter] = vote
	vs.Sum++

	return true, nil
}

// delInvalidTx return if it has invalid txs in voteset
func (vs *VoteSet) delInvalidTx(blockHeight uint64) *consensuspb.RwSetVerifyFailTxs {
	if vs == nil {
		return nil
	}
	if vs.needToDelTxs == nil || len(vs.needToDelTxs) <= 0 {
		return nil
	}

	return &consensuspb.RwSetVerifyFailTxs{BlockHeight: blockHeight,
		TxIds: vs.needToDelTxs}
}

func (vs *VoteSet) twoThirdsMajority() (hash []byte, ok bool) {
	if vs == nil {
		return nil, false
	}

	if vs.Maj23 != nil {
		//vs.logger.Debugf("VoteSet(%s/%d/%d) TwoThirdsMajority (%x/%v)",
		//	vs.Type, vs.Height, vs.Round, vs.Maj23, true)
		return vs.Maj23, true
	}
	vs.logger.Debugf("VoteSet(%s/%d/%d) TwoThirdsMajority (%x/%v)",
		vs.Type, vs.Height, vs.Round, vs.Maj23, false)
	return nil, false
}

// HasTwoThirdsMajority shoule used when the mutex has been lock
func (vs *VoteSet) HasTwoThirdsMajority() (majority bool) {
	if vs == nil {
		return false
	}

	return vs.Maj23 != nil
}

func (vs *VoteSet) hasTwoThirdsAny() bool {
	if vs == nil {
		return false
	}

	ret := true
	leftSum := uint64(vs.validators.Size()) - vs.Sum
	for _, v := range vs.VotesByBlock {
		if (v.Sum + leftSum) >= uint64(vs.validators.Size()*2/3+1) {
			ret = false
			break
		}
	}
	vs.logger.Debugf("VoteSet(%s/%d/%d) sum: %v, HasTwoThirdsAny return %v",
		vs.Type, vs.Height, vs.Round, vs.Sum, ret)
	return ret
}

// 2f+1 any votes received
func (vs *VoteSet) hasTwoThirdsNoMajority() bool {
	if vs == nil {
		return false
	}

	return vs.Sum >= uint64(vs.validators.Size()*2/3+1)
}

type roundVoteSet struct {
	Height     uint64
	Round      int32
	Prevotes   *VoteSet
	Precommits *VoteSet
}

func newRoundVoteSet(height uint64, round int32, prevotes *VoteSet, precommits *VoteSet) *roundVoteSet {
	return &roundVoteSet{
		Height:     height,
		Round:      round,
		Prevotes:   prevotes,
		Precommits: precommits,
	}
}

//func newRoundVoteSetFromProto(logger *logger.CMLogger, rvs *tbftpb.RoundVoteSet,
//	validators *validatorSet) *roundVoteSet {
//	if rvs == nil {
//		return nil
//	}
//	prevotes := NewVoteSetFromProto(logger, rvs.Prevotes, validators)
//	precommits := NewVoteSetFromProto(logger, rvs.Precommits, validators)
//	return newRoundVoteSet(rvs.Height, rvs.Round, prevotes, precommits)
//}

func (rvs *roundVoteSet) ToProto() *tbftpb.RoundVoteSet {
	if rvs == nil {
		return nil
	}

	rvsProto := &tbftpb.RoundVoteSet{
		Height:     rvs.Height,
		Round:      rvs.Round,
		Prevotes:   rvs.Prevotes.ToProto(),
		Precommits: rvs.Precommits.ToProto(),
	}
	return rvsProto
}

func (rvs *roundVoteSet) String() string {
	if rvs == nil {
		return ""
	}
	return fmt.Sprintf("Height: %d, Round: %d, Prevotes: %s, Precommits: %s",
		rvs.Height, rvs.Round, rvs.Prevotes, rvs.Precommits)
}

type heightRoundVoteSet struct {
	logger        protocol.Logger
	Height        uint64
	Round         int32
	RoundVoteSets map[int32]*roundVoteSet

	validators *validatorSet
}

func newHeightRoundVoteSet(logger protocol.Logger, height uint64, round int32,
	validators *validatorSet) *heightRoundVoteSet {
	hvs := &heightRoundVoteSet{
		logger:        logger,
		Height:        height,
		Round:         round,
		RoundVoteSets: make(map[int32]*roundVoteSet),

		validators: validators,
	}
	return hvs
}

//func newHeightRoundVoteSetFromProto(logger *logger.CMLogger, hvsProto *tbftpb.HeightRoundVoteSet,
//	validators *validatorSet) *heightRoundVoteSet {
//	hvs := newHeightRoundVoteSet(logger, hvsProto.Height, hvsProto.Round, validators)
//
//	for k, v := range hvsProto.RoundVoteSets {
//		rvs := NewRoundVoteSetFromProto(logger, v, validators)
//		hvs.RoundVoteSets[k] = rvs
//	}
//
//	return hvs
//}

func (hvs *heightRoundVoteSet) ToProto() *tbftpb.HeightRoundVoteSet {
	if hvs == nil {
		return nil
	}

	hvsProto := &tbftpb.HeightRoundVoteSet{
		Height:        hvs.Height,
		Round:         hvs.Round,
		RoundVoteSets: make(map[int32]*tbftpb.RoundVoteSet),
	}

	for k, v := range hvs.RoundVoteSets {
		hvsProto.RoundVoteSets[k] = v.ToProto()
	}

	return hvsProto
}

func (hvs *heightRoundVoteSet) addRound(round int32) {
	if _, ok := hvs.RoundVoteSets[round]; ok {
		// This should not happen
		panic(fmt.Errorf("round %d alread exists", round))
	}

	prevotes := NewVoteSet(hvs.logger, tbftpb.VoteType_VOTE_PREVOTE, hvs.Height, round, hvs.validators)
	precommits := NewVoteSet(hvs.logger, tbftpb.VoteType_VOTE_PRECOMMIT, hvs.Height, round, hvs.validators)
	hvs.RoundVoteSets[round] = newRoundVoteSet(hvs.Height, round, prevotes, precommits)
}

func (hvs *heightRoundVoteSet) getRoundVoteSet(round int32) *roundVoteSet {
	rvs, ok := hvs.RoundVoteSets[round]
	if !ok {
		return nil
	}
	return rvs
}

func (hvs *heightRoundVoteSet) getVoteSet(round int32, voteType tbftpb.VoteType) *VoteSet {
	rvs, ok := hvs.RoundVoteSets[round]
	if !ok {
		return nil
	}

	switch voteType {
	case tbftpb.VoteType_VOTE_PREVOTE:
		return rvs.Prevotes
	case tbftpb.VoteType_VOTE_PRECOMMIT:
		return rvs.Precommits
	default:
		// This should not happen
		panic(fmt.Errorf("invalid VoteType %s", voteType))
	}
}

func (hvs *heightRoundVoteSet) prevotes(round int32) *VoteSet {
	return hvs.getVoteSet(round, tbftpb.VoteType_VOTE_PREVOTE)
}

func (hvs *heightRoundVoteSet) precommits(round int32) *VoteSet {
	return hvs.getVoteSet(round, tbftpb.VoteType_VOTE_PRECOMMIT)
}

func (hvs *heightRoundVoteSet) addVote(vote *tbftpb.Vote) (added bool, err error) {
	voteSet := hvs.getVoteSet(vote.Round, vote.Type)
	if voteSet == nil {
		hvs.addRound(vote.Round)
		voteSet = hvs.getVoteSet(vote.Round, vote.Type)
	}

	added, err = voteSet.AddVote(vote, true)
	return
}

func (hvs *heightRoundVoteSet) isRequired(round int32, vote *tbftpb.Vote) bool {
	if vote == nil {
		return false
	}
	voteSet := hvs.getVoteSet(round, vote.Type)
	if voteSet == nil || voteSet.Votes == nil {
		return true
	}

	// votes that are not validators are not required
	if !voteSet.validators.HasValidator(vote.Voter) {
		return false
	}
	// is exist
	_, ok := voteSet.Votes[vote.Voter]
	return !ok
}

func createProposalTBFTMsg(proposal *TBFTProposal) *tbftpb.TBFTMsg {
	return &tbftpb.TBFTMsg{
		Type: tbftpb.TBFTMsgType_MSG_PROPOSE,
		Msg:  proposal.Bytes,
	}
}

func createPrevoteTBFTMsg(prevote *tbftpb.Vote) *tbftpb.TBFTMsg {
	return &tbftpb.TBFTMsg{
		Type: tbftpb.TBFTMsgType_MSG_PREVOTE,
		Msg:  mustMarshal(prevote),
	}
}

func createPrecommitTBFTMsg(precommit *tbftpb.Vote) *tbftpb.TBFTMsg {
	return &tbftpb.TBFTMsg{
		Type: tbftpb.TBFTMsgType_MSG_PRECOMMIT,
		Msg:  mustMarshal(precommit),
	}
}

// ConsensusMsg implements transformation of structure and pb
type ConsensusMsg struct {
	Type tbftpb.TBFTMsgType
	Msg  interface{}
}

func createPrevoteConsensusMsg(prevote *tbftpb.Vote) *ConsensusMsg {
	return &ConsensusMsg{
		Type: tbftpb.TBFTMsgType_MSG_PREVOTE,
		Msg: &tbftpb.Vote{
			Type:        prevote.Type,
			Voter:       prevote.Voter,
			Height:      prevote.Height,
			Round:       prevote.Round,
			Hash:        prevote.Hash,
			InvalidTxs:  prevote.InvalidTxs,
			Endorsement: prevote.Endorsement,
		},
	}
}

func createPrecommitConsensusMsg(precommit *tbftpb.Vote) *ConsensusMsg {
	return &ConsensusMsg{
		Type: tbftpb.TBFTMsgType_MSG_PRECOMMIT,
		Msg: &tbftpb.Vote{
			Type:        precommit.Type,
			Voter:       precommit.Voter,
			Height:      precommit.Height,
			Round:       precommit.Round,
			Hash:        precommit.Hash,
			Endorsement: precommit.Endorsement,
		},
	}
}
