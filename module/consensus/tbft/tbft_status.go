package tbft

import (
	"sync"
	"time"

	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/consensus-utils/v2/consistent_service"
	tbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/tbft"
	netpb "chainmaker.org/chainmaker/pb-go/v2/net"
	"chainmaker.org/chainmaker/protocol/v2"
)

const (
	// TypeRemoteTBFTState is the state of remote nodes
	TypeRemoteTBFTState = 1
	// TypeLocalTBFTState is the state of local node
	TypeLocalTBFTState = 2
)

// Node 节点信息（local/remoter）
type Node struct {
	sync.RWMutex
	id     string
	status map[int8]consistent_service.Status
}

// ID 返回节点ID
func (l *Node) ID() string {
	return l.id
}

// Statuses 返回节点状态
func (l *Node) Statuses() map[int8]consistent_service.Status {
	l.RLock()
	defer l.RUnlock()
	return l.status
}

// UpdateStatus 更新节点状态
func (l *Node) UpdateStatus(s consistent_service.Status) {
	l.Lock()
	defer l.Unlock()

	status := l.status[s.Type()]
	status.Update(s)
}

// RemoteState validator status, validator and remote are the same
//
type RemoteState struct {
	sync.RWMutex
	//node id
	Id string
	//current height
	Height uint64
	// current round
	Round int32
	// current step
	Step tbftpb.Step

	// proposal
	Proposal         []byte
	VerifingProposal []byte
	LockedRound      int32
	// locked proposal
	LockedProposal *tbftpb.Proposal
	ValidRound     int32
	// valid proposal
	ValidProposal *tbftpb.Proposal
	RoundVoteSet  *roundVoteSet
}

// Update 更新远端节点的状态
func (r *RemoteState) Update(state consistent_service.Status) {
	r.Lock()
	defer r.Unlock()
	local, ok := state.(*RemoteState)
	if !ok || local.Id != r.Id {
		return
	}
	r.Height = local.Height
	r.Round = local.Round
	r.Step = local.Step
	r.Proposal = local.Proposal
	r.ValidProposal = local.ValidProposal
	r.RoundVoteSet = local.RoundVoteSet
}

// Type 返回当前节点的节点类型, 此处表示远端节点
func (r *RemoteState) Type() int8 {
	return TypeRemoteTBFTState
}

// Data 返回状态内容
func (r *RemoteState) Data() interface{} {
	return r
}

// StatusDecoder 状态解析器
//
type StatusDecoder struct {
	log      protocol.Logger
	tbftImpl *ConsensusTBFTImpl
}

// MsgType 返回msgbus消息类型
func (tD *StatusDecoder) MsgType() int8 {
	return int8(msgbus.RecvConsistentMsg)
}

// Decode 解析消息，返回节点状态
func (tD *StatusDecoder) Decode(d interface{}) interface{} {
	m, ok := d.(*msgbus.Message)
	if !ok {
		return "not msgbus.Message"
	}
	msg, ok := m.Payload.(*netpb.NetMsg)
	if !ok {
		return "not netpb.NetMsg"
	}

	err := "msg.Type=" + msg.Type.String()
	// 仅处理共识消息中的STATE类型消息
	switch msg.Type {
	case netpb.NetMsg_CONSISTENT_MSG:
		consensusMsg := createStateMsgFromTBFTMsgBz(msg.Payload)
		if consensusMsg == nil {
			return "not state message"
		}
		err += " consensusMsg.Type=" + consensusMsg.Type.String()
		switch consensusMsg.Type {
		// 处理state消息
		case tbftpb.TBFTMsgType_MSG_STATE:
			state, ok := consensusMsg.Msg.(*tbftpb.GossipState)
			if !ok {
				return "tbftpb.GossipState"
			}

			rs := &RemoteState{
				Id:               state.Id,
				Height:           state.Height,
				Round:            state.Round,
				Step:             state.Step,
				Proposal:         state.Proposal,
				VerifingProposal: state.VerifingProposal,
				LockedRound:      0,
				LockedProposal:   nil,
				ValidRound:       0,
				ValidProposal:    nil,
				RoundVoteSet:     tD.newRoundVoteSet(state.RoundVoteSet),
			}

			// Update the block height of the validator node in the tbft module
			tD.tbftImpl.validatorSet.Lock()
			if tD.tbftImpl.validatorSet.validatorsHeight[state.Id] < state.Height {
				tD.tbftImpl.validatorSet.validatorsHeight[state.Id] =
					state.Height
			}
			tD.tbftImpl.validatorSet.validatorsBeatTime[state.Id] = time.Now().UnixNano() / 1e6
			tD.tbftImpl.validatorSet.Unlock()

			// fetch votes from this node state
			if tD.tbftImpl != nil && rs.Height == tD.tbftImpl.Height && rs.Round == tD.tbftImpl.Round &&
				rs.RoundVoteSet != nil {
				tD.log.Debugf("[%s] updateVoteWithProto: [%d/%d]", rs.Id, rs.Height, rs.Round)
				tD.updateVoteWithProto(rs.RoundVoteSet, rs.Round)
			}

			rss := make(map[int8]consistent_service.Status)
			rss[rs.Type()] = rs
			remoteInfo := &Node{id: state.Id, status: rss}
			return remoteInfo

		}
	}
	return err

}

// get the votes for tbft Engine based on the remote state
func (tD *StatusDecoder) updateVoteWithProto(voteSet *roundVoteSet, stateRound int32) {
	if voteSet.Prevotes == nil && voteSet.Precommits == nil {
		tD.log.Debugf("updateVoteWithProto failed, invalid voteSet: %+v", voteSet)
		return
	}
	validators := tD.tbftImpl.getValidatorSet().Validators
	tD.tbftImpl.RLock()
	defer tD.tbftImpl.RUnlock()
	// prevote Vote
	if voteSet.Prevotes != nil {
		for _, voter := range validators {
			tD.log.Debugf("%s updateVoteWithProto, voteSet.Prevotes: %+v", voter, voteSet.Prevotes)
			vote := voteSet.Prevotes.Votes[voter]
			if vote != nil && tD.tbftImpl.Step < tbftpb.Step_PRECOMMIT &&
				tD.tbftImpl.Id != vote.Voter &&
				tD.tbftImpl.heightRoundVoteSet.isRequired(stateRound, vote) {
				tD.log.Debugf("updateVoteWithProto prevote : %s", voter)
				if err := tD.tbftImpl.verifyVote(vote); err != nil {
					tD.log.Debugf("updateVoteWithProto prevote verify faield: %v", err)
					continue
				}
				tbftMsg := createPrevoteConsensusMsg(vote)
				tD.tbftImpl.internalMsgC <- tbftMsg
			}
		}
	}
	// precommit Vote
	if voteSet.Precommits != nil {
		for _, voter := range validators {
			tD.log.Debugf("%s updateVoteWithProto, voteSet.Precommits: %+v", voter, voteSet.Precommits)
			vote := voteSet.Precommits.Votes[voter]
			if vote != nil && tD.tbftImpl.Step < tbftpb.Step_COMMIT &&
				tD.tbftImpl.Id != vote.Voter &&
				tD.tbftImpl.heightRoundVoteSet.isRequired(stateRound, vote) {
				tD.log.Debugf("updateVoteWithProto precommit : %s", voter)
				if err := tD.tbftImpl.verifyVote(vote); err != nil {
					tD.log.Debugf("updateVoteWithProto precommit verify faield: %v", err)
					continue
				}
				tbftMsg := createPrecommitConsensusMsg(vote)
				tD.tbftImpl.internalMsgC <- tbftMsg
			}
		}
	}
}

// 创建新轮次投票集合
func (tD *StatusDecoder) newRoundVoteSet(rvs *tbftpb.RoundVoteSet) *roundVoteSet {
	if rvs == nil {
		return nil
	}
	return &roundVoteSet{
		Height:     rvs.Height,
		Round:      rvs.Round,
		Prevotes:   tD.newVoteSet(rvs.Prevotes),
		Precommits: tD.newVoteSet(rvs.Precommits),
	}
}

// 创建投票集合
func (tD *StatusDecoder) newVoteSet(vs *tbftpb.VoteSet) *VoteSet {
	if vs == nil {
		return nil
	}
	voteSet := &VoteSet{
		logger:       tD.log,
		Type:         vs.Type,
		Height:       vs.Height,
		Round:        vs.Round,
		Votes:        make(map[string]*tbftpb.Vote),
		VotesByBlock: make(map[string]*BlockVotes),
		validators:   nil, //本模块不使用
	}
	for _, v := range vs.Votes {
		added, err := voteSet.AddVoteForConsistent(v)
		if !added || err != nil {
			tD.log.Errorf("err: %s", err)
		}
	}
	return voteSet
}
