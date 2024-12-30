package tbft

import (
	"errors"
	"time"

	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/consensus-utils/v2/consistent_service"
	"chainmaker.org/chainmaker/localconf/v2"
	tbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/tbft"
	netpb "chainmaker.org/chainmaker/pb-go/v2/net"
	"chainmaker.org/chainmaker/protocol/v2"

	"github.com/gogo/protobuf/proto"
)

const (
	// MessageBufferSize 缓存消息大小
	MessageBufferSize = 10240

	// StatusBroadcasterTbft TBFT状态广播器ID
	StatusBroadcasterTbft = "TBFT"
	// InterceptorTbft 标识
	InterceptorTbft = 0
)

// TimerInterval 定时器间隔
var TimerInterval = 1000 * time.Millisecond

// StatusBroadcaster 一致性引擎的状态和消息的broadcaster实现
//
type StatusBroadcaster struct {
	log protocol.Logger

	// broadcaster 标识
	id string
	// 运行状态
	running bool
}

// IsRunning 返回运行状态
func (tsb *StatusBroadcaster) IsRunning() bool {
	return tsb.running
}

// Start 启动
func (tsb *StatusBroadcaster) Start() error {
	tsb.running = true
	return nil
}

// Stop 停止
func (tsb *StatusBroadcaster) Stop() error {
	tsb.running = false
	return nil
}

// NewTBFTStatusBroadcaster 得到一个broadcaster实例
func NewTBFTStatusBroadcaster(log protocol.Logger) *StatusBroadcaster {
	bsb := &StatusBroadcaster{
		log:     log,
		id:      StatusBroadcasterTbft,
		running: false,
	}
	return bsb
}

// ID 返回broadcaster 标识
func (tsb *StatusBroadcaster) ID() string {
	return tsb.id
}

//TimePattern 状态广播触发模式
func (tsb *StatusBroadcaster) TimePattern() interface{} {
	return TimerInterval
}

//PreBroadcaster 消息广播前做前置处理，如状态校验/判断是否要发送消息等
func (tsb *StatusBroadcaster) PreBroadcaster() consistent_service.Broadcast {
	return func(localInfo consistent_service.Node, remoteInfo consistent_service.Node) (interface{}, error) {
		tsb.log.Debugf("local id:%s remote id:%s",
			localInfo.ID(), remoteInfo.ID())

		var netMSGs []*netpb.NetMsg
		if localInfo.ID() == "" || remoteInfo.ID() == "" {
			return nil, errors.New("error Node")
		}

		local, ok := localInfo.Statuses()[TypeLocalTBFTState].(*ConsensusTBFTImpl)
		if !ok {
			tsb.log.Warnf("not *ConsensusTBFTImpl type")
		}
		remoter, ok := remoteInfo.Statuses()[TypeRemoteTBFTState].(*RemoteState)
		if !ok {
			tsb.log.Warnf("not *RemoteState type")
		}

		//发送状态state
		sendState(local, remoteInfo, remoter, &netMSGs)

		if local != nil {
			local.metricsMutex.Lock()
			if localconf.ChainMakerConfig.MonitorConfig.Enabled {
				local.metricsHeight[remoteInfo.ID()].WithLabelValues(
					local.chainID, local.Id, remoteInfo.ID()).Set(float64(remoter.Height))
				local.metricsRound[remoteInfo.ID()].WithLabelValues(
					local.chainID, local.Id, remoteInfo.ID()).Set(float64(remoter.Round))
			}
			local.metricsMutex.Unlock()
		}

		if local == nil || remoter == nil || remoter.Height == 0 || local.Height < remoter.Height {
			return netMSGs, nil
		} else if local.Height == remoter.Height {
			local.RLock()
			defer local.RUnlock()
			//发送提案
			sendProposalOfRound(local, remoteInfo, remoter, &netMSGs)

			//发送prevote
			sendPrevoteOfRound(local, remoteInfo, remoter, &netMSGs)

			//发送Precommit
			sendPrecommitOfRound(local, remoteInfo, remoter, &netMSGs)

			//发送RoundQc
			sendRoundQC(local, remoteInfo, remoter, &netMSGs)

		} else {
			local.RLock()
			defer local.RUnlock()
			//远端节点落后很多，取本地缓存的历史高度状态，发送给远端节点
			state := local.consensusStateCache.getConsensusState(remoter.Height)
			if state == nil {
				tsb.log.Debugf("[%s] no caching consensusState, height:%d", local.Id, remoter.Height)
			} else {
				//发送Proposal
				sendProposalInState(state, remoter, &netMSGs)

				//发送Prevote
				sendPrevoteInState(local, state, remoter, &netMSGs)

				//发送Precommit
				sendPrecommitInState(local, state, remoter, &netMSGs)
			}
		}
		return netMSGs, nil
	}
}

// StatusInterceptor 状态拦截器
type StatusInterceptor struct {
}

// Handle 过滤状态类型处理
func (tsb *StatusInterceptor) Handle(status consistent_service.Status) error {
	if status.Type() != TypeLocalTBFTState &&
		status.Type() != TypeRemoteTBFTState {
		return errors.New("error status type")
	}

	return nil
}

//将本地节点（local）最新状态（state）发送给远端节点（remote）
func sendState(local *ConsensusTBFTImpl, remoteInfo consistent_service.Node,
	remoter *RemoteState, netMSGs *[]*netpb.NetMsg) {
	local.RLock()
	defer local.RUnlock()

	var proposal []byte
	if local.Proposal != nil {
		proposal = local.Proposal.PbMsg.Block.Header.BlockHash
	}

	var verifingProposal []byte
	if local.Proposal != nil {
		verifingProposal = local.Proposal.PbMsg.Block.Header.BlockHash
	}

	gossipProto := &tbftpb.GossipState{
		Id:               local.Id,
		Height:           local.Height,
		Round:            local.Round,
		Step:             local.Step,
		Proposal:         proposal,
		VerifingProposal: verifingProposal,
	}
	if local.heightRoundVoteSet.getRoundVoteSet(local.Round) != nil {
		gossipProto.RoundVoteSet = local.heightRoundVoteSet.getRoundVoteSet(local.Round).ToProto()
	}
	msg := proto.Clone(gossipProto)

	tbftMsg := &tbftpb.TBFTMsg{
		Type: tbftpb.TBFTMsgType_MSG_STATE,
		Msg:  mustMarshal(msg),
	}

	netMsg := &netpb.NetMsg{
		Payload: mustMarshal(tbftMsg),
		Type:    netpb.NetMsg_CONSISTENT_MSG,
		To:      remoteInfo.ID(),
	}
	*netMSGs = append(*netMSGs, netMsg)
	local.logger.Infof("[%s](%d/%d/%s) send state to [%s](%d/%d/%s)", local.Id, local.Height, local.Round, local.Step,
		remoteInfo.ID(), remoter.Height, remoter.Round, remoter.Step)
}

//将本地节点（local）最新的提案（proposal）发送给远端节点（remote）
func sendProposalOfRound(local *ConsensusTBFTImpl, remoteInfo consistent_service.Node,
	remoter *RemoteState, netMSGs *[]*netpb.NetMsg) {
	if local.Proposal != nil && local.Proposal.Bytes != nil && local.Proposal.PbMsg != nil {
		msg := createProposalTBFTMsg(local.Proposal)
		netMsg := &netpb.NetMsg{
			Payload: mustMarshal(msg),
			Type:    netpb.NetMsg_CONSISTENT_MSG,
			To:      remoteInfo.ID(),
		}
		*netMSGs = append(*netMSGs, netMsg)
		local.logger.Infof("[%s](%d/%d/%s) send proposal [%d/%d/%x] to [%s](%d/%d/%s)", local.Id, local.Height, local.Round,
			local.Step, local.Proposal.PbMsg.Height, local.Proposal.PbMsg.Round, local.Proposal.PbMsg.Block.Header.BlockHash,
			remoteInfo.ID(), remoter.Height, remoter.Round, remoter.Step)
	}
}

//将本地节点（local）最新的预投票（prevote）发送给远端节点（remote）
func sendPrevoteOfRound(local *ConsensusTBFTImpl,
	remoteInfo consistent_service.Node,
	remoter *RemoteState, netMSGs *[]*netpb.NetMsg) {
	remoter.RLock()
	defer remoter.RUnlock()
	prevoteVs := local.heightRoundVoteSet.prevotes(remoter.Round)
	if prevoteVs != nil {
		vote, ok := prevoteVs.Votes[local.Id]
		if ok && remoter.RoundVoteSet != nil && remoter.RoundVoteSet.Prevotes != nil {

			if _, pOk := remoter.RoundVoteSet.Prevotes.Votes[local.Id]; !pOk {
				msg := createPrevoteTBFTMsg(vote)
				netMsg := &netpb.NetMsg{
					Payload: mustMarshal(msg),
					Type:    netpb.NetMsg_CONSISTENT_MSG,
					To:      remoteInfo.ID(),
				}
				local.logger.Infof("[%s](%d/%d/%s) send prevote [%d/%d/%x] to [%s](%d/%d/%s)", local.Id, local.Height, local.Round,
					local.Step, vote.Height, vote.Round, vote.Hash, remoteInfo.ID(), remoter.Height, remoter.Round, remoter.Step)
				*netMSGs = append(*netMSGs, netMsg)
			}
		}
	}
}

//将本地节点（local）最新的预提交（precommit）发送给远端节点（remote）
func sendPrecommitOfRound(local *ConsensusTBFTImpl,
	remoteInfo consistent_service.Node,
	remoter *RemoteState, netMSGs *[]*netpb.NetMsg) {
	remoter.RLock()
	defer remoter.RUnlock()
	precommitVs := local.heightRoundVoteSet.precommits(remoter.Round)
	if precommitVs != nil {
		vote, ok := precommitVs.Votes[local.Id]
		if ok && remoter.RoundVoteSet != nil && remoter.RoundVoteSet.Precommits != nil {

			if _, pOk := remoter.RoundVoteSet.Precommits.Votes[local.Id]; !pOk {
				msg := createPrecommitTBFTMsg(vote)
				netMsg := &netpb.NetMsg{
					Payload: mustMarshal(msg),
					Type:    netpb.NetMsg_CONSISTENT_MSG,
					To:      remoteInfo.ID(),
				}
				local.logger.Infof("[%s](%d/%d/%s) send precommit [%d/%d/%x] to [%s](%d/%d/%s)", local.Id, local.Height,
					local.Round, local.Step, vote.Height, vote.Round, vote.Hash, remoteInfo.ID(), remoter.Height,
					remoter.Round, remoter.Step)
				*netMSGs = append(*netMSGs, netMsg)
			}
		}
	}
}

//将本地节点（local）历史高度（Height）的状态（state）发送给远端节点（remote）
func sendProposalInState(state *ConsensusState, remoter *RemoteState, netMSGs *[]*netpb.NetMsg) {
	// only the proposer has the serialized proposal
	if state.Proposal != nil && state.Proposal.Bytes != nil && state.Proposal.PbMsg != nil {
		msg := createProposalTBFTMsg(state.Proposal)
		netMsg := &netpb.NetMsg{
			Payload: mustMarshal(msg),
			Type:    netpb.NetMsg_CONSISTENT_MSG,
			To:      remoter.Id,
		}
		state.logger.Infof("[%s](%d/%d/%s) send proposal [%d/%d/%x] to [%s](%d/%d/%s)", state.Id, state.Height, state.Round,
			state.Step, state.Proposal.PbMsg.Height, state.Proposal.PbMsg.Round, state.Proposal.PbMsg.Block.Header.BlockHash,
			remoter.Id, remoter.Height, remoter.Round, remoter.Step)
		*netMSGs = append(*netMSGs, netMsg)
	}
}

//将本地节点（local）历史高度（Height）的预投票（prevote）发送给远端节点（remote）
func sendPrevoteInState(local *ConsensusTBFTImpl, state *ConsensusState,
	remoter *RemoteState, netMSGs *[]*netpb.NetMsg) {
	remoter.RLock()
	defer remoter.RUnlock()
	prevoteVs := state.heightRoundVoteSet.prevotes(remoter.Round)
	if prevoteVs != nil {
		vote, ok := prevoteVs.Votes[local.Id]
		roundVoteSet := remoter.RoundVoteSet
		if ok && roundVoteSet != nil && roundVoteSet.Prevotes != nil {
			if _, pOk := roundVoteSet.Prevotes.Votes[local.Id]; !pOk {
				msg := createPrevoteTBFTMsg(vote)
				netMsg := &netpb.NetMsg{
					Payload: mustMarshal(msg),
					Type:    netpb.NetMsg_CONSISTENT_MSG,
					To:      remoter.Id,
				}
				local.logger.Infof("[%s](%d/%d/%s) send prevote[%d/%d/%x] to [%s](%d/%d/%s)", local.Id, state.Height, state.Round,
					state.Step, vote.Height, vote.Round, vote.Hash, remoter.Id, remoter.Height, remoter.Round, remoter.Step)
				*netMSGs = append(*netMSGs, netMsg)
			}
		}
	}
}

//将本地节点（local）历史高度（Height）的预提案（precommit）发送给远端节点（remote）
func sendPrecommitInState(local *ConsensusTBFTImpl, state *ConsensusState,
	remoter *RemoteState, netMSGs *[]*netpb.NetMsg) {
	remoter.RLock()
	defer remoter.RUnlock()
	precommitVs := state.heightRoundVoteSet.precommits(remoter.Round)
	if precommitVs != nil {
		vote, ok := precommitVs.Votes[local.Id]
		roundVoteSet := remoter.RoundVoteSet
		if ok && roundVoteSet != nil && roundVoteSet.Precommits != nil {
			if _, pOk := roundVoteSet.Precommits.Votes[local.Id]; !pOk {
				msg := createPrecommitTBFTMsg(vote)
				netMsg := &netpb.NetMsg{
					Payload: mustMarshal(msg),
					Type:    netpb.NetMsg_CONSISTENT_MSG,
					To:      remoter.Id,
				}
				local.logger.Infof("[%s](%d/%d/%s) send precommit[%d/%d/%x] to [%s](%d/%d/%s)", local.Id, state.Height, state.Round,
					state.Step, vote.Height, vote.Round, vote.Hash, remoter.Id, remoter.Height, remoter.Round, remoter.Step)
				*netMSGs = append(*netMSGs, netMsg)
			}
		}
	}
}

// 发送QC，用于Round快速同步
// 一段时间没有tx，各节点Height不变，Round持续增加，
// 当Round落后当节点启动时，能快速同步到相同Round。
func sendRoundQC(local *ConsensusTBFTImpl,
	remoteInfo consistent_service.Node,
	remoter *RemoteState, netMSGs *[]*netpb.NetMsg) {
	remoter.RLock()
	defer remoter.RUnlock()
	//高度相同，Round落后超过1时，使用快速同步
	if local.Height == remoter.Height && local.Round > remoter.Round {
		var voteSet *VoteSet
		for round := local.Round; round >= remoter.Round; round-- {
			roundVoteSet := local.heightRoundVoteSet.getRoundVoteSet(round)
			if roundVoteSet == nil {
				continue
			}

			// get precommit QC
			if roundVoteSet.Precommits != nil {
				_, ok := roundVoteSet.Precommits.twoThirdsMajority()
				// we need a QC
				if ok {
					voteSet = roundVoteSet.Precommits
					break
				}
			}

			// get prevote QC
			if roundVoteSet.Prevotes != nil {
				_, ok := roundVoteSet.Prevotes.twoThirdsMajority()
				// we need a QC
				if ok {
					voteSet = roundVoteSet.Prevotes
					break
				}
			}
		}

		if voteSet != nil {
			roundQC := &tbftpb.RoundQC{
				Id:     local.Id,
				Height: voteSet.Height,
				Round:  voteSet.Round,
				Qc:     voteSet.ToProto(),
			}

			tbftMsg := &tbftpb.TBFTMsg{
				Type: tbftpb.TBFTMsgType_MSG_SEND_ROUND_QC,
				Msg:  mustMarshal(roundQC),
			}
			netMsg := &netpb.NetMsg{
				Payload: mustMarshal(tbftMsg),
				Type:    netpb.NetMsg_CONSISTENT_MSG,
				To:      remoteInfo.ID(),
			}

			local.logger.Infof("[%s](%d/%d/%s) send roundqc [%d/%d/%s] to [%s](%d/%d/%s)", local.Id, local.Height,
				local.Round, local.Step, voteSet.Height, voteSet.Round, voteSet.Type, remoteInfo.ID(), remoter.Height,
				remoter.Round, remoter.Step)
			*netMSGs = append(*netMSGs, netMsg)
		}
	}
}

// TbftConsistentMessage 实现一致性引擎消息的接收和发送
//
type TbftConsistentMessage struct {
	log      consistent_service.Logger
	msgBus   msgbus.MessageBus
	messageC chan interface{}
}

// OnMessage 基于msgbus的实现
func (m *TbftConsistentMessage) OnMessage(message *msgbus.Message) {
	m.log.Debugf("receive msg(topic:%d msgbus.ReceiveConsistentMsg:%d)",
		message.Topic, msgbus.RecvConsistentMsg)
	switch message.Topic {
	case msgbus.RecvConsistentMsg:
		m.messageC <- message
	}
}

// OnQuit 基于msgbus的实现
func (m *TbftConsistentMessage) OnQuit() {
	close(m.messageC)
}

// NewTbftConsistentMessage 初始化得到一致性引擎消息处理实例
func NewTbftConsistentMessage(msgBus msgbus.MessageBus, log consistent_service.Logger) *TbftConsistentMessage {
	tcm := &TbftConsistentMessage{
		msgBus:   msgBus,
		messageC: make(chan interface{}, MessageBufferSize),
		log:      log,
	}
	return tcm
}

// Send 发送一致性引擎的消息
func (m *TbftConsistentMessage) Send(payload interface{}) {
	m.log.Debugf("send topic:%d", msgbus.SendConsensusMsg)
	m.msgBus.Publish(msgbus.SendConsistentMsg, payload)
}

// Receive 接收一致性引擎的消息
func (m *TbftConsistentMessage) Receive() interface{} {
	m.log.Debugf("receive len(m.messageC)=%d", len(m.messageC))

	return <-m.messageC
}

// Start 启动Message
func (m *TbftConsistentMessage) Start() error {
	m.log.Debugf("start, Register:%d", msgbus.RecvConsistentMsg)
	m.msgBus.Register(msgbus.RecvConsistentMsg, m)
	return nil
}

// Stop 停止Message
func (m *TbftConsistentMessage) Stop() error {
	m.log.Debugf("stop, UnRegister:%d", msgbus.RecvConsistentMsg)
	m.msgBus.UnRegister(msgbus.RecvConsistentMsg, m)
	return nil
}
