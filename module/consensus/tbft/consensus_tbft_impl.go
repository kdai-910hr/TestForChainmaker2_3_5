/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tbft

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"chainmaker.org/chainmaker/chainconf/v2"
	"chainmaker.org/chainmaker/common/v2/msgbus"
	consensusUtils "chainmaker.org/chainmaker/consensus-utils/v2"
	"chainmaker.org/chainmaker/consensus-utils/v2/consistent_service"
	"chainmaker.org/chainmaker/consensus-utils/v2/wal_service"
	"chainmaker.org/chainmaker/localconf/v2"
	"chainmaker.org/chainmaker/lws"
	"chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/config"
	consensuspb "chainmaker.org/chainmaker/pb-go/v2/consensus"
	tbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/tbft"
	netpb "chainmaker.org/chainmaker/pb-go/v2/net"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/utils/v2"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var clog = zap.S()

var (
	defaultChanCap = 1000
	nilHash        = []byte("NilHash")
	//consensusStateKey              = []byte("ConsensusStateKey")
	defaultConsensusStateCacheSize  = uint64(10)
	defaultConsensusFutureCacheSize = uint64(10)
	// TBFTAddtionalDataKey implements the block key for store tbft infos
	TBFTAddtionalDataKey = "TBFTAddtionalDataKey"
	// TBFT_propose_timeout_key implements the config key for chainconf
	TBFT_propose_timeout_key = "TBFT_propose_timeout"
	// TBFT_propose_delta_timeout_key implements the config key for chainconf
	TBFT_propose_delta_timeout_key = "TBFT_propose_delta_timeout"
	// TBFT_blocks_per_proposer implements the config key for chainconf
	TBFT_blocks_per_proposer = "TBFT_blocks_per_proposer"
	// TBFT_propose_timeout_optimal_key implements the config key for chainconf
	TBFT_propose_timeout_optimal_key = "TBFT_propose_timeout_optimal"
	// TBFT_propose_optimal_key implements the config key for chainconf
	TBFT_propose_optimal_key = "TBFT_propose_optimal"
	// blockVersion231 the blockchain v2.3.1 version
	blockVersion231 = uint32(2030100)
)

const (
	// DefaultTimeoutPropose Timeout of waitting for a proposal before prevoting nil
	DefaultTimeoutPropose = 30 * time.Second
	// DefaultTimeoutProposeDelta Increased time delta of TimeoutPropose between rounds
	DefaultTimeoutProposeDelta = 1 * time.Second
	// DefaultBlocksPerProposer The number of blocks each proposer can propose
	DefaultBlocksPerProposer = uint64(1)
	// DefaultTimeoutProposeOptimal optimal timeout of waitting for a proposal before prevoting nil
	DefaultTimeoutProposeOptimal = 2 * time.Second
	// TimeoutPrevote Timeout of waiting for >2/3 prevote
	TimeoutPrevote = 30 * time.Second
	// TimeoutPrevoteDelta Increased time delta of TimeoutPrevote between round
	TimeoutPrevoteDelta = 1 * time.Second
	// TimeoutPrecommit Timeout of waiting for >2/3 precommit
	TimeoutPrecommit = 30 * time.Second
	// TimeoutPrecommitDelta Increased time delta of TimeoutPrecommit between round
	TimeoutPrecommitDelta = 1 * time.Second
	// TimeoutCommit Timeout to wait for precommite
	TimeoutCommit = 30 * time.Second
	// TimeDisconnet the duration of node disconnectio(3000ms)
	TimeDisconnet = 3000

	// GaugeTypeHeight is the height type for the metrics of prometheus
	GaugeTypeHeight = 0
	// GaugeTypeRound is the round type for the metrics of prometheus
	GaugeTypeRound = 1

	// ChainId is metrics label of chainId
	ChainId = "chainId"
	// LocalId is metrics label of localId
	LocalId = "localId"
	// NodeId is metrics label of nodeId
	NodeId = "nodeId"
)

var msgBusTopics = []msgbus.Topic{msgbus.ProposedBlock, msgbus.VerifyResult,
	msgbus.RecvConsensusMsg, msgbus.RecvConsistentMsg, msgbus.BlockInfo}

// mustMarshal marshals protobuf message to byte slice or panic when marshal twice both failed
func mustMarshal(msg proto.Message) (data []byte) {
	var err error
	defer func() {
		// while first marshal failed, retry marshal again
		if recover() != nil {
			data, err = proto.Marshal(msg)
			if err != nil {
				panic(err)
			}
		}
	}()

	data, err = proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return
}

// mustUnmarshal unmarshals from byte slice to protobuf message or panic
func mustUnmarshal(b []byte, msg proto.Message) {
	if err := proto.Unmarshal(b, msg); err != nil {
		panic(err)
	}
}

func mustMarshalRecover(msg proto.Message) (data []byte) {
	var err error
	defer func() {
		if recover() != nil {
			if err != nil {
				return
			}
		}
	}()

	data, err = proto.Marshal(msg)
	if err != nil {
		return nil
	}
	return
}

func mustMarshalWal(msg proto.Message) (data []byte) {
	var res []byte
	for {
		if res = mustMarshalRecover(msg); res != nil {
			break
		}
	}
	return res
}

// ConsensusTBFTImpl is the implementation of TBFT algorithm
// and it implements the ConsensusEngine interface.
type ConsensusTBFTImpl struct {
	sync.RWMutex
	ctx    context.Context
	logger protocol.Logger
	// chain id
	chainID string
	// node id
	Id string
	// Currently nil, not used
	extendHandler protocol.ConsensusExtendHandler
	// signer（node）
	signer protocol.SigningMember
	// sync service
	syncService protocol.SyncService
	// Access Control
	ac protocol.AccessControlProvider
	// Cache the latest block in ledger(wal)
	ledgerCache protocol.LedgerCache
	// The Proposer of the last block commit height
	lastHeightProposer string
	// block version
	blockVersion uint32
	// chain conf
	chainConf protocol.ChainConf
	// net service （Need to use network module method GetNodeUidByCertId）
	netService protocol.NetService
	// send/receive a message using msgbus
	msgbus msgbus.MessageBus
	// stop tbft
	closeC chan struct{}
	// wal is used to record the consensus state and prevent forks
	wal *lws.Lws
	// write wal sync: 0
	walWriteMode wal_service.WalWriteMode
	// validator Set
	validatorSet *validatorSet
	// Current Consensus State
	*ConsensusState
	// History Consensus State（Save 10）
	// When adding n, delete the cache before n-10
	consensusStateCache *consensusStateCache
	// Cache future consensus msg
	// When update height of consensus, delete the cache before height (triggered every 10 heights)
	consensusFutureMsgCache *ConsensusFutureMsgCache
	// timeScheduler is used by consensus for shecdule timeout events.
	// Outdated timeouts will be ignored in processing.
	timeScheduler *timeScheduler

	// channel for processing a block
	proposedBlockC chan *proposedProposal
	// channel used to verify the results
	verifyResultC chan *consensuspb.VerifyResult
	// channel used to enter new height
	blockHeightC chan uint64
	// channel used to externalMsg（msgbus）
	externalMsgC chan *ConsensusMsg
	// Use in handleConsensusMsg method
	internalMsgC chan *ConsensusMsg

	invalidTxs []string

	// Timeout = TimeoutPropose + TimeoutProposeDelta * round
	TimeoutPropose        time.Duration
	TimeoutProposeDelta   time.Duration
	TimeoutProposeOptimal time.Duration
	ProposeOptimal        bool
	ProposeOptimalTimer   *time.Timer

	// The specific time points of each stage of each round in each altitude
	// for printing logs
	metrics *heightMetrics
	// Tbft Consistent Engine
	// will only work if the node is abnormal
	// prevent message loss
	consistentEngine consistent_service.ConsistentEngine
	// use block verifier from core module
	blockVerifier protocol.BlockVerifier

	state int32

	metricsMutex  sync.Mutex
	metricsHeight map[string]*prometheus.GaugeVec
	metricsRound  map[string]*prometheus.GaugeVec
}

// Type return Status Type(ConsistentEngine)
// @receiver consensus
// @return int
func (consensus *ConsensusTBFTImpl) Type() int8 {
	return TypeLocalTBFTState
}

// Data return Status Data(ConsistentEngine)
// @receiver consensus
// @return interface{}
func (consensus *ConsensusTBFTImpl) Data() interface{} {
	return consensus
}

// Update  update state
// @receiver consensus
// @param state
func (consensus *ConsensusTBFTImpl) Update(state consistent_service.Status) {
	consensus.logger.Debugf("update %s", state.Type())
}

const (
	start = 1
	stop  = 0
)

// New creates a tbft consensus instance
func New(config *consensusUtils.ConsensusImplConfig) (*ConsensusTBFTImpl, error) {
	var err error
	consensus := &ConsensusTBFTImpl{}
	consensus.logger = config.Logger
	consensus.logger.Infof("New ConsensusTBFTImpl[%s]", config.NodeId)
	consensus.chainID = config.ChainId
	consensus.Id = config.NodeId
	consensus.signer = config.Signer
	consensus.ac = config.Ac
	consensus.syncService = config.Sync
	consensus.ledgerCache = config.LedgerCache
	consensus.chainConf = config.ChainConf
	consensus.netService = config.NetService
	consensus.msgbus = config.MsgBus
	consensus.closeC = make(chan struct{})
	// init the wal service
	consensus.wal, consensus.walWriteMode, err = InitLWS(config.ChainConf.ChainConfig().Consensus,
		consensus.chainID, consensus.Id)
	if err != nil {
		return nil, err
	}

	consensus.proposedBlockC = make(chan *proposedProposal, defaultChanCap)
	consensus.verifyResultC = make(chan *consensuspb.VerifyResult, defaultChanCap)
	consensus.blockHeightC = make(chan uint64, defaultChanCap)
	consensus.externalMsgC = make(chan *ConsensusMsg, defaultChanCap)
	consensus.internalMsgC = make(chan *ConsensusMsg, defaultChanCap)
	consensus.blockVerifier = config.Core.GetBlockVerifier()

	validators, err := GetValidatorListFromConfig(consensus.chainConf.ChainConfig())
	if err != nil {
		return nil, err
	}
	consensus.validatorSet = newValidatorSet(consensus.logger, validators, DefaultBlocksPerProposer)
	consensus.ConsensusState = NewConsensusState(consensus.logger, consensus.Id)
	consensus.consensusStateCache = newConsensusStateCache(defaultConsensusStateCacheSize)
	consensus.consensusFutureMsgCache = newConsensusFutureMsgCache(consensus.logger,
		defaultConsensusFutureCacheSize, 0)
	consensus.timeScheduler = newTimeSheduler(consensus.logger, config.NodeId)
	consensus.ProposeOptimal = false
	consensus.ProposeOptimalTimer = time.NewTimer(1 * time.Second)
	consensus.ProposeOptimalTimer.Stop()

	// get broadcaster interval from local config
	broadcasterInterval := localconf.ChainMakerConfig.ConsensusConfig.TbftConfig.BroadcasterInterval
	if broadcasterInterval != 0 {
		TimerInterval = broadcasterInterval * time.Millisecond
	}
	consensus.state = stop
	consensus.metricsHeight = make(map[string]*prometheus.GaugeVec)
	consensus.metricsRound = make(map[string]*prometheus.GaugeVec)
	for _, validator := range consensus.validatorSet.Validators {
		consensus.NewGaugeVec(validator, GaugeTypeHeight)
		consensus.NewGaugeVec(validator, GaugeTypeRound)
	}
	return consensus, nil
}

// InitConsistentEngine init ConsistentEngine
// @receiver consensus
func (consensus *ConsensusTBFTImpl) InitConsistentEngine() {

	status := make(map[int8]consistent_service.Status)
	status[consensus.Type()] = consensus
	localStatus := &Node{
		id:     consensus.Id,
		status: status,
	}
	message := NewTbftConsistentMessage(consensus.msgbus, consensus.logger)
	// create consistentEngine
	consensus.consistentEngine = consistent_service.NewConsistentService(
		localStatus,
		message,
		consensus.logger)

	tbftStatusInterceptor := &StatusInterceptor{}
	// register a tbftStatusInterceptor
	err := consensus.consistentEngine.RegisterStatusInterceptor(InterceptorTbft, tbftStatusInterceptor)
	if err != nil {
		consensus.logger.Errorf(err.Error())
	}
	tbftDecoder := &StatusDecoder{log: consensus.logger,
		tbftImpl: consensus}
	// register a tbftDecoder
	err = consensus.consistentEngine.RegisterStatusCoder(tbftDecoder.MsgType(), tbftDecoder)
	if err != nil {
		consensus.logger.Errorf("RegisterStatusCoder err:%s", err)
		return
	}
	// add a remoter
	for _, id := range consensus.validatorSet.Validators {
		// local node do not need to be added to the consistency engine
		if id == consensus.Id {
			continue
		}
		remoteState := &RemoteState{Id: id, Height: 0}
		status := make(map[int8]consistent_service.Status)
		status[remoteState.Type()] = remoteState
		remoteNodeStatus := &Node{id: id, status: status}
		err = consensus.consistentEngine.PutRemoter(id, remoteNodeStatus)
		if err != nil {
			consensus.logger.Errorf(err.Error())
		}
	}
	// create a tbftStatusBroadcaster
	tbftStatusBroadcaster := NewTBFTStatusBroadcaster(
		consensus.logger)
	// add a tbftStatusBroadcaster to the consistentEngine
	err = consensus.consistentEngine.AddBroadcaster(StatusBroadcasterTbft, tbftStatusBroadcaster)
	if err != nil {
		consensus.logger.Errorf("AddBroadcaster err:%s", err)
	}
}

// StartConsistentEngine start consistent engine
// @Description: Start ConsistentEngine
// @receiver consensus
func (consensus *ConsensusTBFTImpl) StartConsistentEngine() error {
	// start the consistentEngine
	consensus.ctx = context.Background()
	err := consensus.consistentEngine.Start(consensus.ctx)
	if err != nil {
		consensus.logger.Errorf(err.Error())
	}
	return err
}

// Start starts the tbft instance with:
// 1. Register to message bus for subscribing topics
// 2. Start background goroutinues for processing events
// 3. Start timeScheduler for processing timeout shedule
// the consensus module monitors the signal of the sync module,
// when the sync module synchronizes a relatively high block,
// it notifies the consensus module to start the consensus process
func (consensus *ConsensusTBFTImpl) Start() error {
	go func() {
		if consensus.syncService != nil &&
			!(consensus.validatorSet.Size() == 1 && consensus.validatorSet.HasValidator(consensus.Id)) {
			<-consensus.syncService.ListenSyncToIdealHeight()
		}
		for _, topic := range msgBusTopics {
			consensus.msgbus.Register(topic, consensus)
		}
		_ = chainconf.RegisterVerifier(consensus.chainID, consensuspb.ConsensusType_TBFT, consensus)

		consensus.logger.Infof("start ConsensusTBFTImpl[%s]", consensus.Id)
		consensus.timeScheduler.Start()
		consensus.InitConsistentEngine()

		err := consensus.replayWal()
		if err != nil {
			consensus.logger.Warnf("replayWal failed, err = %v", err)
			return
		}
		err = consensus.StartConsistentEngine()
		if err != nil {
			return
		}

		//consensus.gossip.start()
		go consensus.handle()
	}()
	atomic.StoreInt32(&consensus.state, start)
	return nil
}

// sendProposeState
// @Description: Send propose state via msgbus
// @receiver consensus
// @param isProposer Is it a proposal node
func (consensus *ConsensusTBFTImpl) sendProposeState(isProposer bool) {
	consensus.logger.Infof("[%s](%d/%d/%s) sendProposeState isProposer: %v",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step, isProposer)
	consensus.msgbus.PublishSafe(msgbus.ProposeState, isProposer)
}

// Stop implements the Stop method of ConsensusEngine interface.
func (consensus *ConsensusTBFTImpl) Stop() error {
	consensus.Lock()
	defer consensus.Unlock()

	for _, topic := range msgBusTopics {
		consensus.msgbus.UnRegister(topic, consensus)
	}

	consensus.logger.Infof("[%s](%d/%d/%s) stopped", consensus.Id, consensus.Height, consensus.Round,
		consensus.Step)
	if consensus.consistentEngine != nil {
		err := consensus.consistentEngine.Stop(consensus.ctx)
		if err != nil {
			consensus.logger.Errorf(err.Error())
		}
	}
	consensus.wal.Close()
	close(consensus.closeC)
	atomic.StoreInt32(&consensus.state, stop)
	return nil
}

// InitExtendHandler r egistered extendHandler
func (consensus *ConsensusTBFTImpl) InitExtendHandler(handler protocol.ConsensusExtendHandler) {
	consensus.extendHandler = handler
}

// 1. when leadership transfer, change consensus state and send singal
// atomic.StoreInt32()
// proposable <- atomic.LoadInt32(consensus.isLeader)

// 2. when receive pre-prepare block, send block to verifyBlockC
// verifyBlockC <- block

// 3. when receive commit block, send block to commitBlockC
// commitBlockC <- block

// OnMessage implements the OnMessage method of msgbus.
func (consensus *ConsensusTBFTImpl) OnMessage(message *msgbus.Message) {
	consensus.logger.Debugf("[%s] OnMessage receive topic: %s", consensus.Id, message.Topic)

	switch message.Topic {
	case msgbus.ProposedBlock:
		if proposedBlock, ok := message.Payload.(*consensuspb.ProposalBlock); ok {
			consensus.proposedBlockC <- &proposedProposal{proposedBlock, nil}
			consensus.logger.Debugf("len of proposedBlockC: %d", len(consensus.proposedBlockC))
		} else {
			consensus.logger.Warnf("assert ProposalBlock failed, get type:{%s}", reflect.TypeOf(message.Payload))
		}
	case msgbus.VerifyResult:
		if verifyResult, ok := message.Payload.(*consensuspb.VerifyResult); ok {
			consensus.logger.Debugf("[%s] verify result: %s", consensus.Id, verifyResult.Code)
			consensus.verifyResultC <- verifyResult
		} else {
			consensus.logger.Warnf("assert VerifyResult failed, get type:{%s}", reflect.TypeOf(message.Payload))
		}
	case msgbus.RecvConsensusMsg:
		if msg, ok := message.Payload.(*netpb.NetMsg); ok {
			if consensusMsg := consensus.createConsensusMsgFromTBFTMsgBz(msg.Payload); consensusMsg != nil {
				consensus.externalMsgC <- consensusMsg
			} else {
				consensus.logger.Warnf("assert Consensus Msg failed")
			}
		} else {
			consensus.logger.Warnf("assert NetMsg failed, get type:{%s}", reflect.TypeOf(message.Payload))
		}
	case msgbus.RecvConsistentMsg:
		if msg, ok := message.Payload.(*netpb.NetMsg); ok {
			if consensusMsg := consensus.createConsensusMsgFromTBFTMsgBz(msg.Payload); consensusMsg != nil {
				consensus.externalMsgC <- consensusMsg
			} else {
				consensus.logger.Warnf("assert Consensus Msg failed")
			}
		} else {
			consensus.logger.Warnf("assert NetMsg failed, get type:{%s}", reflect.TypeOf(message.Payload))
		}
	case msgbus.BlockInfo:
		if blockInfo, ok := message.Payload.(*common.BlockInfo); ok {
			if blockInfo == nil || blockInfo.Block == nil {
				consensus.logger.Errorf("receive message failed, error message BlockInfo = nil")
				return
			}
			consensus.blockHeightC <- blockInfo.Block.Header.BlockHeight
		} else {
			consensus.logger.Warnf("assert BlockInfo failed, get type:{%s}", reflect.TypeOf(message.Payload))
		}
	default:
		panic("unhandled default case")
	}
}

// OnQuit implements the OnQuit method of msgbus.
func (consensus *ConsensusTBFTImpl) OnQuit() {
	consensus.logger.Infof("ConsensusTBFTImpl msgbus OnQuit")
	// do nothing
	//panic("implement me")
}

// Verify implements interface of struct Verifier,
// This interface is used to verify the validity of parameters,
// it executes before consensus.
func (consensus *ConsensusTBFTImpl) Verify(consensusType consensuspb.ConsensusType,
	chainConfig *config.ChainConfig) error {
	consensus.logger.Infof("[%s](%d/%d/%v) verify chain consensusConfig",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step)
	if consensusType != consensuspb.ConsensusType_TBFT {
		errMsg := fmt.Sprintf("consensus type is not TBFT: %s", consensusType)
		return errors.New(errMsg)
	}
	consensusConfig := chainConfig.Consensus
	_, _, _, _, _, _, err := consensus.extractConsensusConfig(consensusConfig)
	return err
}

// updateChainConfig
// @Description: Update configuration, return validators to be added and removed
// @receiver consensus
// @return addedValidators
// @return removedValidators
// @return err
func (consensus *ConsensusTBFTImpl) updateChainConfig() (addedValidators []string, removedValidators []string,
	err error) {
	consensus.logger.Debugf("[%s](%d/%d/%v) update chain consensusConfig",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step)

	consensusConfig := consensus.chainConf.ChainConfig().Consensus
	validators, timeoutPropose, timeoutProposeDelta, tbftBlocksPerProposer, proposeOptimal,
		timeoutProposeOptimal, err := consensus.extractConsensusConfig(consensusConfig)
	if err != nil {
		return nil, nil, err
	}

	consensus.ProposeOptimal = proposeOptimal
	consensus.TimeoutProposeOptimal = timeoutProposeOptimal
	consensus.TimeoutPropose = timeoutPropose
	consensus.TimeoutProposeDelta = timeoutProposeDelta
	consensus.logger.Debugf("[%s](%d/%d/%v) update chain consensusConfig, consensusConfig: %v,"+
		" TimeoutPropose: %v, TimeoutProposeDelta: %v, ProposeOptimal:%v, TimeoutProposeOptimal:%v, validators: %v",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step, consensusConfig,
		consensus.TimeoutPropose, consensus.TimeoutProposeDelta, consensus.TimeoutProposeOptimal, consensus.ProposeOptimal,
		validators)

	if consensus.extendHandler != nil {
		consensus.logger.Debugf("enter extend handle to get proposers ...")
		if validators, err = consensus.extendHandler.GetValidators(); err != nil {
			return nil, nil, err
		}
	} else {
		consensus.logger.Debugf("enter default tbft to get proposers ...")
		err := consensus.validatorSet.updateBlocksPerProposer(tbftBlocksPerProposer)
		if err != nil {
			consensus.logger.Errorf("update Proposer per Blocks failed err: %s", err)
		}
	}
	return consensus.validatorSet.updateValidators(validators)
}

// extractConsensusConfig
// @Description: return config from extractConsensusConfig
// @receiver consensus
// @param config
// @return validators
// @return timeoutPropose
// @return timeoutProposeDelta
// @return tbftBlocksPerProposer
// @return err
func (consensus *ConsensusTBFTImpl) extractConsensusConfig(config *config.ConsensusConfig) (validators []string,
	timeoutPropose time.Duration, timeoutProposeDelta time.Duration, tbftBlocksPerProposer uint64,
	proposeOptimal bool, timeoutProposeOptimal time.Duration, err error) {
	timeoutPropose = DefaultTimeoutPropose
	timeoutProposeDelta = DefaultTimeoutProposeDelta
	timeoutProposeOptimal = DefaultTimeoutProposeOptimal
	tbftBlocksPerProposer = uint64(1)
	proposeOptimal = false

	validators, err = GetValidatorListFromConfig(consensus.chainConf.ChainConfig())
	if err != nil {
		consensus.logger.Errorf("[%s](%d/%d/%v) get validator list from config failed: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, validators)
		return
	}

	for _, v := range config.ExtConfig {
		switch v.Key {
		case TBFT_propose_timeout_key:
			timeoutPropose, err = consensus.extractProposeTimeout(v.Value)
		case TBFT_propose_delta_timeout_key:
			timeoutProposeDelta, err = consensus.extractProposeTimeoutDelta(v.Value)
		case TBFT_blocks_per_proposer:
			tbftBlocksPerProposer, err = consensus.extractBlocksPerProposer(v.Value)
		case TBFT_propose_optimal_key:
			proposeOptimal, err = consensus.extractProposeOptimal(v.Value)
		case TBFT_propose_timeout_optimal_key:
			timeoutProposeOptimal, err = consensus.extractTimeoutProposeOptimal(v.Value)
		}

		if err != nil {
			return
		}
	}

	return
}

// extractProposeTimeout
// @Description: extract the timeout based on value
// @receiver consensus
// @param value
// @return timeoutPropose
// @return err
func (consensus *ConsensusTBFTImpl) extractProposeTimeout(value string) (timeoutPropose time.Duration, err error) {
	if timeoutPropose, err = time.ParseDuration(value); err != nil {
		consensus.logger.Infof("[%s](%d/%d/%v) update chain config, TimeoutPropose: %v,"+
			" TimeoutProposeDelta: %v,"+" parse TimeoutPropose error: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			consensus.TimeoutPropose, consensus.TimeoutProposeDelta, err)
	}
	return
}

// extractProposeTimeoutDelta
// @Description: extract the delta timeout based on value
// @receiver consensus
// @param value
// @return timeoutProposeDelta
// @return err
func (consensus *ConsensusTBFTImpl) extractProposeTimeoutDelta(value string) (timeoutProposeDelta time.Duration,
	err error) {
	if timeoutProposeDelta, err = time.ParseDuration(value); err != nil {
		consensus.logger.Infof("[%s](%d/%d/%v) update chain config, TimeoutPropose: %v,"+
			" TimeoutProposeDelta: %v,"+" parse TimeoutProposeDelta error: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			consensus.TimeoutPropose, consensus.TimeoutProposeDelta, err)
	}
	return
}

// extractBlocksPerProposer
// @Description: Calculate the number of extracted blocks for each proposer
// @receiver consensus
// @param value
// @return tbftBlocksPerProposer
// @return err
func (consensus *ConsensusTBFTImpl) extractBlocksPerProposer(value string) (tbftBlocksPerProposer uint64, err error) {
	if tbftBlocksPerProposer, err = strconv.ParseUint(value, 10, 32); err != nil {
		consensus.logger.Infof("[%s](%d/%d/%v) update chain config, parse BlocksPerProposer error: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, err)
		return
	}
	if tbftBlocksPerProposer <= 0 {
		err = fmt.Errorf("invalid TBFT_blocks_per_proposer: %d", tbftBlocksPerProposer)
		return
	}
	return
}

// extract the timeout optimal based on value
func (consensus *ConsensusTBFTImpl) extractTimeoutProposeOptimal(value string) (timeoutProposeOptimal time.Duration,
	err error) {
	if timeoutProposeOptimal, err = time.ParseDuration(value); err != nil {
		consensus.logger.Infof("[%s](%d/%d/%v) update chain config, timeoutProposeOptimal: %v,"+
			" parse timeoutProposeOptimal error: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			consensus.TimeoutProposeOptimal, err)
	}
	return
}

// extract the propose optimal based on value
func (consensus *ConsensusTBFTImpl) extractProposeOptimal(value string) (proposeOptimal bool, err error) {
	if proposeOptimal, err = strconv.ParseBool(value); err != nil {
		consensus.logger.Infof("[%s](%d/%d/%v) update chain config, parse ProposeOptimal error: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, err)
		return
	}
	return
}

// handle
// @Description: Main methods of consensus process
// @receiver consensus
func (consensus *ConsensusTBFTImpl) handle() {
	consensus.logger.Infof("[%s] handle start", consensus.Id)
	defer consensus.logger.Infof("[%s] handle end", consensus.Id)

	loop := true
	for loop {
		select {
		case proposedBlock := <-consensus.proposedBlockC:
			consensus.handleProposedBlock(proposedBlock)
		case result := <-consensus.verifyResultC:
			consensus.handleVerifyResult(result)
		case height := <-consensus.blockHeightC:
			consensus.handleBlockHeight(height)
		case msg := <-consensus.externalMsgC:
			consensus.logger.Debugf("[%s] receive from externalMsgC %s", consensus.Id, msg.Type)
			consensus.handleConsensusMsg(msg)
		case msg := <-consensus.internalMsgC:
			consensus.logger.Debugf("[%s] receive from internalMsgC %s", consensus.Id, msg.Type)
			consensus.handleConsensusMsg(msg)
		case ti := <-consensus.timeScheduler.GetTimeoutC():
			consensus.handleTimeout(ti)
		case <-consensus.ProposeOptimalTimer.C:
			consensus.handleProposeOptimalTimeout()
		case <-consensus.closeC:
			loop = false
		}
	}
}

// handleProposedBlock
// @Description: Process the proposed block
// @receiver consensus
// @param proposedBlock
func (consensus *ConsensusTBFTImpl) handleProposedBlock(proposedProposal *proposedProposal) {
	consensus.Lock()
	defer consensus.Unlock()

	receiveBlockTime := CurrentTime()
	// consensus not using the same block pointer with core, but internal content using the same pointer
	block := CopyBlock(proposedProposal.proposedBlock.Block)
	consensus.logger.Infof("[%s](%d/%d/%s) receive block from core engine (%d/%x), isProposer: %v",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step,
		block.Header.BlockHeight, block.Header.BlockHash, consensus.isProposer(consensus.Height,
			consensus.Round),
	)

	// process only blocks of current height
	if block.Header.BlockHeight != consensus.Height {
		consensus.logger.Warnf("[%s](%d/%d/%v) handle proposed block failed,"+
			" receive block from invalid height: %d",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, block.Header.BlockHeight)
		return
	}

	// only process blocks for which you are the proposer
	if !consensus.isProposer(consensus.Height, consensus.Round) {
		consensus.logger.Warnf("[%s](%d/%d/%s) receive proposal from core engine (%d/%x), but isProposer: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			block.Header.BlockHeight, block.Header.BlockHash, consensus.isProposer(consensus.Height, consensus.Round),
		)
		return
	}
	// continue processing only if you are in the proposal stage（Step_PROPOSE）
	if consensus.Step != tbftpb.Step_PROPOSE {
		consensus.logger.Warnf("[%s](%d/%d/%s) receive proposal from core engine (%d/%x), step error",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			block.Header.BlockHeight, block.Header.BlockHash,
		)
		return
	}

	// Tell the core engine that it is no longer a proposal node and does not propose blocks anymore
	consensus.sendProposeState(false)

	// Add extend handle consensus args into the block of consensus[DPoS], actually also add them to the block of core
	if consensus.extendHandler != nil {
		if err := consensus.extendHandler.CreateRWSet(block.Header.PreBlockHash, NewProposalBlock(block,
			proposedProposal.proposedBlock.TxsRwSet)); err != nil {
			consensus.logger.Errorf("[%s](%d/%d/%s) Create RWSets failed, reason: %s",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step, err)
			return
		}
	}

	// Add proposal
	proposal := NewProposal(consensus.Id, consensus.Height, consensus.Round, -1, block)
	// this is not a locked proposal
	if proposedProposal.qc == nil {
		// Add hash and signature to the block of consensus, actually also add them to the block of core
		hash, sig, err := utils.SignBlock(consensus.chainConf.ChainConfig().Crypto.Hash, consensus.signer, block)
		if err != nil {
			consensus.logger.Errorf("[%s]sign block failed, %s", consensus.Id, err)
			return
		}
		block.Header.BlockHash = hash[:]
		block.Header.Signature = sig
	} else {
		// the locked proposal need txrwset and qc to send other nodes
		proposal.TxsRwSet = proposedProposal.proposedBlock.TxsRwSet
		proposal.Qc = proposedProposal.qc
	}
	signBlockTime := CurrentTime()

	// proposal for cut block
	if proposedProposal.proposedBlock.CutBlock != nil {
		oriBlock := proposedProposal.proposedBlock.Block
		proposal.Block = proposedProposal.proposedBlock.CutBlock
		err := consensus.signProposal(proposal)
		if err != nil {
			consensus.logger.Errorf("sign proposal err %s", err)
			return
		}
		consensus.Proposal = NewTBFTProposal(proposal, true)
		proposal.Block = oriBlock
	} else {
		err := consensus.signProposal(proposal)
		if err != nil {
			consensus.logger.Errorf("sign proposal err %s", err)
			return
		}
		consensus.Proposal = NewTBFTProposal(proposal, true)
	}

	// need txsRWSet to saved in wal
	consensus.Proposal.PbMsg.TxsRwSet = proposedProposal.proposedBlock.TxsRwSet
	signProposalTime := CurrentTime()

	consensus.logger.Infof("[%s](%d/%d/%s) generated proposal (%d/%d/%x), "+
		"time:[signBlock:%dms,signProposal:%dms,total:%dms]",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step,
		proposal.Height, proposal.Round, proposal.Block.Header.BlockHash,
		signBlockTime.Sub(receiveBlockTime).Milliseconds(), signProposalTime.Sub(signBlockTime).Milliseconds(),
		signProposalTime.Sub(receiveBlockTime).Milliseconds())

	// Broadcast proposal
	consensus.sendConsensusProposal(consensus.Proposal, "")
	// Enter the prevote stage
	consensus.enterPrevote(consensus.Height, consensus.Round)
}

// handleVerifyResult
// @Description: Process validation results
// @receiver consensus
// @param verifyResult
func (consensus *ConsensusTBFTImpl) handleVerifyResult(verifyResult *consensuspb.VerifyResult) {
	consensus.Lock()
	defer consensus.Unlock()

	receiveResTime := CurrentTime()

	height := verifyResult.VerifiedBlock.Header.BlockHeight
	hash := verifyResult.VerifiedBlock.Header.BlockHash
	consensus.logger.Infof("[%s](%d/%d/%s) receive verify result (%d/%x) %v",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step,
		height, hash, verifyResult.Code)

	if consensus.VerifingProposal == nil {
		consensus.logger.Warnf("[%s](%d/%d/%s) receive verify result failed, (%d/%x) %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			height, hash, verifyResult.Code,
		)
		return
	}

	if consensus.Height == height &&
		consensus.Round == consensus.VerifingProposal.PbMsg.Round &&
		verifyResult.Code == consensuspb.VerifyResult_FAIL {
		consensus.logger.Warnf("[%s](%d/%d/%s) %x receive verify result (%d/%x) %v failed",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			consensus.VerifingProposal.PbMsg.Block.Header.BlockHash,
			height, hash, verifyResult.Code,
		)
		consensus.VerifingProposal = nil

		// inconsistent RwSet
		if verifyResult.RwSetVerifyFailTxs != nil && len(verifyResult.RwSetVerifyFailTxs.TxIds) > 0 {
			consensus.logger.Infof("handleVerifyResult failed , invalidTx = %v", verifyResult.RwSetVerifyFailTxs.TxIds)
			consensus.invalidTxs = verifyResult.RwSetVerifyFailTxs.TxIds
			// inconsistent RWSets enter prevote immediately
			consensus.enterPrevote(consensus.Height, consensus.Round)
		}
		return
	}

	// if there are quorum pre_commit vote, then commit block
	if bytes.Equal(consensus.VerifingProposal.PbMsg.Block.Header.BlockHash, hash) {
		if consensus.heightRoundVoteSet != nil && consensus.heightRoundVoteSet.precommits(consensus.Round) != nil {
			voteSet := consensus.heightRoundVoteSet.precommits(consensus.Round)
			quorumHash, ok := voteSet.twoThirdsMajority()
			//if ok && bytes.Compare(quorumHash, consensus.VerifingProposal.Block.Header.BlockHash) == 0 {
			if ok && bytes.Equal(quorumHash, consensus.VerifingProposal.PbMsg.Block.Header.BlockHash) {
				consensus.Proposal = consensus.VerifingProposal

				// Commit block to core engine
				consensus.commitBlock(consensus.Proposal.PbMsg.Block, voteSet.ToProto())
				return
			}
		}
	}

	// maybe received multiple proposal, the last proposal verify succeed
	// the previous invalid txs is no longer needed
	consensus.invalidTxs = nil

	if consensus.Step != tbftpb.Step_PROPOSE ||
		!bytes.Equal(consensus.VerifingProposal.PbMsg.Block.Header.BlockHash, hash) {
		consensus.logger.Warnf("[%s](%d/%d/%s) %x receive verify result (%d/%x) error",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			consensus.VerifingProposal.PbMsg.Block.Header.BlockHash,
			height, hash,
		)
		consensus.VerifingProposal = nil
		return
	}

	// [DPos]
	if consensus.extendHandler != nil {
		consensus.logger.Debugf("enter extend handle to verify consensus args ...")
		if err := consensus.extendHandler.VerifyConsensusArgs(verifyResult.VerifiedBlock, verifyResult.TxsRwSet); err != nil {
			consensus.logger.Warnf("verify block failed, reason: %s", err)
			return
		}
	}

	consensus.Proposal = consensus.VerifingProposal
	consensus.Proposal.PbMsg.Block = verifyResult.VerifiedBlock
	consensus.Proposal.PbMsg.TxsRwSet = verifyResult.TxsRwSet
	endTime := CurrentTime()

	consensus.logger.Infof("[%s](%d/%d/%s) processed verify result (%d/%x), time[verify:%dms]",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step, height, hash,
		endTime.Sub(receiveResTime).Milliseconds())

	// Prevote
	consensus.enterPrevote(consensus.Height, consensus.Round)
}

// handleBlockHeight
// @Description: Process block height messages and enter new heights
// @receiver consensus
// @param height
func (consensus *ConsensusTBFTImpl) handleBlockHeight(height uint64) {
	consensus.Lock()
	defer consensus.Unlock()

	consensus.logger.Infof("[%s](%d/%d/%s) receive block height %d",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step, height)

	// Outdated block height event
	if consensus.Height > height {
		return
	}

	consensus.logger.Infof("[%s](%d/%d/%s) enterNewHeight because receiving block height %d",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step, height)
	consensus.enterNewHeight(height + 1)
}

// procPropose
// @Description: Process proposal
// @receiver consensus
// @param proposal
func (consensus *ConsensusTBFTImpl) procPropose(proposal *tbftpb.Proposal) {
	receiveProposalTime := CurrentTime()
	if proposal == nil || proposal.Block == nil {
		consensus.logger.Warnf("[%s](%d/%d/%s) receive invalid proposal because nil proposal",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step)
		return
	}

	consensus.logger.Infof("[%s](%d/%d/%s) receive proposal from [%s], proposal(%d/%d/%x/%d)",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step,
		proposal.Voter,
		proposal.Height, proposal.Round, proposal.Block.Header.BlockHash, proto.Size(proposal.Block),
	)

	// add future proposal in cache
	if consensus.Height < proposal.Height ||
		(consensus.Height == proposal.Height && consensus.Round < proposal.Round) {
		consensus.logger.Infof("[%s](%d/%d/%s) receive future proposal %s(%d/%d/%x)",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			proposal.Voter, proposal.Height, proposal.Round, proposal.Block.Header.BlockHash)
		consensus.consensusFutureMsgCache.addFutureProposal(consensus.validatorSet, proposal)
		return
	}

	height := proposal.Block.Header.BlockHeight
	if consensus.Height != height || consensus.Round != proposal.Round || consensus.Step != tbftpb.Step_PROPOSE {
		consensus.logger.Debugf("[%s](%d/%d/%s) receive invalid proposal: %s(%d/%d)",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			proposal.Voter, proposal.Height, proposal.Round)
		return
	}

	proposer, _ := consensus.validatorSet.GetProposer(consensus.blockVersion, consensus.lastHeightProposer,
		proposal.Height, proposal.Round)
	if proposer != proposal.Voter {
		consensus.logger.Infof("[%s](%d/%d/%s) proposer: %s, receive proposal from incorrect proposal: %s",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, proposer, proposal.Voter)
		return
	}

	// Proposals have been received, do not continue processing
	if consensus.Proposal != nil {
		if bytes.Equal(consensus.Proposal.PbMsg.Block.Header.BlockHash, proposal.Block.Header.BlockHash) {
			consensus.logger.Infof("[%s](%d/%d/%s) receive duplicate proposal from proposer: %s(%x)",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step, proposal.Voter, proposal.Block.Header.BlockHash)
		} else {
			consensus.logger.Infof("[%s](%d/%d/%s) receive unequal proposal from proposer: %s(%x)",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step, consensus.Proposal.PbMsg.Block.Header.BlockHash,
				proposal.Voter, proposal.Block.Header.BlockHash)
		}
		return
	}

	// Proposal validating, do not proceed
	if consensus.VerifingProposal != nil {
		if bytes.Equal(consensus.VerifingProposal.PbMsg.Block.Header.BlockHash, proposal.Block.Header.BlockHash) {
			consensus.logger.Infof("[%s](%d/%d/%s) receive proposal which is verifying from proposer: %s(%x)",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step,
				proposal.Voter, proposal.Block.Header.BlockHash)
		} else {
			consensus.logger.Infof("[%s](%d/%d/%s) receive unequal proposal with verifying proposal from proposer: %s(%x)",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step,
				consensus.VerifingProposal.PbMsg.Block.Header.BlockHash, proposal.Voter, proposal.Block.Header.BlockHash)
		}
		return
	}

	// this is a locked proposal, proc VerifyBlockWithRwSets by core
	// if verify succeed, we can enter Prevote
	if proposal.Qc != nil && proposal.TxsRwSet != nil {
		// ZYF 检查读写集以及DAG
		if err := consensus.procVerifyBlockWithRwSets(proposal); err != nil {
			consensus.logger.Warnf("[%s](%d/%d/%s) procVerifyBlockWithRwSetsl error: %v",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step,
				err,
			)
			return
		}

		consensus.Proposal = NewTBFTProposal(proposal, false)
		// enter Prevote
		consensus.enterPrevote(consensus.Height, consensus.Round)
		return
	}
	procProposalTime := CurrentTime()
	consensus.VerifingProposal = NewTBFTProposal(proposal, false)
	// Tell the consensus module to perform block validation
	consensus.msgbus.PublishSafe(msgbus.VerifyBlock, proposal.Block)

	consensus.logger.Infof("[%s](%d/%d/%s) processed proposal (%d/%d/%x), time[verify:%dms]",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step,
		proposal.Height, proposal.Round, proposal.Block.Header.BlockHash,
		procProposalTime.Sub(receiveProposalTime).Milliseconds())
}

// procVerifyBlockWithRwSets
// @Description: Process verify votes
// @receiver consensus
// @param proposal
func (consensus *ConsensusTBFTImpl) procVerifyBlockWithRwSets(proposal *tbftpb.Proposal) error {
	// verify qc
	vs, err := VerifyQcFromVotes(consensus.logger, proposal.Qc, consensus.ac,
		consensus.validatorSet, tbftpb.VoteType_VOTE_PREVOTE, proposal.GetBlock().GetHeader().GetBlockVersion())
	if err != nil {
		consensus.logger.Infof("[%s](%d/%d/%s) verify votes failed. from [%s](%d/%d/%x)",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			proposal.Voter, proposal.Height, proposal.Round, proposal.Block.Hash())
		return err
	}

	var txsRwSet []*common.TxRWSet
	for _, txRwSet := range proposal.TxsRwSet {
		txsRwSet = append(txsRwSet, txRwSet)
	}

	qc := mustMarshal(vs.ToProto())

	if proposal.Block.AdditionalData == nil || proposal.Block.AdditionalData.ExtraData == nil {
		proposal.Block.AdditionalData = &common.AdditionalData{
			ExtraData: make(map[string][]byte),
		}
	}
	proposal.Block.AdditionalData.ExtraData[TBFTAddtionalDataKey] = qc
	err = consensus.blockVerifier.VerifyBlockWithRwSets(proposal.Block, txsRwSet, protocol.SYNC_VERIFY)
	// proposal.Block.AdditionalData just for VerifyBlockWithRwSets
	proposal.Block.AdditionalData = nil
	return err
}

// updateValidatorHeight update node height
// the height of a node is updated by vote so that the height of other nodes
// can be updated as soon as possible. only updated a higher node height
func (consensus *ConsensusTBFTImpl) updateValidatorHeight(vote *tbftpb.Vote) {
	consensus.validatorSet.Lock()
	defer consensus.validatorSet.Unlock()

	height := consensus.validatorSet.validatorsHeight[vote.Voter]
	if height < vote.Height {
		consensus.validatorSet.validatorsHeight[vote.Voter] = vote.Height
	}
	consensus.validatorSet.validatorsBeatTime[vote.Voter] = time.Now().UnixNano() / 1e6
}

// procPrevote
// @Description: Process vote
// @receiver consensus
// @param prevote
func (consensus *ConsensusTBFTImpl) procPrevote(prevote *tbftpb.Vote) {
	// update node height
	consensus.updateValidatorHeight(prevote)
	// add future prevote in cache
	if consensus.Height < prevote.Height {
		consensus.logger.Infof("[%s](%d/%d/%s) receive future prevote %s(%d/%d/%x)",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			prevote.Voter, prevote.Height, prevote.Round, prevote.Hash)
		consensus.consensusFutureMsgCache.addFutureVote(consensus.validatorSet, prevote)
		return
	}

	receivePrevoteTime := CurrentTime()
	// reject the vote that below consensus current height or locked round or current round or step
	if consensus.Height != prevote.Height ||
		consensus.LockedRound > prevote.Round ||
		consensus.Round > prevote.Round ||
		(consensus.Round == prevote.Round && consensus.Step > tbftpb.Step_PREVOTE) {
		consensus.procLocalVote(prevote)
		consensus.logger.Debugf("[%s](%d/%d/%s) receive invalid vote %s(%d/%d/%s)",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			prevote.Voter, prevote.Height, prevote.Round, prevote.Type)
		return
	}

	// judge whether we need this prevote vote before verifyVote
	// we allow locally generated prevoteVote to be added repeatedly
	// because the local node will not be a byzantine node
	if !consensus.heightRoundVoteSet.isRequired(prevote.Round, prevote) &&
		prevote.Voter != consensus.Id {
		consensus.logger.Debugf("receive prevote, but the vote is not required")
		return
	}

	// retain verifyPrecommitTime , Compatible with time-consuming analysis scripts
	verifyPrevoteTime := CurrentTime()

	consensus.logger.Infof("[%s](%d/%d/%s) receive prevote %s(%d/%d/%x), time[verify:%dms]",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step,
		prevote.Voter, prevote.Height, prevote.Round, prevote.Hash,
		verifyPrevoteTime.Sub(receivePrevoteTime).Milliseconds())

	err := consensus.addVote(prevote, false)
	if err != nil {
		consensus.logger.Errorf("[%s](%d/%d/%s) addVote %s(%d/%d/%s) failed, %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			prevote.Voter, prevote.Height, prevote.Round, prevote.Type, err,
		)
		return
	}
}

func (consensus *ConsensusTBFTImpl) procPrecommit(precommit *tbftpb.Vote) {
	// update node height
	consensus.updateValidatorHeight(precommit)
	// add future precommit in cache
	if consensus.Height < precommit.Height {
		consensus.logger.Infof("[%s](%d/%d/%s) receive future precommit %s(%d/%d/%x)",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			precommit.Voter, precommit.Height, precommit.Round, precommit.Hash)
		consensus.consensusFutureMsgCache.addFutureVote(consensus.validatorSet, precommit)
		return
	}

	receivePrecommitTime := CurrentTime()
	// reject the vote that below consensus current height or locked round or current round or step
	if consensus.Height != precommit.Height ||
		consensus.LockedRound > precommit.Round ||
		consensus.Round > precommit.Round ||
		(consensus.Round == precommit.Round && consensus.Step > tbftpb.Step_PRECOMMIT) {
		consensus.procLocalVote(precommit)
		consensus.logger.Debugf("[%s](%d/%d/%s) receive invalid precommit %s(%d/%d)",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			precommit.Voter, precommit.Height, precommit.Round,
		)
		return
	}

	// judge whether we need the precommit vote before verifyVote
	// we allow locally generated precommitVote to be added repeatedly
	// because the local node will not be a byzantine node
	if !consensus.heightRoundVoteSet.isRequired(precommit.Round, precommit) &&
		precommit.Voter != consensus.Id {
		consensus.logger.Debugf("receive precommit, but the vote is not required")
		return
	}

	// retain verifyPrecommitTime , Compatible with time-consuming analysis scripts
	verifyPrecommitTime := CurrentTime()

	consensus.logger.Infof("[%s](%d/%d/%s) receive precommit %s(%d/%d/%x), time[verify:%dms]",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step,
		precommit.Voter, precommit.Height, precommit.Round, precommit.Hash,
		verifyPrecommitTime.Sub(receivePrecommitTime).Milliseconds())

	err := consensus.addVote(precommit, false)
	if err != nil {
		consensus.logger.Errorf("[%s](%d/%d/%s) addVote %s(%d/%d/%s) failed, %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			precommit.Voter, precommit.Height, precommit.Round, precommit.Type, err,
		)
		return
	}
}

// procLocalVote process vote from local node.
// consensus received quorum votes to enter other step, we still need to save and broadcast
// the vote when we receive the vote of this node.
func (consensus *ConsensusTBFTImpl) procLocalVote(vote *tbftpb.Vote) {
	if vote.Voter != consensus.Id {
		return
	}
	// add vote
	if vote.Height == consensus.Height {
		_, err := consensus.heightRoundVoteSet.addVote(vote)
		if err != nil {
			consensus.logger.Infof("[%s](%d/%d/%s) addVote %v, err: %v",
				consensus.Id, vote.Height, vote.Round, vote.Type, vote, err)
			return
		}
	} else {
		cache := consensus.consensusStateCache.getConsensusState(vote.Height)
		if cache != nil {
			_, err := cache.heightRoundVoteSet.addVote(vote)
			if err != nil {
				consensus.logger.Infof("[%s](%d/%d/%s) addVote %v, err: %v",
					consensus.Id, vote.Height, vote.Round, vote.Type, vote, err)
				return
			}
		}
	}

	consensus.logger.Infof("[%s](%d/%d/%s) receive local vote %s(%d/%d/%v/%x)",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step,
		vote.Voter, vote.Height, vote.Round, vote.Type, vote.Hash)
	// Broadcast your vote to others
	consensus.sendConsensusVote(vote, "")
}

// fetch QC, quickly reach higher round
func (consensus *ConsensusTBFTImpl) procRoundQC(roundQC *tbftpb.RoundQC) {
	if roundQC == nil {
		consensus.logger.Infof("[%s](%d/%d/%s) receive invalid roundQc: nil roundQc",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step)
		return
	}
	if roundQC.Qc == nil {
		consensus.logger.Infof("[%s](%d/%d/%s) receive invalid roundQc: roundQc.Qc is nil. from [%s](%d/%d/)",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			roundQC.Id, roundQC.Height, roundQC.Round)
		return
	}
	consensus.logger.Infof("[%s](%d/%d/%s) receive round qc from [%s](%d/%d/%x)",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step,
		roundQC.Id, roundQC.Height, roundQC.Round, roundQC.Qc.Maj23)

	// receive invalid round qc
	if roundQC.Height != consensus.Height || roundQC.Round < consensus.Round ||
		(roundQC.Round == consensus.Round && roundQC.Qc.Type != tbftpb.VoteType_VOTE_PRECOMMIT) {
		consensus.logger.Infof("[%s](%d/%d/%s) receive invalid round qc from [%s](%d/%d/%x)",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			roundQC.Id, roundQC.Height, roundQC.Round, roundQC.Qc.Maj23)
		return
	}
	// verify qc
	if VerifyRoundQc(consensus.logger, consensus.ac,
		consensus.validatorSet, roundQC, consensus.blockVersion) != nil {
		consensus.logger.Infof("[%s](%d/%d/%s) verify round qc failed. from [%s](%d/%d/%x)",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			roundQC.Id, roundQC.Height, roundQC.Round, roundQC.Qc.Maj23)
		return
	}

	for _, vote := range roundQC.Qc.Votes {
		_, err := consensus.heightRoundVoteSet.addVote(vote)
		if err != nil {
			consensus.logger.Infof("procRoundQC [%s](%d/%d/%s) addVote %v, err: %v",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step, vote, err)
			return
		}
	}
	if roundQC.Qc.Type == tbftpb.VoteType_VOTE_PREVOTE {
		// this is equivalent to completing the consensus step of prevote
		// enter prevote base on roundQC.Round
		consensus.Round = roundQC.Round
		consensus.Step = tbftpb.Step_PREVOTE
		consensus.enterPrecommit(consensus.Height, roundQC.Round)
		return
	}
	// this is equivalent to completing the consensus process of roundQC.Round
	// enter new round base on roundQC.Round+1
	if bytes.Equal(roundQC.Qc.Maj23, nilHash) {
		consensus.enterNewRound(consensus.Height, roundQC.Round+1)
	}
}

func (consensus *ConsensusTBFTImpl) handleConsensusMsg(msg *ConsensusMsg) {
	consensus.Lock()
	defer consensus.Unlock()

	switch msg.Type {
	case tbftpb.TBFTMsgType_MSG_PROPOSE:
		consensus.procPropose(msg.Msg.(*tbftpb.Proposal))
	case tbftpb.TBFTMsgType_MSG_PREVOTE:
		consensus.procPrevote(msg.Msg.(*tbftpb.Vote))
	case tbftpb.TBFTMsgType_MSG_PRECOMMIT:
		consensus.procPrecommit(msg.Msg.(*tbftpb.Vote))
	case tbftpb.TBFTMsgType_MSG_STATE:
		// Async is ok
		//go consensus.gossip.onRecvState(msg.Msg.(*tbftpb.GossipState))
	case tbftpb.TBFTMsgType_MSG_SEND_ROUND_QC:
		consensus.procRoundQC(msg.Msg.(*tbftpb.RoundQC))
	}
}

// handleProposeOptimalTimeout handles timeout event
func (consensus *ConsensusTBFTImpl) handleProposeOptimalTimeout() {
	consensus.Lock()
	defer consensus.Unlock()

	consensus.logger.Infof("[%s](%d/%d/%s) handleProposeOptimalTimeout",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step)
	if consensus.Step != tbftpb.Step_PROPOSE {
		consensus.ProposeOptimalTimer.Stop()
		return
	}

	validator, _ := consensus.validatorSet.GetProposer(consensus.blockVersion, consensus.lastHeightProposer,
		consensus.Height, consensus.Round)
	timeNow := time.Now().UnixNano() / 1e6
	consensus.validatorSet.Lock()
	if timeNow > consensus.validatorSet.validatorsBeatTime[validator]+TimeDisconnet {
		consensus.enterPrevote(consensus.Height, consensus.Round)
	} else {
		consensus.ProposeOptimalTimer.Reset(TimeDisconnet * time.Millisecond)
	}
	consensus.validatorSet.Unlock()
}

// handleTimeout handles timeout event
func (consensus *ConsensusTBFTImpl) handleTimeout(ti tbftpb.TimeoutInfo) {
	consensus.Lock()
	defer consensus.Unlock()

	consensus.logger.Infof("[%s](%d/%d/%s) handleTimeout ti: %v",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step, ti)

	switch ti.Step {
	case tbftpb.Step_PREVOTE:
		consensus.enterPrevote(ti.Height, ti.Round)
	case tbftpb.Step_PRECOMMIT:
		consensus.enterPrecommit(ti.Height, ti.Round)
	case tbftpb.Step_COMMIT:
		consensus.enterCommit(ti.Height, ti.Round)
	}
}

func (consensus *ConsensusTBFTImpl) commitBlock(block *common.Block, voteSet *tbftpb.VoteSet) {
	consensus.logger.Debugf("[%s] commitBlock to %d-%x",
		consensus.Id, block.Header.BlockHeight, block.Header.BlockHash)
	beginTime := CurrentTime()
	qc := mustMarshal(voteSet)
	marshalTime := CurrentTime()

	if block.AdditionalData == nil || block.AdditionalData.ExtraData == nil {
		block.AdditionalData = &common.AdditionalData{
			ExtraData: make(map[string][]byte),
		}
	}
	block.AdditionalData.ExtraData[TBFTAddtionalDataKey] = qc
	consensus.msgbus.Publish(msgbus.CommitBlock, block)

	consensus.logger.Infof("[%s](%d/%d/%s) consensus commit block (%d/%x), time[marshalQC:%dms]",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step,
		block.Header.BlockHeight, block.Header.BlockHash,
		marshalTime.Sub(beginTime).Milliseconds())
}

// ProposeTimeout returns timeout to wait for proposing at `round`
func (consensus *ConsensusTBFTImpl) ProposeTimeout(round int32) time.Duration {
	return time.Duration(
		consensus.TimeoutPropose.Nanoseconds()+consensus.TimeoutProposeDelta.Nanoseconds()*int64(round),
	) * time.Nanosecond
}

// PrevoteTimeout returns timeout to wait for prevoting at `round`
func (consensus *ConsensusTBFTImpl) PrevoteTimeout(round int32) time.Duration {
	return time.Duration(
		TimeoutPrevote.Nanoseconds()+TimeoutPrevoteDelta.Nanoseconds()*int64(round),
	) * time.Nanosecond
}

// PrecommitTimeout returns timeout to wait for precommiting at `round`
func (consensus *ConsensusTBFTImpl) PrecommitTimeout(round int32) time.Duration {
	return time.Duration(
		TimeoutPrecommit.Nanoseconds()+TimeoutPrecommitDelta.Nanoseconds()*int64(round),
	) * time.Nanosecond
}

// CommitTimeout returns timeout to wait for precommiting at `round`
func (consensus *ConsensusTBFTImpl) CommitTimeout(round int32) time.Duration {
	return time.Duration(TimeoutCommit.Nanoseconds()*int64(round)) * time.Nanosecond
}

// AddTimeout adds timeout event to timeScheduler
func (consensus *ConsensusTBFTImpl) AddTimeout(duration time.Duration, height uint64, round int32,
	step tbftpb.Step) {
	consensus.timeScheduler.AddTimeoutInfo(tbftpb.TimeoutInfo{
		Duration: duration.Nanoseconds(),
		Height:   height,
		Round:    round,
		Step:     step})
}

// addVote adds `vote` to heightVoteSet
func (consensus *ConsensusTBFTImpl) addVote(vote *tbftpb.Vote, replayMode bool) error {
	_, err := consensus.heightRoundVoteSet.addVote(vote)
	if err != nil {
		consensus.logger.Infof("[%s](%d/%d/%s) addVote %v, err: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, vote, err)
		return err
	}

	switch vote.Type {
	case tbftpb.VoteType_VOTE_PREVOTE:
		consensus.addPrevoteVote(vote)
	case tbftpb.VoteType_VOTE_PRECOMMIT:
		consensus.addPrecommitVote(vote)
	}

	if consensus.Id == vote.Voter {
		// save prevote qc when send self precommit vote
		if vote.Type == tbftpb.VoteType_VOTE_PRECOMMIT && !replayMode {
			if consensus.Proposal != nil && consensus.Proposal.PbMsg != nil &&
				consensus.Proposal.PbMsg.Block != nil &&
				bytes.Equal(consensus.Proposal.PbMsg.Block.Header.BlockHash, vote.Hash) {
				// get prevote qc
				voteSet := consensus.heightRoundVoteSet.prevotes(consensus.Round)
				hash, ok := voteSet.twoThirdsMajority()
				walEntry := consensus.Proposal.PbMsg
				// get prevotes from voteSet by maj32
				if ok && !bytes.Equal(hash, nilHash) {
					// we save non-nil qc
					if walEntry.Qc != nil && len(walEntry.Qc) == 0 {
						hashStr := base64.StdEncoding.EncodeToString(hash)
						for _, v := range voteSet.VotesByBlock[hashStr].Votes {
							walEntry.Qc = append(walEntry.Qc, v)
						}
					}
					consensus.saveWalEntry(walEntry)
				}
			}
		}
		// Broadcast your vote to others
		consensus.sendConsensusVote(vote, "")
	}
	return nil
}

// addPrevoteVote
// @Description: add Prevote
// @receiver consensus
// @param vote
func (consensus *ConsensusTBFTImpl) addPrevoteVote(vote *tbftpb.Vote) {
	if consensus.Step != tbftpb.Step_PREVOTE {
		consensus.logger.Infof("[%s](%d/%d/%s) addVote prevote %s(%d/%d/%x) at inappropriate step",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			vote.Voter, vote.Height, vote.Round, vote.Hash)
		return
	}
	voteSet := consensus.heightRoundVoteSet.prevotes(vote.Round)
	hash, ok := voteSet.twoThirdsMajority()
	if !ok || voteSet.Round != consensus.Round {
		consensus.logger.Debugf("[%s](%d/%d/%s) addVote %v without majority",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, vote)

		if consensus.Round == vote.Round && voteSet.hasTwoThirdsAny() {
			consensus.logger.Infof("[%s](%d/%d/%s) addVote %v with hasTwoThirdsAny",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step, vote)
			consensus.enterPrecommit(consensus.Height, consensus.Round)
		} else if consensus.Round == vote.Round && voteSet.hasTwoThirdsNoMajority() {
			// add the prevote timeout event
			consensus.logger.Infof("[%s](%d/%d/%s) addVote %v with hasTwoThirdsAny, PrevoteTimeout is igniting",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step, vote)
			consensus.AddTimeout(consensus.PrevoteTimeout(consensus.Round), consensus.Height,
				consensus.Round, tbftpb.Step_PRECOMMIT)
		}
		return
	}

	// there is the action for unlocked
	// if we're locked but this is a recent majority32, so unlock.
	// if it matches our ProposalBlock, update the ValidBlock, else we need new round and get a new proposal
	// logic of Unlock: if " consensus.LockedRound < vote.Round <= consensus.Round "
	if (consensus.LockedProposal != nil) &&
		(consensus.LockedRound < vote.Round) &&
		(vote.Round <= consensus.Round) &&
		!bytes.Equal(hash, consensus.LockedProposal.Block.Header.BlockHash) {
		consensus.logger.Debugf("unlocking because of a higher round maj32 ,locked_round: %d, height_round: %d",
			consensus.LockedRound, vote.Round)
		// there is the unlocked action ,it means that we are ready to receive new proposal and vote for it
		consensus.LockedRound = -1
		consensus.LockedProposal = nil
	}

	consensus.logger.Infof("[%s](%d/%d/%s) prevoteQC (%d/%d/%x)",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step, vote.Height, vote.Round, hash)

	// Upon >2/3 prevotes, Step into StepPrecommit
	if consensus.Proposal != nil {
		// recent non-nil block of maj32 .
		if !isNilHash(hash) && (consensus.ValidRound < vote.Round) && (vote.Round == consensus.Round) {
			if bytes.Equal(hash, consensus.Proposal.PbMsg.Block.Header.BlockHash) {
				consensus.logger.Debugf("updating valid block because of maj32, valid_round: %d, majority_round: %d",
					consensus.ValidRound, vote.Round)
				consensus.ValidRound = vote.Round
				consensus.ValidProposal = consensus.Proposal.PbMsg
				// ValidProposal need qc
				// The backup node receives a proposal containing qc, execute "VerifyBlockWithRwSets"
				hashStr := base64.StdEncoding.EncodeToString(hash)
				for _, v := range voteSet.VotesByBlock[hashStr].Votes {
					consensus.ValidProposal.Qc = append(consensus.ValidProposal.Qc, v)
				}
			} else {
				consensus.logger.Infof("proposal [%x] is not match majority[%x], need new round, set ProposalBlock=nil",
					consensus.Proposal.PbMsg.Block.Header.BlockHash, hash)
				// we need a new round
				consensus.Proposal = nil
			}
		}
		consensus.enterPrecommit(consensus.Height, consensus.Round)
	} else {
		if isNilHash(hash) {
			consensus.enterPrecommit(consensus.Height, consensus.Round)
		} else {
			consensus.logger.Infof("[%s](%d/%d/%s) add vote failed, receive valid block: %x, but proposal is nil",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step, hash)
			consensus.enterPrecommit(consensus.Height, consensus.Round)
		}
	}
}

func (consensus *ConsensusTBFTImpl) delInvalidTxs(vs *VoteSet, hash []byte) {
	// del invalid txs
	if isNilHash(hash) {
		payload := vs.delInvalidTx(consensus.Height)
		if payload != nil {
			consensus.logger.Infof("[%s](%d/%d/%s) delete invalidTxs",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step)
			consensus.msgbus.PublishSafe(msgbus.RwSetVerifyFailTxs, payload)
		}
	}
}

// addPrecommitVote
// @Description: add Precommit Vote
// @receiver consensus
// @param vote
func (consensus *ConsensusTBFTImpl) addPrecommitVote(vote *tbftpb.Vote) {
	if consensus.Step != tbftpb.Step_PRECOMMIT {
		consensus.logger.Infof("[%s](%d/%d/%s) addVote precommit %s(%d/%d/%x) at inappropriate step",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			vote.Voter, vote.Height, vote.Round, vote.Hash)
		return
	}

	voteSet := consensus.heightRoundVoteSet.precommits(vote.Round)
	hash, ok := voteSet.twoThirdsMajority()
	if !ok || voteSet.Round != consensus.Round {
		consensus.logger.Debugf("[%s](%d/%d/%s) addVote %v without majority",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, vote)

		if consensus.Round == vote.Round && voteSet.hasTwoThirdsAny() {
			consensus.logger.Infof("[%s](%d/%d/%s) addVote %v with hasTwoThirdsAny",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step, vote)
			consensus.enterCommit(consensus.Height, consensus.Round)
		} else if consensus.Round == vote.Round && voteSet.hasTwoThirdsNoMajority() {
			// add the precommit timeout event
			consensus.logger.Infof("[%s](%d/%d/%s) addVote %v with hasTwoThirdsNoMajority, PrecommitTimeout is igniting",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step, vote)
			consensus.AddTimeout(consensus.PrecommitTimeout(consensus.Round), consensus.Height,
				consensus.Round, tbftpb.Step_COMMIT)
		}
		return
	}

	consensus.logger.Infof("[%s](%d/%d/%s) precommitQC (%d/%d/%x)",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step, vote.Height, vote.Round, hash)

	// Upon >2/3 precommits, Step into StepCommit
	if consensus.Proposal != nil {
		if isNilHash(hash) || bytes.Equal(hash, consensus.Proposal.PbMsg.Block.Header.BlockHash) {
			consensus.enterCommit(consensus.Height, consensus.Round)
		} else {
			consensus.logger.Warnf("[%s](%d/%d/%s) block matched failed, receive valid block: %x,"+
				" but unmatched with proposal: %x",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step,
				hash, consensus.Proposal.PbMsg.Block.Header.BlockHash)
		}
	} else {
		if !isNilHash(hash) {
			consensus.logger.Warnf("[%s](%d/%d/%s) receive valid block: %x, but proposal is nil",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step, hash)
		}
		consensus.enterCommit(consensus.Height, consensus.Round)
	}
}

// enterNewHeight enter `height`
func (consensus *ConsensusTBFTImpl) enterNewHeight(height uint64) {
	consensus.logger.Infof("[%s]attempt enter new height to (%d)", consensus.Id, height)
	if consensus.Height >= height {
		consensus.logger.Errorf("[%s](%v/%v/%v) invalid enter invalid new height to (%v)",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, height)
		return
	}
	addedValidators, removedValidators, err := consensus.updateChainConfig()
	if err != nil {
		consensus.logger.Errorf("[%s](%v/%v/%v) update chain config failed: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, err)
	}

	consensus.metricsMutex.Lock()
	// Update the validator in the consistency engine
	for i := 0; i < len(addedValidators); i++ {
		remoteState := &RemoteState{Id: addedValidators[i], Height: 0}
		status := make(map[int8]consistent_service.Status)
		status[remoteState.Type()] = remoteState
		remoteNodeStatus := &Node{id: addedValidators[i], status: status}
		err := consensus.consistentEngine.PutRemoter(remoteNodeStatus.ID(), remoteNodeStatus)
		if err != nil {
			consensus.logger.Errorf(err.Error())
			continue
		}
		consensus.NewGaugeVec(remoteNodeStatus.ID(), GaugeTypeHeight)
		consensus.NewGaugeVec(remoteNodeStatus.ID(), GaugeTypeRound)
	}

	for i := 0; i < len(removedValidators); i++ {
		err := consensus.consistentEngine.RemoveRemoter(removedValidators[i])
		if err != nil {
			consensus.logger.Errorf(err.Error())
			continue
		}
		if metrics, ok := consensus.metricsHeight[removedValidators[i]]; ok {
			prometheus.Unregister(metrics)
			delete(consensus.metricsHeight, removedValidators[i])
		}
		if metrics, ok := consensus.metricsRound[removedValidators[i]]; ok {
			prometheus.Unregister(metrics)
			delete(consensus.metricsRound, removedValidators[i])
		}
	}

	if metrics, ok := consensus.metricsHeight[consensus.Id]; ok {
		metrics.WithLabelValues(consensus.chainID, consensus.Id, consensus.Id).Set(float64(height))
	}
	consensus.metricsMutex.Unlock()

	consensus.consensusFutureMsgCache.updateConsensusHeight(consensus.Height)
	consensus.consensusStateCache.addConsensusState(consensus.ConsensusState)
	consensus.ConsensusState = NewConsensusState(consensus.logger, consensus.Id)
	consensus.Height = height
	consensus.validatorSet.Lock()
	consensus.validatorSet.validatorsHeight[consensus.Id] = height
	consensus.validatorSet.Unlock()
	consensus.Round = 0
	consensus.LockedRound = -1
	consensus.LockedProposal = nil
	consensus.ValidRound = -1
	consensus.ValidProposal = nil
	consensus.Step = tbftpb.Step_NEW_HEIGHT
	consensus.heightRoundVoteSet = newHeightRoundVoteSet(
		consensus.logger, consensus.Height, consensus.Round, consensus.validatorSet)
	consensus.metrics = newHeightMetrics(consensus.Height)
	consensus.metrics.SetEnterNewHeightTime()
	consensus.enterNewRound(height, 0)
}

// enterNewRound enter `round` at `height`
func (consensus *ConsensusTBFTImpl) enterNewRound(height uint64, round int32) {
	consensus.logger.Infof("[%s] attempt enterNewRound to (%d/%d)", consensus.Id, height, round)
	consensus.blockVersion = consensus.chainConf.ChainConfig().GetBlockVersion()
	if consensus.blockVersion >= blockVersion231 {
		// get the Proposer of the last block commit height
		consensus.lastHeightProposer = consensus.getLastBlockProposer()
	}
	if consensus.Height > height ||
		consensus.Round > round ||
		(consensus.Round == round && consensus.Step != tbftpb.Step_NEW_HEIGHT) {
		consensus.logger.Infof("[%s](%v/%v/%v) enter new round invalid(%v/%v)",

			consensus.Id, consensus.Height, consensus.Round, consensus.Step, height, round)
		return
	}
	consensus.Height = height
	consensus.Round = round
	consensus.Step = tbftpb.Step_NEW_ROUND
	consensus.Proposal = nil
	consensus.VerifingProposal = nil
	consensus.invalidTxs = nil
	consensus.metrics.SetEnterNewRoundTime(consensus.Round)
	consensus.metricsMutex.Lock()
	if metrics, ok := consensus.metricsRound[consensus.Id]; ok {
		metrics.WithLabelValues(consensus.chainID, consensus.Id, consensus.Id).Set(float64(consensus.Round))
	}
	consensus.metricsMutex.Unlock()
	consensus.enterPropose(height, round)
}

// enterPropose
// @Description: enter Propose
// @receiver consensus
// @param height
// @param round
func (consensus *ConsensusTBFTImpl) enterPropose(height uint64, round int32) {
	consensus.logger.Infof("[%s] attempt enterPropose to (%d/%d)", consensus.Id, height, round)
	consensus.ProposeOptimalTimer.Stop()
	if consensus.Height != height ||
		consensus.Round > round ||
		(consensus.Round == round && consensus.Step != tbftpb.Step_NEW_ROUND) {
		consensus.logger.Infof("[%s](%v/%v/%v) enter invalid propose(%v/%v)",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, height, round)
		return
	}

	// Step into Propose
	consensus.Step = tbftpb.Step_PROPOSE
	consensus.metrics.SetEnterProposalTime(consensus.Round)
	// Calculate timeout
	validator, _ := consensus.validatorSet.GetProposer(consensus.blockVersion, consensus.lastHeightProposer,
		height, round)
	consensus.validatorSet.Lock()
	validatorHeight := consensus.validatorSet.validatorsHeight[validator]
	consensus.validatorSet.Unlock()

	timeout := consensus.getProposeTimeout(validator, validatorHeight, height, round)

	consensus.AddTimeout(timeout, height, round, tbftpb.Step_PREVOTE)

	if consensus.isProposer(height, round) {
		// create the proposal based on validProposal
		if consensus.ValidProposal != nil {
			proposalBlock := &consensuspb.ProposalBlock{
				Block:    consensus.ValidProposal.Block,
				TxsRwSet: consensus.ValidProposal.TxsRwSet,
			}
			consensus.proposedBlockC <- &proposedProposal{
				proposedBlock: proposalBlock,
				qc:            consensus.ValidProposal.Qc,
			}
		} else {
			// checks whether the local node need generate the proposal
			if consensus.validatorSet.checkProposed(consensus.Height) {
				consensus.sendProposeState(true)
			}
		}
	} else {
		consensus.sendProposeState(false)
	}

	// get proposal and vote from cache
	// notice : only the backup node possible get proposal from cache
	consensus.getFutureMsgFromCache(consensus.Height, consensus.Round)
}

// getProposeTimeout return timeout to wait for proposing at `round`
// we implement the optimization of timeout for proposed step. select two different processes
// through the config:
// 1. the lower the height of the proposer, the shorter ProposeTimeout for other nodes to wait for it
// 2. if the state of the proposer is len(validators) heights behind, the ProposeTimeout of the other nodes
// is TimeoutProposeOptimal
func (consensus *ConsensusTBFTImpl) getProposeTimeout(validator string, validatorHeight,
	height uint64, round int32) time.Duration {
	timeout := consensus.ProposeTimeout(round)
	// two different ways
	if consensus.ProposeOptimal {
		// local node or height 1, not optimized timeout
		if validator == consensus.Id || height <= 1 {
			return timeout
		}
		// if the state of the proposer is len(validators) heights behind or proposer disconnected
		// the ProposeTimeout of the other nodes is TimeoutProposeOptimal
		timeNow := time.Now().UnixNano() / 1e6
		if validatorHeight+uint64(len(consensus.validatorSet.Validators)) < height ||
			timeNow > consensus.validatorSet.validatorsBeatTime[validator]+TimeDisconnet {
			timeout = consensus.TimeoutProposeOptimal
		} else {
			// start propose optimal timer
			// we periodically check the connection status of proposer
			// the timer will be stopped at enterPrevote or entrPrepose
			consensus.ProposeOptimalTimer.Reset(TimeDisconnet * time.Millisecond)
		}
	} else {
		t := height - validatorHeight
		if height <= validatorHeight {
			t = 1
		}
		timeout = time.Duration(uint64(timeout) / t)
		if timeout < (2 * time.Second) {
			timeout = 2 * time.Second
		}
	}
	if timeout != consensus.ProposeTimeout(round) {
		consensus.logger.Infof("set timeout %d(height:%d,validatorHeight:%d,round:%d)",
			timeout, height, validatorHeight, round)
	}
	return timeout
}

func (consensus *ConsensusTBFTImpl) getFutureMsgFromCache(height uint64, round int32) {
	// only the backup node possible get proposal from cache
	// proposer is waiting proposal from core
	proposal := consensus.consensusFutureMsgCache.getConsensusFutureProposal(height, round)
	if proposal != nil {
		consensus.externalMsgC <- &ConsensusMsg{
			Type: tbftpb.TBFTMsgType_MSG_PROPOSE,
			Msg:  proposal,
		}
		consensus.logger.Infof("[%s](%d/%d/%s) get future proposal %s(%d/%d/%x)",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			proposal.Voter, proposal.Height, proposal.Round, proposal.Block.Hash())
	}

	// get vote from cache
	// put into the externalMsgC
	roundVs := consensus.consensusFutureMsgCache.getConsensusFutureVote(height, round)
	if roundVs != nil {
		for _, prevote := range roundVs.Prevotes.Votes {
			consensus.externalMsgC <- &ConsensusMsg{
				Type: tbftpb.TBFTMsgType_MSG_PREVOTE,
				Msg:  prevote,
			}
			consensus.logger.Infof("[%s](%d/%d/%s) get future prevote from cache. %s(%d/%d/%x)",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step,
				prevote.Voter, prevote.Height, prevote.Round, prevote.Hash)
		}

		for _, precommit := range roundVs.Precommits.Votes {
			consensus.externalMsgC <- &ConsensusMsg{
				Type: tbftpb.TBFTMsgType_MSG_PRECOMMIT,
				Msg:  precommit,
			}
			consensus.logger.Infof("[%s](%d/%d/%s) get future precommit from cache. %s(%d/%d/%x)",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step,
				precommit.Voter, precommit.Height, precommit.Round, precommit.Hash)
		}
	}
}

// enterPrevote enter `prevote` phase
func (consensus *ConsensusTBFTImpl) enterPrevote(height uint64, round int32) {
	consensus.ProposeOptimalTimer.Stop()
	if consensus.Height != height ||
		consensus.Round > round ||
		(consensus.Round == round && consensus.Step != tbftpb.Step_PROPOSE) {
		consensus.logger.Infof("[%s](%v/%v/%v) enter invalid prevote(%v/%v)",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, height, round)
		return
	}

	enterPrevoteTime := CurrentTime()
	consensus.logger.Infof("[%s](%v/%v/%v) enter prevote (%v/%v)",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step, height, round)

	// Enter StepPrevote
	consensus.Step = tbftpb.Step_PREVOTE
	consensus.metrics.SetEnterPrevoteTime(consensus.Round)

	// Disable propose
	consensus.sendProposeState(false)

	var hash = nilHash
	if consensus.LockedProposal != nil {
		// If a block is locked, prevote that.
		consensus.logger.Infof("prevote step; already locked on a block; prevoting locked block")
		hash = consensus.LockedProposal.Block.Header.BlockHash
	} else if consensus.Proposal != nil {
		hash = consensus.Proposal.PbMsg.Block.Header.BlockHash
	}

	// Broadcast prevote
	// prevote := createPrevoteProto(consensus.Id, consensus.Height, consensus.Round, hash)
	prevote := NewVote(tbftpb.VoteType_VOTE_PREVOTE, consensus.Id, consensus.Height, consensus.Round, hash)

	// if hash is nil
	if bytes.Equal(hash, nilHash) {
		prevote.InvalidTxs = consensus.invalidTxs
	}
	err := consensus.signVote(prevote)
	if err != nil {
		consensus.logger.Errorf("enter Prevote sign Vote error: %s", err)
	}
	signPrevoteTime := CurrentTime()

	prevoteMsg := createPrevoteConsensusMsg(prevote)
	consensus.internalMsgC <- prevoteMsg

	consensus.logger.Infof("[%s](%v/%v/%v) generated prevote (%d/%d/%x), time[sign:%dms]",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step,
		prevote.Height, prevote.Round, prevote.Hash,
		signPrevoteTime.Sub(enterPrevoteTime).Milliseconds())
}

// enterPrecommit enter `precommit` phase
func (consensus *ConsensusTBFTImpl) enterPrecommit(height uint64, round int32) {
	if consensus.Height != height ||
		consensus.Round > round ||
		(consensus.Round == round && consensus.Step != tbftpb.Step_PREVOTE) {
		consensus.logger.Infof("[%s](%v/%v/%v) enter precommit invalid(%v/%v)",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, height, round)
		return
	}

	enterPrecommitTime := CurrentTime()
	consensus.logger.Infof("[%s](%v/%v/%v) enter precommit (%v/%v)",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step, height, round)

	// Enter StepPrecommit
	consensus.Step = tbftpb.Step_PRECOMMIT
	consensus.metrics.SetEnterPrecommitTime(consensus.Round)

	voteSet := consensus.heightRoundVoteSet.prevotes(consensus.Round)
	hash, ok := voteSet.twoThirdsMajority()
	// del invalid txs
	consensus.delInvalidTxs(voteSet, hash)
	if !ok {
		if voteSet.hasTwoThirdsAny() || voteSet.hasTwoThirdsNoMajority() {
			hash = nilHash
			consensus.logger.Infof("[%s](%v/%v/%v) enter precommit to nil because hasTwoThirdsAny "+
				"or hasTwoThirdsNoMajority", consensus.Id, consensus.Height, consensus.Round, consensus.Step)
		} else {
			consensus.logger.Errorf("panic: this should not happen")
			panic("this should not happen")
		}
	} else {
		// There was a maj32 in the prevote set
		switch {
		case isNilHash(hash):
			// +2/3 prevoted nil. Unlock and precommit nil.
			if consensus.LockedProposal == nil {
				consensus.logger.Debugf("precommit step; +2/3 prevoted for nil")
			} else {
				consensus.logger.Debugf("precommit step; +2/3 prevoted for nil; unlocking")
				consensus.LockedRound = -1
				consensus.LockedProposal = nil
			}
		case consensus.LockedProposal != nil && bytes.Equal(hash, consensus.LockedProposal.Block.Header.BlockHash):
			// If we're already locked on that block, precommit it, and update the LockedRound
			consensus.logger.Debugf("precommit step; +2/3 prevoted locked block; relocking")
			consensus.LockedRound = round
		case consensus.Proposal != nil && bytes.Equal(hash, consensus.Proposal.PbMsg.Block.Header.BlockHash):
			// If +2/3 prevoted for proposal block, locked and precommit it
			consensus.logger.Debugf("precommit step; +2/3 prevoted proposal block; locking ,hash = %x", hash)
			consensus.LockedRound = round
			consensus.LockedProposal = consensus.Proposal.PbMsg
		default:
			// There was a maj32 in this round for a block we don't have
			consensus.logger.Debugf("precommit step; +2/3 prevotes for a block we do not have; voting nil")
			consensus.LockedRound = -1
			consensus.LockedProposal = nil
			hash = nilHash
		}
	}

	// Broadcast precommit
	precommit := NewVote(tbftpb.VoteType_VOTE_PRECOMMIT, consensus.Id, consensus.Height, consensus.Round, hash)
	err := consensus.signVote(precommit)
	if err != nil {
		consensus.logger.Errorf("enter Precommit sign Vote error: %s", err)
	}
	signPrecommitTime := CurrentTime()

	precommitMsg := createPrecommitConsensusMsg(precommit)
	consensus.internalMsgC <- precommitMsg

	consensus.logger.Infof("[%s](%v/%v/%v) generated precommit (%d/%d/%x), time[sign:%dms]",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step,
		precommit.Height, precommit.Round, precommit.Hash,
		signPrecommitTime.Sub(enterPrecommitTime))
}

// enterCommit enter `Commit` phase
func (consensus *ConsensusTBFTImpl) enterCommit(height uint64, round int32) {
	if consensus.Height != height ||
		consensus.Round > round ||
		(consensus.Round == round && consensus.Step != tbftpb.Step_PRECOMMIT) {
		consensus.logger.Infof("[%s](%d/%d/%s) enterCommit invalid(%v/%v)",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, height, round)
		return
	}

	consensus.logger.Infof("[%s](%d/%d/%s) enter commit (%v/%v)",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step, height, round)

	// Enter StepCommit
	consensus.Step = tbftpb.Step_COMMIT
	consensus.metrics.SetEnterCommitTime(consensus.Round)
	consensus.logger.Infof("[%s] consensus cost: %s", consensus.Id, consensus.metrics.roundString(consensus.Round))

	voteSet := consensus.heightRoundVoteSet.precommits(consensus.Round)
	hash, ok := voteSet.twoThirdsMajority()
	if !isNilHash(hash) && !ok {
		// This should not happen
		consensus.logger.Errorf("[%s]-%x, enter commit failed, without majority", consensus.Id, hash)
		panic(fmt.Errorf("[%s]-%x, enter commit failed, without majority", consensus.Id, hash))
	}

	if consensus.LockedProposal != nil && bytes.Equal(hash, consensus.LockedProposal.Block.Hash()) {
		if consensus.Proposal == nil ||
			(consensus.Proposal != nil && !bytes.Equal(consensus.Proposal.PbMsg.Block.Hash(),
				consensus.LockedProposal.Block.Hash())) {
			consensus.logger.Debugf("commit is for a locked block; set ProposalBlock=LockedBlock, hash = %x", hash)
			if consensus.isProposer(consensus.Height, consensus.Round) {
				consensus.Proposal = NewTBFTProposal(consensus.LockedProposal, true)
			} else {
				consensus.Proposal = NewTBFTProposal(consensus.LockedProposal, false)
			}
		}

	}

	if consensus.Proposal != nil && !bytes.Equal(hash, consensus.Proposal.PbMsg.Block.Hash()) {
		hash = nilHash
	}

	if isNilHash(hash) || consensus.Proposal == nil {
		// consensus.AddTimeout(consensus.CommitTimeout(round), consensus.Height, round+1, tbftpb.Step_NEW_ROUND)
		consensus.enterNewRound(consensus.Height, round+1)
	} else {
		// Proposal block hash must be match with precommited block hash
		if !bytes.Equal(hash, consensus.Proposal.PbMsg.Block.Header.BlockHash) {
			// This should not happen
			consensus.logger.Errorf("[%s] block match failed, unmatch precommit hash: %x with proposal hash: %x",
				consensus.Id, hash, consensus.Proposal.PbMsg.Block.Header.BlockHash)
			panic(fmt.Errorf("[%s] block match failed, unmatch precommit hash: %x with proposal hash: %x",
				consensus.Id, hash, consensus.Proposal.PbMsg.Block.Header.BlockHash))
		}

		// Commit block to core engine
		consensus.commitBlock(consensus.Proposal.PbMsg.Block, voteSet.ToProto())
	}
}

func isNilHash(hash []byte) bool {
	return len(hash) == 0 || bytes.Equal(hash, nilHash)
}

// isProposer returns true if this node is proposer at `height` and `round`,
// and returns false otherwise
func (consensus *ConsensusTBFTImpl) isProposer(height uint64, round int32) bool {
	proposer, _ := consensus.validatorSet.GetProposer(consensus.blockVersion, consensus.lastHeightProposer,
		height, round)

	return proposer == consensus.Id
}

// getLastBlockProposer return the proposer in the last block
func (consensus *ConsensusTBFTImpl) getLastBlockProposer() string {
	lastBlock := consensus.ledgerCache.GetLastCommittedBlock()
	if lastBlock == nil || lastBlock.Header.Proposer == nil {
		return ""
	}

	member, err := consensus.ac.NewMember(lastBlock.Header.Proposer)
	if err != nil {
		consensus.logger.Errorf("[%s](%d/%d/%s) getLastBlockProposer failed, ac err : %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, err)
		return ""
	}

	certId := member.GetMemberId()
	proposerId, err := consensus.netService.GetNodeUidByCertId(certId)
	if err != nil {
		consensus.logger.Errorf("[%s](%d/%d/%s) getLastBlockProposer failed: %v, GetNodeUidByCertId failed %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, certId, err)
		return ""
	}

	consensus.logger.Infof("[%s](%d/%d/%s) getLastBlockProposer , proposerId[%s]",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step, proposerId)
	return proposerId
}

// ToProto copy *ConsensusState to *tbftpb.ConsensusState
// @receiver consensus
// @return *tbftpb.ConsensusState
func (consensus *ConsensusTBFTImpl) ToProto() *tbftpb.ConsensusState {
	consensus.RLock()
	defer consensus.RUnlock()
	msg := proto.Clone(consensus.toProto())
	return msg.(*tbftpb.ConsensusState)
}

// ToGossipStateProto convert *ConsensusTBFTImpl to *tbftpb.GossipState
// @receiver consensus
// @return *tbftpb.GossipState
func (consensus *ConsensusTBFTImpl) ToGossipStateProto() *tbftpb.GossipState {
	consensus.RLock()
	defer consensus.RUnlock()

	var proposal []byte
	if consensus.Proposal != nil {
		proposal = consensus.Proposal.PbMsg.Block.Header.BlockHash
	}

	var verifingProposal []byte
	if consensus.Proposal != nil {
		verifingProposal = consensus.Proposal.PbMsg.Block.Header.BlockHash
	}

	gossipProto := &tbftpb.GossipState{
		Id:               consensus.Id,
		Height:           consensus.Height,
		Round:            consensus.Round,
		Step:             consensus.Step,
		Proposal:         proposal,
		VerifingProposal: verifingProposal,
	}
	if consensus.heightRoundVoteSet.getRoundVoteSet(consensus.Round) != nil {
		gossipProto.RoundVoteSet = consensus.heightRoundVoteSet.getRoundVoteSet(consensus.Round).ToProto()
	}
	msg := proto.Clone(gossipProto)
	return msg.(*tbftpb.GossipState)
}

// signProposal
// @Description: sign Proposal
// @receiver consensus
// @param proposal
// @return error
func (consensus *ConsensusTBFTImpl) signProposal(proposal *tbftpb.Proposal) error {
	proposalBz := mustMarshal(CopyProposalWithBlockHeader(proposal))
	sig, err := consensus.signer.Sign(consensus.chainConf.ChainConfig().Crypto.Hash, proposalBz)
	if err != nil {
		consensus.logger.Errorf("[%s](%d/%d/%v) sign proposal %s(%d/%d)-%x failed: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			proposal.Voter, proposal.Height, proposal.Round, proposal.Block.Header.BlockHash, err)
		return err
	}

	serializeMember, err := consensus.signer.GetMember()
	if err != nil {
		consensus.logger.Errorf("[%s](%d/%d/%v) get serialize member failed: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, err)
		return err
	}

	proposal.Endorsement = &common.EndorsementEntry{
		Signer:    serializeMember,
		Signature: sig,
	}
	return nil
}

// signVote
// @Description: sign Vote
// @receiver consensus
// @param vote
// @return error
func (consensus *ConsensusTBFTImpl) signVote(vote *tbftpb.Vote) error {
	voteBz := mustMarshal(vote)
	sig, err := consensus.signer.Sign(consensus.chainConf.ChainConfig().Crypto.Hash, voteBz)
	if err != nil {
		consensus.logger.Errorf("[%s](%d/%d/%v) sign vote %s(%d/%d)-%x failed: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			vote.Voter, vote.Height, vote.Round, vote.Hash, err)
		return err
	}

	serializeMember, err := consensus.signer.GetMember()
	if err != nil {
		consensus.logger.Errorf("[%s](%d/%d/%v) get serialize member failed: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, err)
		return err
	}
	vote.Endorsement = &common.EndorsementEntry{
		Signer:    serializeMember,
		Signature: sig,
	}
	return nil
}

func (consensus *ConsensusTBFTImpl) verifyProposal(proposal *tbftpb.Proposal) error {
	if proposal.Endorsement == nil || proposal.Endorsement.Signer == nil {
		return fmt.Errorf("endorsement or signer is nil. proposal:%+v", proposal)
	}
	proposalBz := mustMarshal(CopyProposalWithBlockHeader(proposal))
	principal, err := consensus.ac.CreatePrincipal(
		protocol.ResourceNameConsensusNode,
		[]*common.EndorsementEntry{proposal.Endorsement},
		proposalBz,
	)
	if err != nil {
		consensus.logger.Errorf("[%s](%d/%d/%s) receive proposal new principal failed, %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, err)
		return err
	}

	result, err := consensus.ac.VerifyMsgPrincipal(principal, proposal.GetBlock().GetHeader().GetBlockVersion())
	if err != nil {
		consensus.logger.Errorf("[%s](%d/%d/%s) receive proposal VerifyPolicy result: %v, error %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, result, err)
		return err
	}

	if !result {
		consensus.logger.Errorf("[%s](%d/%d/%s) receive proposal VerifyPolicy result: %v, error %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, result, err)
		return fmt.Errorf("VerifyPolicy result: %v", result)
	}

	return nil
}

func (consensus *ConsensusTBFTImpl) verifyVote(voteProto *tbftpb.Vote) error {
	if voteProto.Endorsement == nil || voteProto.Endorsement.Signer == nil {
		return fmt.Errorf("endorsement or Signer is nil. voteProto:%+v", voteProto)
	}
	voteProtoCopy := proto.Clone(voteProto)
	vote, ok := voteProtoCopy.(*tbftpb.Vote)
	if !ok {
		return fmt.Errorf("interface to *tbftpb.Vote failed")
	}
	vote.Endorsement = nil
	message := mustMarshal(vote)

	principal, err := consensus.ac.CreatePrincipal(
		protocol.ResourceNameConsensusNode,
		[]*common.EndorsementEntry{voteProto.Endorsement},
		message,
	)
	if err != nil {
		consensus.logger.Errorf("[%s](%d/%d/%s) verifyVote new policy failed %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, err)
		return err
	}

	result, err := consensus.ac.VerifyMsgPrincipal(principal, consensus.blockVersion)
	if err != nil {
		consensus.logger.Errorf("[%s](%d/%d/%s) verifyVote verify policy failed %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, err)
		return err
	}

	if !result {
		consensus.logger.Errorf("[%s](%d/%d/%s) verifyVote verify policy result: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, result)
		return fmt.Errorf("verifyVote result: %v", result)
	}

	member, err := consensus.ac.NewMember(voteProto.Endorsement.Signer)
	if err != nil {
		consensus.logger.Errorf("[%s](%d/%d/%s) verifyVote new member failed %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, err)
		return err
	}

	var uid string
	chainConf := consensus.chainConf.ChainConfig()
	for _, v := range chainConf.TrustMembers {
		if v.MemberInfo == string(voteProto.Endorsement.Signer.MemberInfo) {
			uid = v.NodeId
			break
		}
	}

	if uid == "" {
		certId := member.GetMemberId()
		uid, err = consensus.netService.GetNodeUidByCertId(certId)
		if err != nil {
			consensus.logger.Errorf("[%s](%d/%d/%s) verifyVote certId: %v, GetNodeUidByCertId failed %v",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step, certId, err)
			return err
		}
	}

	if uid != voteProto.Voter {
		consensus.logger.Errorf("[%s](%d/%d/%s) verifyVote failed, uid %s is not equal with voter %s",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step,
			uid, voteProto.Voter)
		return fmt.Errorf("verifyVote failed, unmatch uid: %v with vote: %v", uid, voteProto.Voter)
	}

	return nil
}

// getValidatorSet get validator set from tbft
func (consensus *ConsensusTBFTImpl) getValidatorSet() *validatorSet {
	consensus.Lock()
	defer consensus.Unlock()
	return consensus.validatorSet
}

// saveWalEntry saves entry to Wal
func (consensus *ConsensusTBFTImpl) saveWalEntry(entry interface{}) {
	beginTime := CurrentTime()
	if consensus.walWriteMode == wal_service.NonWalWrite {
		return
	}

	var walType tbftpb.WalEntryType
	var data []byte
	switch m := entry.(type) {
	case *tbftpb.Proposal:
		walType = tbftpb.WalEntryType_PROPOSAL_ENTRY
		data = mustMarshalWal(m)
		consensus.logger.Debugf("[%s](%d/%d/%s) save wal data length: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, len(data))
	default:
		consensus.logger.Fatalf("[%s](%d/%d/%s) save wal of unknown type",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step)
	}
	marshalDataTime := CurrentTime()

	walEntry := &tbftpb.WalEntry{
		Height: consensus.Height,
		Type:   walType,
		Data:   data,
	}

	entry, err := proto.Marshal(walEntry)
	marshalEntryTime := CurrentTime()
	if err != nil {
		consensus.logger.Fatalf("[%s](%d/%d/%s) marshal failed: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, err)
	}

	err = consensus.wal.Write(0, entry)
	if err != nil {
		consensus.logger.Fatalf("[%s](%d/%d/%s) save wal type: %s write error: %v",
			consensus.Id, consensus.Height, consensus.Round, consensus.Step, walType, err)
	}
	endTime := CurrentTime()

	consensus.logger.Infof("[%s](%d/%d/%s) consensus save wal, "+
		"time[marshalData:%dms, marshalEntry:%dms, saveWal:%dms, total:%dms]",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step,
		marshalDataTime.Sub(beginTime).Milliseconds(), marshalEntryTime.Sub(marshalDataTime).Milliseconds(),
		endTime.Sub(marshalEntryTime).Milliseconds(), endTime.Sub(beginTime).Milliseconds())

	consensus.logger.Debugf("[%s](%d/%d/%s) save wal type: %s data length: %v",
		consensus.Id, consensus.Height, consensus.Round, consensus.Step, walType, len(data))
}

// replayWal replays the wal when the node starting
func (consensus *ConsensusTBFTImpl) replayWal() error {
	currentHeight, err := consensus.ledgerCache.CurrentHeight()
	if err != nil {
		return err
	}

	// if no write wal now, enter new height directly
	if consensus.walWriteMode == wal_service.NonWalWrite {
		consensus.logger.Infof("[%s] no write wal, enter new height(%d) directly", consensus.Id, currentHeight+1)
		consensus.enterNewHeight(currentHeight + 1)
		return nil
	}

	// if write wal, need to replay wal
	lastEntry := &tbftpb.WalEntry{}
	it := consensus.wal.NewLogIterator()
	if it == nil {
		consensus.logger.Infof("[%s] no write wal, enter new height(%d) directly", consensus.Id, currentHeight+1)
		consensus.enterNewHeight(currentHeight + 1)
		return nil
	}
	it.SkipToLast()
	defer it.Release()
	if it.HasPre() {
		lastData, err := it.Previous().Get()
		if err != nil {
			return err
		}
		mustUnmarshal(lastData, lastEntry)
	}

	height := lastEntry.Height
	consensus.logger.Infof("[%s] replayWal chainHeight: %d and walHeight: %d",
		consensus.Id, currentHeight, height)

	if currentHeight+1 < height {
		consensus.logger.Fatalf("[%s] replay currentHeight: %v < height-1: %v, this should not happen",
			consensus.Id, currentHeight, height-1)
	}

	if currentHeight >= height {
		// consensus is slower than ledger
		consensus.enterNewHeight(currentHeight + 1)
		return nil
	}

	// replay wal log, currentHeight=height-1
	consensus.enterNewHeight(height)

	switch lastEntry.Type {
	case tbftpb.WalEntryType_PROPOSAL_ENTRY:
		proposal := new(tbftpb.Proposal)
		mustUnmarshal(lastEntry.Data, proposal)
		err := consensus.enterPrecommitFromReplayWal(proposal)
		if err != nil {
			return err
		}

	default:
		consensus.logger.Warnf("[%s] wal replay found unrecognized type[%v], this should not happen",
			consensus.Id, lastEntry.Type)
	}
	return nil
}

func (consensus *ConsensusTBFTImpl) enterPrecommitFromReplayWal(proposal *tbftpb.Proposal) error {
	if proposal == nil || proposal.Qc == nil || proposal.TxsRwSet == nil {
		consensus.logger.Warnf("enterPrecommitFromReplayWal failed, the data from wal is unrecognized")
		return fmt.Errorf("the data from wal is unrecognized, data is null")
	}

	qc := proposal.Qc
	consensus.logger.Infof("[%s](%v/%v/%x) enter precommit from replay wal",
		consensus.Id, consensus.Height, proposal.Round, proposal.Block.Hash())

	if bytes.Equal(proposal.Block.Hash(), nilHash) || !bytes.Equal(proposal.Block.Hash(), qc[0].Hash) {
		consensus.logger.Warnf("enterPrecommitFromReplayWal failed, The proposal and the vote do not match")
		return fmt.Errorf("the proposal and the vote do not match")
	}
	// Enter Step_PREVOTE
	consensus.Step = tbftpb.Step_PREVOTE
	consensus.Proposal = NewTBFTProposal(proposal, false)
	consensus.Round = proposal.Round
	consensus.LockedProposal = proposal
	consensus.LockedRound = proposal.Round
	consensus.ValidProposal = proposal
	consensus.ValidRound = proposal.Round

	// replay prevote vote
	for _, v := range qc {
		_, err := consensus.heightRoundVoteSet.addVote(v)
		if err != nil {
			consensus.logger.Errorf("[%s](%d/%d/%s) addVote %s(%d/%d/%s) failed, %v",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step,
				v.Voter, v.Height, v.Round, v.Type, err,
			)
			return fmt.Errorf("addVote failed")
		}
	}
	consensus.enterPrecommit(consensus.Height, proposal.Round)

	consensus.logger.Infof("enterPrecommitFromReplayWal succeed")
	return nil
}

// GetValidators get validators from consensus state
// GetValidators get validators from consensus state
// @receiver consensus
// @return []string validators
// @return error always return nil
func (consensus *ConsensusTBFTImpl) GetValidators() ([]string, error) {
	if consensus.state != start {
		return nil, fmt.Errorf("the node is not a consensus node consensus.state : %d",
			consensus.state)
	}
	consensus.RLock()
	defer consensus.RUnlock()
	return consensus.validatorSet.Validators, nil
}

// GetLastHeight get current height from consensus state
// @receiver consensus
// @return uint64
func (consensus *ConsensusTBFTImpl) GetLastHeight() uint64 {
	if consensus.state != start {
		return math.MaxUint64
	}
	consensus.RLock()
	defer consensus.RUnlock()
	return consensus.Height
}

// GetConsensusStateJSON get consensus status in json format
// @receiver consensus
// @return []byte
// @return error always return nil
func (consensus *ConsensusTBFTImpl) GetConsensusStateJSON() ([]byte, error) {
	if consensus.state != start {
		return nil, fmt.Errorf("the consensus engine of this node is not started")
	}
	consensus.RLock()
	defer consensus.RUnlock()
	//isProposer:=consensus.isProposer(consensus.Height, consensus.Round)
	cs := consensus.ConsensusState.toProto()
	return mustMarshal(cs), nil
}

// createConsensusMsgFromTBFTMsgBz
// @Description: Convert tbftMsgBz to *ConsensusMsg
// @param tbftMsgBz
// @return *ConsensusMsg
func (consensus *ConsensusTBFTImpl) createConsensusMsgFromTBFTMsgBz(tbftMsgBz []byte) *ConsensusMsg {
	tbftMsg := new(tbftpb.TBFTMsg)
	mustUnmarshal(tbftMsgBz, tbftMsg)

	switch tbftMsg.Type {
	case tbftpb.TBFTMsgType_MSG_PROPOSE:
		proposal := new(tbftpb.Proposal)
		mustUnmarshal(tbftMsg.Msg, proposal)
		if err := consensus.verifyProposal(proposal); err != nil {
			consensus.logger.Warnf("[%s](%d/%d/%s) verify proposal error: %v",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step, err,
			)
			return nil
		}
		return &ConsensusMsg{
			Type: tbftMsg.Type,
			Msg:  proposal,
		}
	case tbftpb.TBFTMsgType_MSG_PREVOTE:
		prevote := new(tbftpb.Vote)
		mustUnmarshal(tbftMsg.Msg, prevote)
		if err := consensus.verifyVote(prevote); err != nil {
			consensus.logger.Errorf("[%s](%d/%d/%s) receive prevote %s(%d/%d/%x), verifyVote failed: %v",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step,
				prevote.Voter, prevote.Height, prevote.Round, prevote.Hash, err,
			)
			return nil
		}
		return &ConsensusMsg{
			Type: tbftMsg.Type,
			Msg:  prevote,
		}
	case tbftpb.TBFTMsgType_MSG_PRECOMMIT:
		precommit := new(tbftpb.Vote)
		mustUnmarshal(tbftMsg.Msg, precommit)
		if err := consensus.verifyVote(precommit); err != nil {
			consensus.logger.Errorf("[%s](%d/%d/%s) receive prevote %s(%d/%d/%x), verifyVote failed: %v",
				consensus.Id, consensus.Height, consensus.Round, consensus.Step,
				precommit.Voter, precommit.Height, precommit.Round, precommit.Hash, err,
			)
			return nil
		}
		return &ConsensusMsg{
			Type: tbftMsg.Type,
			Msg:  precommit,
		}
	case tbftpb.TBFTMsgType_MSG_STATE:
		state := new(tbftpb.GossipState)
		mustUnmarshal(tbftMsg.Msg, state)
		return &ConsensusMsg{
			Type: tbftMsg.Type,
			Msg:  state,
		}
	case tbftpb.TBFTMsgType_MSG_FETCH_ROUNDQC:
		fetchRoundQC := new(tbftpb.FetchRoundQC)
		mustUnmarshal(tbftMsg.Msg, fetchRoundQC)
		return &ConsensusMsg{
			Type: tbftMsg.Type,
			Msg:  fetchRoundQC,
		}
	case tbftpb.TBFTMsgType_MSG_SEND_ROUND_QC:
		roundQC := new(tbftpb.RoundQC)
		mustUnmarshal(tbftMsg.Msg, roundQC)
		return &ConsensusMsg{
			Type: tbftMsg.Type,
			Msg:  roundQC,
		}
	default:
		return nil
	}
}

func createStateMsgFromTBFTMsgBz(tbftMsgBz []byte) *ConsensusMsg {
	tbftMsg := new(tbftpb.TBFTMsg)
	mustUnmarshal(tbftMsgBz, tbftMsg)

	switch tbftMsg.Type {
	case tbftpb.TBFTMsgType_MSG_STATE:
		state := new(tbftpb.GossipState)
		mustUnmarshal(tbftMsg.Msg, state)
		return &ConsensusMsg{
			Type: tbftMsg.Type,
			Msg:  state,
		}
	default:
		return nil
	}
}

// NewGaugeVec builds a height or round gauge vec for prometheus
func (consensus *ConsensusTBFTImpl) NewGaugeVec(nodeId string, gaugeType int) {
	if gaugeType == GaugeTypeHeight {
		consensus.metricsHeight[nodeId] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "chainmaker",
			Subsystem: "consensus",
			Name:      fmt.Sprintf("node_status_height_%s", nodeId),
			Help:      fmt.Sprintf("block height of node %s", nodeId),
		}, []string{ChainId, LocalId, NodeId})
		consensus.logger.Infof("register metrics:chainmaker_consensus_node_status_height_%s", nodeId)
		err := prometheus.Register(consensus.metricsHeight[nodeId])
		if err != nil {
			consensus.logger.Infof("register metrics [chainmaker_consensus_node_status_height_%s] failed,"+
				" error:%+v", nodeId, err)
		}
	} else {
		consensus.metricsRound[nodeId] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "chainmaker",
			Subsystem: "consensus",
			Name:      fmt.Sprintf("node_status_round_%s", nodeId),
			Help:      fmt.Sprintf("consensus round of node %s", nodeId),
		}, []string{ChainId, LocalId, NodeId})
		consensus.logger.Infof("register metrics:chainmaker_consensus_node_status_round_%s", nodeId)
		err := prometheus.Register(consensus.metricsRound[nodeId])
		if err != nil {
			consensus.logger.Infof("register metrics [chainmaker_consensus_node_status_round_%s] failed,"+
				" error:%+v", nodeId, err)
		}
	}
}
