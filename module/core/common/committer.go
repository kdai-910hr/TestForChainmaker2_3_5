/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package common

import (
	"fmt"
	"strconv"

	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/localconf/v2"
	commonpb "chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/config"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/utils/v2"
)

const (
	blockVersion230 = uint32(2300)
)

type CommitBlock struct {
	store           protocol.BlockchainStore
	log             protocol.Logger
	snapshotManager protocol.SnapshotManager
	ledgerCache     protocol.LedgerCache
	chainConf       protocol.ChainConf
	txFilter        protocol.TxFilter
	msgBus          msgbus.MessageBus
}

// CommitBlock the action that all consensus types do when a block is committed
func (cb *CommitBlock) CommitBlock(
	block *commonpb.Block,
	rwSetMap map[string]*commonpb.TxRWSet,
	conEventMap map[string][]*commonpb.ContractEvent) (
	dbLasts, snapshotLasts, confLasts, otherLasts, pubEventLasts, filterLasts int64, blockInfo *commonpb.BlockInfo,
	err error) {
	// record block
	rwSet := utils.RearrangeRWSet(block, rwSetMap)
	// record contract event
	events := rearrangeContractEvent(block, conEventMap)

	if block.Header.BlockVersion >= blockVersion230 {
		// notify chainConf to update config before put block
		startConfTick := utils.CurrentTimeMillisSeconds()
		if err = cb.NotifyMessage(block, events); err != nil {
			return 0, 0, 0, 0, 0, 0, nil, err
		}
		confLasts = utils.CurrentTimeMillisSeconds() - startConfTick
	}
	// put block
	startDBTick := utils.CurrentTimeMillisSeconds()
	if err = cb.store.PutBlock(block, rwSet); err != nil {
		// if put db error, then panic
		cb.log.Error(err)
		panic(err)
	}
	cb.ledgerCache.SetLastCommittedBlock(block)
	dbLasts = utils.CurrentTimeMillisSeconds() - startDBTick

	// TxFilter adds
	filterLasts = utils.CurrentTimeMillisSeconds()
	// The default filter type does not run AddsAndSetHeight
	if localconf.ChainMakerConfig.TxFilter.Type != int32(config.TxFilterType_None) {
		err = cb.txFilter.AddsAndSetHeight(utils.GetTxIds(block.Txs), block.Header.GetBlockHeight())
		if err != nil {
			// if add filter error, then panic
			cb.log.Error(err)
			panic(err)
		}
	}
	filterLasts = utils.CurrentTimeMillisSeconds() - filterLasts

	// clear snapshot
	startSnapshotTick := utils.CurrentTimeMillisSeconds()
	if err = cb.snapshotManager.NotifyBlockCommitted(block); err != nil {
		err = fmt.Errorf("notify snapshot error [%d](hash:%x)",
			block.Header.BlockHeight, block.Header.BlockHash)
		cb.log.Error(err)
		return 0, 0, 0, 0, 0, 0, nil, err
	}
	snapshotLasts = utils.CurrentTimeMillisSeconds() - startSnapshotTick
	// v220_compat Deprecated
	if block.Header.BlockVersion < blockVersion230 {
		// notify chainConf to update config when config block committed
		startConfTick := utils.CurrentTimeMillisSeconds()
		if err = NotifyChainConf(block, cb.chainConf); err != nil {
			return 0, 0, 0, 0, 0, 0, nil, err
		}
		confLasts = utils.CurrentTimeMillisSeconds() - startConfTick
	}
	// contract event
	pubEventLasts = cb.publishContractEvent(block, events)

	// monitor
	startOtherTick := utils.CurrentTimeMillisSeconds()
	blockInfo = &commonpb.BlockInfo{
		Block:     block,
		RwsetList: rwSet,
	}
	otherLasts = utils.CurrentTimeMillisSeconds() - startOtherTick

	return
}

// publishContractEvent publish contract event, return time used
func (cb *CommitBlock) publishContractEvent(block *commonpb.Block, events []*commonpb.ContractEvent) int64 {

	var (
		eventsInfos = make([]*commonpb.ContractEventInfo, 0, len(events))
		height      = block.Header.BlockHeight
		chainId     = block.Header.ChainId
	)

	if len(events) == 0 {
		// 为避免由于没有event的情况下，导致contract event订阅落后很多区块，此处依然选择推送event事件给订阅模块
		cb.msgBus.Publish(msgbus.ContractEventInfo, &commonpb.ContractEventMessageInfo{
			BlockHeight:       height,
			ChainId:           chainId,
			ContractEventList: &commonpb.ContractEventInfoList{ContractEvents: eventsInfos},
		})
		return 0
	}

	startPublishContractEventTick := utils.CurrentTimeMillisSeconds()
	cb.log.DebugDynamic(func() string {
		return fmt.Sprintf("start publish contractEventsInfo: block[%d] ", height)
	})
	for _, t := range events {
		eventInfo := &commonpb.ContractEventInfo{
			BlockHeight:     height,
			ChainId:         chainId,
			Topic:           t.Topic,
			TxId:            t.TxId,
			ContractName:    t.ContractName,
			ContractVersion: t.ContractVersion,
			EventData:       t.EventData,
		}
		eventsInfos = append(eventsInfos, eventInfo)
	}
	cb.msgBus.Publish(msgbus.ContractEventInfo, &commonpb.ContractEventMessageInfo{
		BlockHeight:       height,
		ChainId:           chainId,
		ContractEventList: &commonpb.ContractEventInfoList{ContractEvents: eventsInfos},
	})
	return utils.CurrentTimeMillisSeconds() - startPublishContractEventTick
}

func rearrangeContractEvent(block *commonpb.Block,
	conEventMap map[string][]*commonpb.ContractEvent) []*commonpb.ContractEvent {
	conEvent := make([]*commonpb.ContractEvent, 0, len(block.Txs))
	if conEventMap == nil {
		return conEvent
	}
	for _, tx := range block.Txs {
		if event, ok := conEventMap[tx.Payload.TxId]; ok {
			conEvent = append(conEvent, event...)
		}
	}
	return conEvent
}

// NotifyMessage Notify other subscription modules of chain configuration and certificate management events
func (cb *CommitBlock) NotifyMessage(block *commonpb.Block, events []*commonpb.ContractEvent) (err error) {
	if block == nil || len(block.GetTxs()) == 0 {
		return nil
	}

	if native, _ := utils.IsNativeTx(block.Txs[0]); !native {
		return nil
	}

	for _, event := range events { // one by one
		data := event.EventData
		if len(data) == 0 {
			continue
		}
		topicEnum, err := strconv.Atoi(event.Topic)
		if err != nil {
			continue
		}
		topic := msgbus.Topic(topicEnum)
		cb.msgBus.PublishSync(topic, data) // data is a []string, hexToString(proto.Marshal(data))
	}
	return nil
}
