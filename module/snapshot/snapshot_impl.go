/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package snapshot

import (
	"chainmaker.org/chainmaker-go/module/core/common/switch_control"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/atomic"

	"chainmaker.org/chainmaker/common/v2/bitmap"
	"chainmaker.org/chainmaker/localconf/v2"
	"chainmaker.org/chainmaker/pb-go/v2/accesscontrol"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/config"
	vmPb "chainmaker.org/chainmaker/pb-go/v2/vm"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/utils/v2"
)

// The record value is written by the SEQ corresponding to TX
type sv struct {
	seq   int
	value []byte
}

type readEntry struct {
	contractName string
	rawKey       []byte
	value        []byte
}


type SnapshotImpl struct {
	lock            sync.RWMutex
	tableLock       sync.RWMutex
	blockchainStore protocol.BlockchainStore
	log             protocol.Logger
	// If the snapshot has been sealed, the results of subsequent vm execution will not be added to the snapshot
	sealed *atomic.Bool

	chainId        string
	blockTimestamp int64
	blockProposer  *accesscontrol.Member
	blockHeight    uint64
	blockVersion   uint32
	preBlockHash   []byte

	preSnapshot protocol.Snapshot

	blockFingerprint string
	lastChainConfig  *config.ChainConfig

	// applied data, please lock it before using
	txRWSetTable   []*commonPb.TxRWSet
	txTable        []*commonPb.Transaction
	specialTxTable []*commonPb.Transaction
	txResultMap    map[string]*commonPb.Result

	// 调整为分片map
	readTable  *ShardSet
	writeTable *ShardSet

	applyConflictTime *atomic.Int64
	applyAddReadTime  *atomic.Int64
	applyAddWriteTime *atomic.Int64

	txRoot    []byte
	dagHash   []byte
	rwSetHash []byte

	//wzy
	//存储注入的读写集
	AUXRwMap map[string][]sv
	//lhl
	staleReadKeys []string // ✅ 新增字段
}

// NewQuerySnapshot create a snapshot for query tx
func NewQuerySnapshot(store protocol.BlockchainStore, log protocol.Logger) (*SnapshotImpl, error) {
	txCount := 1
	lastBlock, err := store.GetLastBlock()
	if err != nil {
		return nil, err
	}
	lastChainConfig, err := store.GetLastChainConfig()
	if err != nil || lastChainConfig == nil {
		return nil, fmt.Errorf("failed to get last chain config, %v", err)
	}

	querySnapshot := &SnapshotImpl{
		blockchainStore: store,
		preSnapshot:     nil,
		log:             log,
		txResultMap:     make(map[string]*commonPb.Result, txCount),

		chainId:         lastBlock.Header.ChainId,
		blockHeight:     lastBlock.Header.BlockHeight,
		blockVersion:    lastBlock.Header.BlockVersion,
		blockTimestamp:  lastBlock.Header.BlockTimestamp,
		blockProposer:   lastBlock.Header.Proposer,
		preBlockHash:    lastBlock.Header.PreBlockHash,
		lastChainConfig: lastChainConfig,

		txTable:      make([]*commonPb.Transaction, 0, txCount),
		txRWSetTable: make([]*commonPb.TxRWSet, 0, txCount*10),

		readTable:  newShardSet(),
		writeTable: newShardSet(),

		applyConflictTime: atomic.NewInt64(0),
		applyAddReadTime:  atomic.NewInt64(0),
		applyAddWriteTime: atomic.NewInt64(0),

		txRoot:    lastBlock.Header.TxRoot,
		dagHash:   lastBlock.Header.DagHash,
		rwSetHash: lastBlock.Header.RwSetRoot,
	}

	return querySnapshot, nil
}

// GetPreSnapshot previous snapshot
func (s *SnapshotImpl) GetPreSnapshot() protocol.Snapshot {
	return s.preSnapshot
}

// SetPreSnapshot previous snapshot
func (s *SnapshotImpl) SetPreSnapshot(snapshot protocol.Snapshot) {
	s.preSnapshot = snapshot
}

// GetBlockchainStore return the blockchainStore of the snapshot
func (s *SnapshotImpl) GetBlockchainStore() protocol.BlockchainStore {
	return s.blockchainStore
}

func (s *SnapshotImpl) AddStaleReadKey(key string) {
	if s.staleReadKeys == nil {
		s.staleReadKeys = make([]string, 0)
	}
	s.staleReadKeys = append(s.staleReadKeys, key)
	s.log.Warnf("✅ 成功添加 StaleReadKey：%s", key)
}

// GetLastChainConfig return the last chain config
func (s *SnapshotImpl) GetLastChainConfig() *config.ChainConfig {
	return s.lastChainConfig
}

// GetSnapshotSize return the len of the txTable
func (s *SnapshotImpl) GetSnapshotSize() int {
	s.tableLock.RLock()
	defer s.tableLock.RUnlock()
	return len(s.txTable)
}

// GetTxTable return the txTable of the snapshot
func (s *SnapshotImpl) GetTxTable() []*commonPb.Transaction {
	return s.txTable
}

// GetSpecialTxTable return the specialTxTable of the snapshot
func (s *SnapshotImpl) GetSpecialTxTable() []*commonPb.Transaction {
	return s.specialTxTable
}

// GetTxResultMap After the scheduling is completed, get the result from the current snapshot
func (s *SnapshotImpl) GetTxResultMap() map[string]*commonPb.Result {
	return s.txResultMap
}

// GetTxRWSetTable return the snapshot's txRWSetTable
func (s *SnapshotImpl) GetTxRWSetTable() []*commonPb.TxRWSet {
	if localconf.ChainMakerConfig.SchedulerConfig.RWSetLog {
		s.log.DebugDynamic(func() string {
			info := "rwset: "
			for i, txRWSet := range s.txRWSetTable {
				info += fmt.Sprintf("read set for tx id:[%s], count [%d]<", s.txTable[i].Payload.TxId, len(txRWSet.TxReads))
				//for _, txRead := range txRWSet.TxReads {
				//	if !strings.HasPrefix(string(txRead.Key), protocol.ContractByteCode) {
				//		info += fmt.Sprintf("[%v] -> [%v], contract name [%v], version [%v],",
				//		txRead.Key, txRead.Value, txRead.ContractName, txRead.Version)
				//	}
				//}
				info += "> "
				info += fmt.Sprintf("write set for tx id:[%s], count [%d]<", s.txTable[i].Payload.TxId, len(txRWSet.TxWrites))
				for _, txWrite := range txRWSet.TxWrites {
					info += fmt.Sprintf("[%v] -> [%v], contract name [%v], ", txWrite.Key, txWrite.Value, txWrite.ContractName)
				}
				info += ">"
			}
			return info
		})
		//log.Debugf(info)
	}

	//for _, txRWSet := range s.txRWSetTable {
	//	for _, txRead := range txRWSet.TxReads {
	//		if strings.HasPrefix(string(txRead.Key), protocol.ContractByteCode) ||
	//			strings.HasPrefix(string(txRead.Key), protocol.ContractCreator) ||
	//			txRead.ContractName == syscontract.SystemContract_CERT_MANAGE.String() {
	//			txRead.Value = nil
	//		}
	//	}
	//}
	return s.txRWSetTable
}

// GetKey from snapshot
func (s *SnapshotImpl) GetKey(txExecSeq int, contractName string, key []byte) ([]byte, error) {
	// get key before txExecSeq
	//snapshotSize := s.GetSnapshotSize()

	//s.lock.RLock()
	//defer s.lock.RUnlock()
	//if txExecSeq > snapshotSize || txExecSeq < 0 {
	//	txExecSeq = snapshotSize //nolint: ineffassign, staticcheck
	//}
	finalKey := constructKey(contractName, key)
	//wzy
	//从AUXRwMap中读，由于根据dag执行，所依赖的交易一定已经执行
	if s.AUXRwMap != nil && txExecSeq != -1 {
		if AUXRwSlice, ok := s.AUXRwMap[finalKey]; ok {
			//遍历该key的所有写版本，找到小于txExecSeq的最新版本
			resultIndex := -1
			for i, sv := range AUXRwSlice {
				if sv.seq < txExecSeq {
					resultIndex = i
				}
			}
			if resultIndex != -1 {
				s.log.Debug("txseq is", txExecSeq, "read key:", finalKey, " is ", AUXRwSlice[resultIndex], "index is", resultIndex, "AUXRwSlice is", AUXRwSlice)
				return AUXRwSlice[resultIndex].value, nil
			}
		}
	}
	if sv, ok := s.writeTable.getByLock(finalKey); ok {
		return sv.value, nil
	}
	if sv, ok := s.readTable.getByLock(finalKey); ok {
		return sv.value, nil
	}

	iter := s.preSnapshot
	for iter != nil {
		if value, err := iter.GetKey(-1, contractName, key); err == nil {
			return value, nil
		}
		iter = iter.GetPreSnapshot()
	}

	return s.blockchainStore.ReadObject(contractName, key)
}

// GetKeys from snapshot
func (s *SnapshotImpl) GetKeys(txExecSeq int, keys []*vmPb.BatchKey) ([]*vmPb.BatchKey, error) {
	var (
		done              bool
		err               error
		AUXRwValues       []*vmPb.BatchKey
		writeSetValues    []*vmPb.BatchKey
		readSetValues     []*vmPb.BatchKey
		emptyAUXRwKeys    []*vmPb.BatchKey
		emptyWriteSetKeys []*vmPb.BatchKey
		emptyReadSetKeys  []*vmPb.BatchKey
		value             []*vmPb.BatchKey
	)
	// get key before txExecSeq
	//snapshotSize := s.GetSnapshotSize()
	//
	////s.lock.RLock()
	////defer s.lock.RUnlock()
	//if txExecSeq > snapshotSize || txExecSeq < 0 {
	//	txExecSeq = snapshotSize //nolint: ineffassign, staticcheck
	//}
	//wzy
	enableDAGPartial := s.AUXRwMap != nil
	if enableDAGPartial {
		if AUXRwValues, emptyAUXRwKeys, done = s.getBatchFromAUXRwMap(keys, txExecSeq); done {
			return writeSetValues, nil
		}
		if writeSetValues, emptyWriteSetKeys, done = s.getBatchFromWriteSet(emptyAUXRwKeys); done {
			return writeSetValues, nil
		}
	} else {
		if writeSetValues, emptyWriteSetKeys, done = s.getBatchFromWriteSet(keys); done {
			return writeSetValues, nil
		}
	}

	if readSetValues, emptyReadSetKeys, done = s.getBatchFromReadSet(emptyWriteSetKeys); done {
		return append(readSetValues, writeSetValues...), nil
	}

	iter := s.preSnapshot
	for iter != nil {
		if value, err = iter.GetKeys(-1, emptyReadSetKeys); err == nil {
			return append(value, append(readSetValues, writeSetValues...)...), nil
		}
		iter = iter.GetPreSnapshot()
	}

	objects, err := s.getObjects(emptyReadSetKeys)
	if err != nil {
		return nil, err
	}
	if enableDAGPartial {
		return append(objects, append(value, append(readSetValues, append(writeSetValues, AUXRwValues...)...)...)...), nil
	} else {
		return append(objects, append(value, append(readSetValues, writeSetValues...)...)...), nil
	}

}

// getObjects returns objects on given keys
func (s *SnapshotImpl) getObjects(keys []*vmPb.BatchKey) ([]*vmPb.BatchKey, error) {
	var contractName string
	if len(keys) > 0 {
		contractName = keys[0].ContractName
	}
	// index keys
	indexKeys := make(map[int]*vmPb.BatchKey, len(keys))
	res := make([]*vmPb.BatchKey, 0, len(keys))
	inputKeys := make([][]byte, 0, len(keys))
	for i, key := range keys {
		indexKeys[i] = key
		inputKeys = append(inputKeys, protocol.GetKeyStr(key.Key, key.Field))
	}

	readObjects, err := s.blockchainStore.ReadObjects(contractName, inputKeys)
	if err != nil {
		return nil, err
	}

	// construct keys from read objects and index keys
	for i, value := range readObjects {
		key := indexKeys[i]
		key.Value = value
		res = append(res, key)
	}
	return res, nil
}

// getBatchFromWriteSet  getBatchFromWriteSet
func (s *SnapshotImpl) getBatchFromWriteSet(keys []*vmPb.BatchKey) ([]*vmPb.BatchKey,
	[]*vmPb.BatchKey, bool) {
	txWrites := make([]*vmPb.BatchKey, 0, len(keys))
	emptyTxWrite := make([]*vmPb.BatchKey, 0, len(keys))
	for _, key := range keys {
		finalKey := constructKey(key.ContractName, protocol.GetKeyStr(key.Key, key.Field))
		if sv, ok := s.writeTable.getByLock(finalKey); ok {
			key.Value = sv.value
			txWrites = append(txWrites, key)
		} else {
			emptyTxWrite = append(emptyTxWrite, key)
		}
	}

	if len(emptyTxWrite) == 0 {
		return txWrites, nil, true
	}
	return txWrites, emptyTxWrite, false
}

// getBatchFromReadSet  getBatchFromReadSet
func (s *SnapshotImpl) getBatchFromReadSet(keys []*vmPb.BatchKey) ([]*vmPb.BatchKey,
	[]*vmPb.BatchKey, bool) {
	txReads := make([]*vmPb.BatchKey, 0, len(keys))
	emptyTxReadsKeys := make([]*vmPb.BatchKey, 0, len(keys))
	for _, key := range keys {
		finalKey := constructKey(key.ContractName, protocol.GetKeyStr(key.Key, key.Field))
		if sv, ok := s.readTable.getByLock(finalKey); ok {
			key.Value = sv.value
			txReads = append(txReads, key)
		} else {
			emptyTxReadsKeys = append(emptyTxReadsKeys, key)
		}
	}

	if len(emptyTxReadsKeys) == 0 {
		return txReads, nil, true
	}
	return txReads, emptyTxReadsKeys, false
}

// ApplyTxSimContext add TxSimContext to the snapshot, return current applied tx num whether success of not
func (s *SnapshotImpl) ApplyTxSimContext(txSimContext protocol.TxSimContext, specialTxType protocol.ExecOrderTxType,
	runVmSuccess bool, applySpecialTx bool, _controllers interface{}) (bool, int) {
	enableBatch := false
	enableStale := false
	if controllers, ok := _controllers.(*switch_control.SwitchControllerImpl); ok {
		enableBatch = controllers.IsEnabled(switch_control.PartDAGControl)
		enableStale = controllers.IsEnabled(switch_control.StaleControl)
	}
	//wzy
	if s.AUXRwMap != nil {
		return s.applyTxSimContextWithOrder(txSimContext, specialTxType, runVmSuccess, applySpecialTx)
	}

	tx := txSimContext.GetTx()
	s.log.DebugDynamic(func() string {
		return fmt.Sprintf("apply tx: %s, execOrderTxType:%d, runVmSuccess:%v, applySpecialTx:%v", tx.Payload.TxId,
			specialTxType, runVmSuccess, applySpecialTx)
	})

	if !applySpecialTx && s.IsSealed() {
		return false, s.GetSnapshotSize()
	}
	// 乐观处理，以所有交易都不冲突的情况进行优先处理
	txExecSeq := txSimContext.GetTxExecSeq()
	var txRWSet *commonPb.TxRWSet
	var txResult *commonPb.Result

	if enableBatch {
		s.log.Info("-------------执行到批处理交易调度代码部分---------")
	}
	// Only when the virtual machine is running normally can the read-write set be saved, or write fake conflicted key
	txRWSet = txSimContext.GetTxRWSet(runVmSuccess)
	s.log.Debugf("【gas calc】%v, ApplyTxSimContext, txRWSet = %v", txSimContext.GetTx().Payload.TxId, txRWSet)
	txResult = txSimContext.GetTxResult()
	// 实现准备好要处理的数据
	finalReadKvs := make(map[string]*sv, len(txRWSet.TxReads))
	var finalReads []readEntry
	if !enableBatch {
		for _, txRead := range txRWSet.TxReads {
			finalKey := constructKey(txRead.ContractName, txRead.Key)
			// 乐观检查，便于提前发现冲突
			if enableStale {
				currentValue, err := s.GetKey(txExecSeq, txRead.ContractName, txRead.Key)
				if err != nil {
					s.log.Warnf("failed getKey for stale-read check [%s]: %v", finalKey, err)
					return false, s.GetSnapshotSize()
				}
				if !bytes.Equal(currentValue, txRead.Value) {
					s.log.Warnf("StaleRead detected early on key %s: vmRead=%x, snapshot=%x",
						finalKey, txRead.Value, currentValue)
					txSimContext.SetStaleRead(finalKey) // 记录下哪个 key 冲突
					return false, s.GetSnapshotSize()
				}
			}
			if sv, ok := s.writeTable.getByLock(finalKey); ok {
				// WJY: 对于 txRWSet.TxReads 中的每一个读操作
				// WJY: 若存在相同键且执行序列更高的写操作，则发生冲突，返回 false
				if sv.seq >= txExecSeq {
					s.log.Debugf("Key Conflicted %+v-%+v, tx id:%s", sv.seq, txExecSeq, tx.Payload.TxId)
					if enableStale {
						// 在这里加日志
						s.log.Warnf("[STALEREAD-DETECT] txid=%s detected stale-read on key=%s, sv.seq=%d, txExecSeq=%d",
							tx.Payload.TxId, finalKey, sv.seq, txExecSeq)
						s.AddStaleReadKey(finalKey) // 记录要回滚的 key
					}
					return false, len(s.txTable) + len(s.specialTxTable)
				}
			}
			finalReadKvs[finalKey] = &sv{
				value: txRead.Value,
			}
			finalReads = append(finalReads, readEntry{
				contractName: txRead.ContractName,
				rawKey:       txRead.Key,
				value:        txRead.Value,
			})
		}
	}
	finalWriteKvs := make(map[string]*sv, len(txRWSet.TxWrites))
	if enableBatch {
		//比较交易id大小用交易执行的序号txExecSeq,tx.payload.TxId是字符串不好比较
		//提交阶段冲突检查，写写不提交，读写和写读可以提交
		//写写冲突返回false，其余冲突可以提交
		//检查是否存在WAW冲突
		for _, txWrite := range txRWSet.TxWrites {
			finalKey := constructKey(txWrite.ContractName, txWrite.Key)
			if sv, ok := s.writeTable.getByLock(finalKey); ok {
				if sv.seq >= txExecSeq {
					fmt.Println("存在WAW冲突")
					s.log.Debugf("Key Conflicted %+v-%+v, tx id:%s", sv.seq, txExecSeq, tx.Payload.TxId)
					return false, len(s.txTable) + len(s.specialTxTable)
				}
			}
		}

		//重排序算法，只要有WAR和RAW一种不冲突即可提交
		if !(hasWARConflicts(txExecSeq, txRWSet, s.readTable) == false ||
			hasRAWConflicts(txExecSeq, txRWSet, s.writeTable) == false) {
			fmt.Println("存在冲突，不提交")
			s.log.Debugf("has conflicts with RAW or WAR, not install")
			return false, len(s.txTable) + len(s.specialTxTable)
		}

		//没有冲突才可以生效结果
		//放到所有检查的最后面，对应install函数
	}
	// Append to write table
	for _, txWrite := range txRWSet.TxWrites {
		finalKey := constructKey(txWrite.ContractName, txWrite.Key)
		if enableBatch {
			//写写不冲突直接生效交易结果
			fmt.Println("提交结果")
			s.log.Debugf("tx id:%s 生效", tx.Payload.TxId)
		}
		finalWriteKvs[finalKey] = &sv{
			value: txWrite.Value,
		}
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	// it is necessary to check sealed secondly
	if !applySpecialTx && s.IsSealed() {
		return false, s.GetSnapshotSize()
	}

	if !applySpecialTx && specialTxType == protocol.ExecOrderTxTypeIterator {
		s.specialTxTable = append(s.specialTxTable, tx)
		return true, s.GetSnapshotSize() + len(s.specialTxTable)
	}

	if specialTxType == protocol.ExecOrderTxTypeIterator || txExecSeq >= len(s.txTable) {
		s.applyOptimize(tx, txRWSet, txResult, runVmSuccess, finalReadKvs, finalWriteKvs)
		return true, s.GetSnapshotSize()
	}

	// Double-Check
	// Check whether the dependent state has been modified during the running it
	start := time.Now()
	if enableStale {
		for _, entry := range finalReads {
			// —— Stale Read 再次检测 ——
			currentValue, err := s.GetKey(
				txExecSeq,
				entry.contractName,
				entry.rawKey,
			)
			if err != nil {
				s.log.Warnf("double-check getKey failed: %v", err)
				return false, s.GetSnapshotSize()
			}
			if !bytes.Equal(currentValue, entry.value) {
				s.log.Warnf("StaleRead detected in double-check on key %s", entry.contractName)
				// 统一使用 constructKey 保证格式一致
				txSimContext.SetStaleRead(constructKey(entry.contractName, entry.rawKey)) // 你可以传 entry.contractName+":"+string(entry.rawKey) */)
				return false, s.GetSnapshotSize()
			}
			composite := constructKey(entry.contractName, entry.rawKey)
			if sv, ok := s.writeTable.getByLock(composite); ok && sv.seq >= txExecSeq {
				return false, s.GetSnapshotSize() + len(s.specialTxTable)
			}
		}
	} else {
		for finalKey := range finalReadKvs {
		if sv, ok := s.writeTable.getByLock(finalKey); ok {
			if sv.seq >= txExecSeq {
				s.log.Debugf("Key Conflicted %+v-%+v, tx id:%s", sv.seq, txExecSeq, tx.Payload.TxId)
				return false, s.GetSnapshotSize() + len(s.specialTxTable)
			}
		}
	}
	micro := time.Since(start).Microseconds()
	s.applyConflictTime.Add(micro)
	s.applyOptimize(tx, txRWSet, txResult, runVmSuccess, finalReadKvs, finalWriteKvs)
	return true, s.GetSnapshotSize()
}

// wzy
func (s *SnapshotImpl) applyTxSimContextWithOrder(txSimContext protocol.TxSimContext, specialTxType protocol.ExecOrderTxType,
	runVmSuccess bool, applySpecialTx bool) (bool, int) {

	tx := txSimContext.GetTx()
	s.log.DebugDynamic(func() string {
		return fmt.Sprintf("apply tx: %s, execOrderTxType:%d, runVmSuccess:%v, applySpecialTx:%v", tx.Payload.TxId,
			specialTxType, runVmSuccess, applySpecialTx)
	})

	if !applySpecialTx && s.IsSealed() {
		return false, s.GetSnapshotSize()
	}
	// 乐观处理，以所有交易都不冲突的情况进行优先处理
	txExecSeq := txSimContext.GetTxExecSeq()
	var txRWSet *commonPb.TxRWSet
	var txResult *commonPb.Result

	// Only when the virtual machine is running normally can the read-write set be saved, or write fake conflicted key
	txRWSet = txSimContext.GetTxRWSet(runVmSuccess)
	s.log.Debugf("【gas calc】%v, ApplyTxSimContext, txRWSet = %v", txSimContext.GetTx().Payload.TxId, txRWSet)
	txResult = txSimContext.GetTxResult()
	// 实现准备好要处理的数据
	finalReadKvs := make(map[string]*sv, len(txRWSet.TxReads))
	for _, txRead := range txRWSet.TxReads {
		finalKey := constructKey(txRead.ContractName, txRead.Key)
		//从AUXRwMap中读取的sv无法检测是否冲突，因为读取时自动寻找不冲突的最新版本读
		if rwSlice, ok := s.AUXRwMap[finalKey]; !ok || txExecSeq < rwSlice[0].seq { //如果key不存在于AUXRwMap中
			// 乐观检查，便于提前发现冲突
			if sv, ok := s.writeTable.getByLock(finalKey); ok {
				if sv.seq >= txExecSeq {
					s.log.Warnf("Key Conflicted %+v-%+v, tx id:%s", sv.seq, txExecSeq, tx.Payload.TxId)
					return false, len(s.txTable) + len(s.specialTxTable)
				}
			}
			//如果从AUXDataRwMap中读则不能加入readtable会覆盖掉原始版本
			//直接抛弃似乎没有影响，txRWSet保存了所有的读写，table又仅作为缓存使用
			//但是最终要加入table因为snapshot会从上一个块的table中寻找最后的读或写
			finalReadKvs[finalKey] = &sv{
				value: txRead.Value,
			}
		}
	}
	finalWriteKvs := make(map[string]*sv, len(txRWSet.TxWrites))
	// Append to write table
	//对于写如果key存在于AUXRwMap中，则只在AUXRwMap中更新，不放入table，防止检测出冲突

	for _, txWrite := range txRWSet.TxWrites {
		finalKey := constructKey(txWrite.ContractName, txWrite.Key)
		if AUXRw, ok := s.AUXRwMap[finalKey]; !ok || txExecSeq < AUXRw[0].seq {
			finalWriteKvs[finalKey] = &sv{
				value: txWrite.Value,
			}
		} else { //如果key存在于AUXRwMap中
			resultIndex := 0
			for i, sv := range AUXRw {
				if sv.seq < txExecSeq {
					s.log.Debug("txseq is ", txExecSeq, "find sv", i, sv)
					resultIndex = i
				}
			}
			AUXRw[resultIndex] = sv{seq: txExecSeq, value: txWrite.Value}
			s.log.Debug("txseq is", txExecSeq, "write key:", finalKey, " is ", AUXRw[resultIndex], "index is", resultIndex, "AUXRwSlice is", AUXRw)
		}
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	// it is necessary to check sealed secondly
	if !applySpecialTx && s.IsSealed() {
		return false, s.GetSnapshotSize()
	}

	if !applySpecialTx && specialTxType == protocol.ExecOrderTxTypeIterator {
		s.specialTxTable = append(s.specialTxTable, tx)
		return true, s.GetSnapshotSize() + len(s.specialTxTable)
	}

	if specialTxType == protocol.ExecOrderTxTypeIterator || txExecSeq >= len(s.txTable) {
		s.applyOptimizeWithOrder(tx, txRWSet, txResult, runVmSuccess, finalReadKvs, finalWriteKvs, txExecSeq)
		return true, s.GetSnapshotSize()
	}

	// Double-Check
	// Check whether the dependent state has been modified during the running it
	start := time.Now()
	for finalKey := range finalReadKvs {
		if sv, ok := s.writeTable.getByLock(finalKey); ok {
			if sv.seq >= txExecSeq {
				s.log.Warnf("Key Conflicted %+v-%+v, tx id:%s", sv.seq, txExecSeq, tx.Payload.TxId)
				return false, s.GetSnapshotSize() + len(s.specialTxTable)
			}
		}
	}
	micro := time.Since(start).Microseconds()
	s.applyConflictTime.Add(micro)

	s.log.Debugf("tx [%+v] final readKVs is %+v, writeKvs is %+v", txExecSeq, finalReadKvs, finalWriteKvs)
	s.applyOptimizeWithOrder(tx, txRWSet, txResult, runVmSuccess, finalReadKvs, finalWriteKvs, txExecSeq)
	return true, s.GetSnapshotSize()
}

// 检查是否有WAR冲突
func hasWARConflicts(txExecSeq int, TxRWSet *commonPb.TxRWSet, reservations *ShardSet) bool {
	for _, txWrite := range TxRWSet.TxWrites {
		key := constructKey(txWrite.ContractName, txWrite.Key)
		if sv, ok := reservations.getByLock(key); ok && sv.seq < txExecSeq {
			return true
		}
	}
	return false
}

// 检查是否有RAW冲突
func hasRAWConflicts(txExecSeq int, TxRWSet *commonPb.TxRWSet, reservations *ShardSet) bool {
	for _, txRead := range TxRWSet.TxReads {
		key := constructKey(txRead.ContractName, txRead.Key)
		if sv, ok := reservations.getByLock(key); ok && sv.seq < txExecSeq {
			return true
		}
	}
	return false
}

// ApplyBlock apply tx rwset map to block
func (s *SnapshotImpl) ApplyBlock(block *commonPb.Block, txRWSetMap map[string]*commonPb.TxRWSet) {
	if len(block.Txs) != len(txRWSetMap) {
		s.log.Warnf("txs num is: %d, but rwSet num is: %d", len(block.Txs), len(txRWSetMap))
		return
	}
	for _, tx := range block.Txs {
		s.apply(tx, txRWSetMap[tx.Payload.TxId], tx.Result, tx.Result.Code == commonPb.TxStatusCode_SUCCESS)
	}
}

// After the read-write set is generated, add TxSimContext to the snapshot
func (s *SnapshotImpl) apply(tx *commonPb.Transaction, txRWSet *commonPb.TxRWSet, txResult *commonPb.Result,
	runVmSuccess bool) {
	// Append to read table
	//applySeq := len(s.txTable)
	applySeq := s.GetSnapshotSize()
	// compatible with version lower than 2201, failed transaction should not apply read set to snapshot
	// that may cause next transaction read out an error value. Failed transaction can produce invalid read set
	// by read, write and then read again the same value.
	if s.blockVersion < 2201 || runVmSuccess {
		for _, txRead := range txRWSet.TxReads {
			finalKey := constructKey(txRead.ContractName, txRead.Key)
			s.readTable.putByLock(finalKey, &sv{
				seq:   applySeq,
				value: txRead.Value,
			})
		}
	}

	// Append to write table
	for _, txWrite := range txRWSet.TxWrites {
		finalKey := constructKey(txWrite.ContractName, txWrite.Key)
		s.writeTable.putByLock(finalKey, &sv{
			seq:   applySeq,
			value: txWrite.Value,
		})
	}

	// Append to read-write-set table
	s.txRWSetTable = append(s.txRWSetTable, txRWSet)
	//s.log.DebugDynamic(func() string {
	//	return fmt.Sprintf("apply tx: %s, rwset.TxReads: %v", tx.Payload.TxId, txRWSet.TxReads)
	//})
	//s.log.DebugDynamic(func() string {
	//	return fmt.Sprintf("apply tx: %s, rwset.TxWrites: %v", tx.Payload.TxId, txRWSet.TxWrites)
	//})
	s.log.DebugDynamic(func() string {
		return fmt.Sprintf("apply tx: %s, rwset table size %d", tx.Payload.TxId, len(s.txRWSetTable))
	})

	// Add to tx result map
	s.txResultMap[tx.Payload.TxId] = txResult

	// Add to transaction table
	s.tableLock.Lock()
	defer s.tableLock.Unlock()
	s.txTable = append(s.txTable, tx)
}

// After the read-write set is generated, add TxSimContext to the snapshot
func (s *SnapshotImpl) applyOptimize(tx *commonPb.Transaction, txRWSet *commonPb.TxRWSet, txResult *commonPb.Result,
	runVmSuccess bool, finalReadKvs, finalWriteKvs map[string]*sv) {
	// Append to read table
	applySeq := len(s.txTable)
	//applySeq := s.GetSnapshotSize()
	// compatible with version lower than 2201, failed transaction should not apply read set to snapshot
	// that may cause next transaction read out an error value. Failed transaction can produce invalid read set
	// by read, write and then read again the same value.
	var wg sync.WaitGroup
	wg.Add(1)
	if s.blockVersion < 2201 || runVmSuccess {
		wg.Add(1)
		go func() {
			start := time.Now()
			for finalKey, rsv := range finalReadKvs {
				rsv.seq = applySeq
				s.readTable.putByLock(finalKey, rsv)
			}
			wg.Done()
			micro := time.Since(start).Microseconds()
			s.applyAddReadTime.Add(micro)
		}()
	}

	// Append to write table
	go func() {
		start := time.Now()
		for finalKey, wsv := range finalWriteKvs {
			wsv.seq = applySeq
			s.writeTable.putByLock(finalKey, wsv)
		}
		wg.Done()
		micro := time.Since(start).Microseconds()
		s.applyAddWriteTime.Add(micro)
	}()
	wg.Wait()
	// Append to read-write-set table
	s.txRWSetTable = append(s.txRWSetTable, txRWSet)
	//s.log.DebugDynamic(func() string {
	//	return fmt.Sprintf("apply tx: %s, rwset.TxReads: %v", tx.Payload.TxId, txRWSet.TxReads)
	//})
	//s.log.DebugDynamic(func() string {
	//	return fmt.Sprintf("apply tx: %s, rwset.TxWrites: %v", tx.Payload.TxId, txRWSet.TxWrites)
	//})
	s.log.DebugDynamic(func() string {
		return fmt.Sprintf("apply tx: %s, rwset table size %d", tx.Payload.TxId, len(s.txRWSetTable))
	})

	// Add to tx result map
	s.txResultMap[tx.Payload.TxId] = txResult

	// Add to transaction table
	s.tableLock.Lock()
	defer s.tableLock.Unlock()
	s.txTable = append(s.txTable, tx)
}

func (s *SnapshotImpl) applyOptimizeWithOrder(tx *commonPb.Transaction, txRWSet *commonPb.TxRWSet, txResult *commonPb.Result,
	runVmSuccess bool, finalReadKvs, finalWriteKvs map[string]*sv, txSeq int) {
	// Append to read table
	applySeq := txSeq
	//applySeq := s.GetSnapshotSize()
	// compatible with version lower than 2201, failed transaction should not apply read set to snapshot
	// that may cause next transaction read out an error value. Failed transaction can produce invalid read set
	// by read, write and then read again the same value.
	var wg sync.WaitGroup
	wg.Add(1)
	if s.blockVersion < 2201 || runVmSuccess {
		wg.Add(1)
		go func() {
			start := time.Now()
			for finalKey, rsv := range finalReadKvs {
				rsv.seq = applySeq
				s.readTable.putByLock(finalKey, rsv)
			}
			wg.Done()
			micro := time.Since(start).Microseconds()
			s.applyAddReadTime.Add(micro)
		}()
	}

	// Append to write table
	go func() {
		start := time.Now()
		for finalKey, wsv := range finalWriteKvs {
			wsv.seq = applySeq
			s.writeTable.putByLock(finalKey, wsv)
		}
		wg.Done()
		micro := time.Since(start).Microseconds()
		s.applyAddWriteTime.Add(micro)
	}()
	wg.Wait()
	// Append to read-write-set table
	s.txRWSetTable = append(s.txRWSetTable, txRWSet)
	//s.log.DebugDynamic(func() string {
	//	return fmt.Sprintf("apply tx: %s, rwset.TxReads: %v", tx.Payload.TxId, txRWSet.TxReads)
	//})
	//s.log.DebugDynamic(func() string {
	//	return fmt.Sprintf("apply tx: %s, rwset.TxWrites: %v", tx.Payload.TxId, txRWSet.TxWrites)
	//})
	s.log.DebugDynamic(func() string {
		return fmt.Sprintf("apply tx: %s, rwset table size %d", tx.Payload.TxId, len(s.txRWSetTable))
	})

	// Add to tx result map
	s.txResultMap[tx.Payload.TxId] = txResult

	// Add to transaction table
	s.tableLock.Lock()
	defer s.tableLock.Unlock()
	s.txTable = append(s.txTable, tx)
}

// IsSealed check if snapshot is sealed
func (s *SnapshotImpl) IsSealed() bool {
	return s.sealed.Load()
}

// GetBlockHeight returns current block height
func (s *SnapshotImpl) GetBlockHeight() uint64 {
	return s.blockHeight
}

// GetBlockTimestamp returns current block timestamp
func (s *SnapshotImpl) GetBlockTimestamp() int64 {
	return s.blockTimestamp
}

// GetBlockProposer for current snapshot
func (s *SnapshotImpl) GetBlockProposer() *accesscontrol.Member {
	return s.blockProposer
}

// Seal the snapshot
func (s *SnapshotImpl) Seal() {
	s.applyAUXRwMap()
	s.sealed.Store(true)
	s.log.Infof("block apply time[%d] is %d, %d, %d", s.blockHeight,
		s.applyConflictTime.Load(), s.applyAddReadTime.Load(), s.applyAddWriteTime.Load())
}

// BuildDAG build the block dag according to the read-write table
func (s *SnapshotImpl) BuildDAG(isSql bool, txRWSetTable []*commonPb.TxRWSet) *commonPb.DAG {
	s.lock.RLock()
	defer s.lock.RUnlock()

	var txRWSets []*commonPb.TxRWSet
	if txRWSetTable == nil {
		txRWSets = s.txRWSetTable
	} else {
		txRWSets = txRWSetTable
	}
	txCount := uint32(len(txRWSets))
	s.log.Infof("start to build DAG for block %d with %d txs", s.blockHeight, txCount)
	dag := &commonPb.DAG{}
	if txCount == 0 {
		return dag
	}
	dag.Vertexes = make([]*commonPb.DAG_Neighbor, txCount)

	if isSql {
		for i := uint32(0); i < txCount; i++ {
			dag.Vertexes[i] = &commonPb.DAG_Neighbor{
				Neighbors: make([]uint32, 0, 1),
			}
			if i != 0 {
				dag.Vertexes[i].Neighbors = append(dag.Vertexes[i].Neighbors, uint32(i-1))
			}
		}
		return dag
	}
	// build all txs' readKeyDictionary, writeKeyDictionary, readPos(the pos in readKeyDictionary) and
	// writePos(the pos in writeKeyDictionary)
	readKeyDict, writeKeyDict, readPos, writePos := s.buildDictAndPos(txRWSets)
	reachMap := make([]*bitmap.Bitmap, txCount)
	// build vertexes
	for i := uint32(0); i < txCount; i++ {
		directReachMap := s.buildReachMap(i, txRWSets[i], readKeyDict, writeKeyDict, readPos, writePos, reachMap)
		dag.Vertexes[i] = &commonPb.DAG_Neighbor{
			Neighbors: make([]uint32, 0, 16),
		}
		for _, j := range directReachMap.Pos1() {
			dag.Vertexes[i].Neighbors = append(dag.Vertexes[i].Neighbors, uint32(j))
		}
	}
	s.log.Infof("build DAG for block %d finished", s.blockHeight)
	return dag
}

// buildDictAndPos build read/write key dict and read/write key pos
func (s *SnapshotImpl) buildDictAndPos(txRWSetTable []*commonPb.TxRWSet) (map[string][]uint32, map[string][]uint32,
	map[uint32]map[string]uint32, map[uint32]map[string]uint32) {
	//Suppose there are at least 4 keys in each transaction，2 read and 2 write
	readKeyDict := make(map[string][]uint32, len(txRWSetTable)*2)
	writeKeyDict := make(map[string][]uint32, len(txRWSetTable)*2)
	readPos := make(map[uint32]map[string]uint32, len(txRWSetTable))
	writePos := make(map[uint32]map[string]uint32, len(txRWSetTable))
	for i := uint32(0); i < uint32(len(txRWSetTable)); i++ {
		readTableItemForI := txRWSetTable[i].TxReads
		writeTableItemForI := txRWSetTable[i].TxWrites
		readPos[i] = make(map[string]uint32, len(readTableItemForI))
		writePos[i] = make(map[string]uint32, len(writeTableItemForI))
		// put all read key in to readKeyDict and set their pos into readPos and writePos
		for _, keyForI := range readTableItemForI {
			key := string(keyForI.Key)
			readPos[i][key] = uint32(len(readKeyDict[key]))
			writePos[i][key] = uint32(len(writeKeyDict[key]))
			readKeyDict[key] = append(readKeyDict[key], i)
		}
		// put all write key in to writeKeyDict and set their pos into readPos and writePos
		for _, keyForI := range writeTableItemForI {
			key := string(keyForI.Key)
			writePos[i][key] = uint32(len(writeKeyDict[key]))
			_, ok := readPos[i][key]
			if !ok {
				readPos[i][key] = uint32(len(readKeyDict[key]))
			}
			writeKeyDict[key] = append(writeKeyDict[key], i)
		}
	}
	return readKeyDict, writeKeyDict, readPos, writePos
}

func (s *SnapshotImpl) buildReachMap(i uint32, txRWSet *commonPb.TxRWSet, readKeyDict, writeKeyDict map[string][]uint32,
	readPos, writePos map[uint32]map[string]uint32, reachMap []*bitmap.Bitmap) *bitmap.Bitmap {
	readTableItemForI := txRWSet.TxReads
	writeTableItemForI := txRWSet.TxWrites
	allReachForI := &bitmap.Bitmap{}
	allReachForI.Set(int(i))
	directReachForI := &bitmap.Bitmap{}
	enableDAGPartial := s.AUXRwMap != nil
	//ReadSet && WriteSet conflict
	for _, keyForI := range readTableItemForI {
		readKey := string(keyForI.Key)
		writeKeyTxs := writeKeyDict[readKey]
		if len(writeKeyTxs) == 0 {
			continue
		}
		// just check 1 write key before the tx because write keys all are conflict
		j := int(writePos[i][readKey]) - 1
		if j >= 0 && (!enableDAGPartial && !allReachForI.Has(int(writeKeyTxs[j])) || enableDAGPartial) {
			//if j >= 0 && !allReachForI.Has(int(writeKeyTxs[j])) {
			directReachForI.Set(int(writeKeyTxs[j]))
			allReachForI.Or(reachMap[writeKeyTxs[j]])
		}
	}
	//WriteSet and (all ReadSet, WriteSet) conflict
	for _, keyForI := range writeTableItemForI {
		writeKey := string(keyForI.Key)
		readKeyTxs := readKeyDict[writeKey]
		if len(readKeyTxs) > 0 {
			// we should check all readKeyTxs because read keys has no conflict
			j := int(readPos[i][writeKey]) - 1
			for ; j >= 0; j-- {
				if !allReachForI.Has(int(readKeyTxs[j])) {
					directReachForI.Set(int(readKeyTxs[j]))
					allReachForI.Or(reachMap[readKeyTxs[j]])
				}
			}
		}
		writeKeyTxs := writeKeyDict[writeKey]
		if len(writeKeyTxs) == 0 {
			continue
		}
		// just check 1 write key before the tx because write keys all are conflict
		j := int(writePos[i][writeKey]) - 1
		if j >= 0 && !allReachForI.Has(int(writeKeyTxs[j])) {
			directReachForI.Set(int(writeKeyTxs[j]))
			allReachForI.Or(reachMap[writeKeyTxs[j]])
		}
	}
	reachMap[i] = allReachForI
	return directReachForI
}

// constructKey construct keys: contractName#key
func constructKey(contractName string, key []byte) string {
	// with higher performance
	return contractName + string(key)
	//var builder strings.Builder
	//builder.WriteString(contractName)
	//builder.Write(key)
	//return builder.String()
}

// SetBlockFingerprint set block fingerprint
func (s *SnapshotImpl) SetBlockFingerprint(fp utils.BlockFingerPrint) {
	s.blockFingerprint = string(fp)
}

// GetBlockFingerprint returns current block fingerprint
func (s *SnapshotImpl) GetBlockFingerprint() string {
	return s.blockFingerprint
}

// wzy
type Sv struct {
	Seq   int    `json:"Seq"`
	Value []byte `json:"Value"`
}

func (s *SnapshotImpl) SetAUXRwMap(AUXRwMapBytes []byte) {
	//wzy
	if AUXRwMapBytes != nil {
		AUXRwMap := make(map[string][]Sv)
		s.log.Debugf("AUXRwMapBytes:%+v", AUXRwMapBytes)

		err := json.Unmarshal(AUXRwMapBytes, &AUXRwMap)
		if err != nil {
			s.log.Errorf("Unmarshal AUXRwMap Error %+v", err)
			return
		}
		s.AUXRwMap = make(map[string][]sv)
		for key, slices := range AUXRwMap {
			for _, Sv := range slices {
				s.AUXRwMap[key] = append(s.AUXRwMap[key], sv{value: Sv.Value, seq: Sv.Seq})
			}
		}
		s.log.Debugf("SetAUXRwMap:%+v", s.AUXRwMap)
	} else {
		s.AUXRwMap = nil
		s.log.Warnf("AUXRwMapBytes is empty")
	}
}
func (s *SnapshotImpl) getBatchFromAUXRwMap(keys []*vmPb.BatchKey, txExecSeq int) ([]*vmPb.BatchKey,
	[]*vmPb.BatchKey, bool) {
	txWrites := make([]*vmPb.BatchKey, 0, len(keys))
	emptyTxWrite := make([]*vmPb.BatchKey, 0, len(keys))
	for _, key := range keys {
		finalKey := constructKey(key.ContractName, protocol.GetKeyStr(key.Key, key.Field))
		if AUXRwSlice, ok := s.AUXRwMap[finalKey]; ok {
			//wzy遍历该key的所有写版本，找到小于txExecSeq的最新版本
			//s.log.Warn("txseq is ", txExecSeq, "read key :" , finalKey, "AUXRwSlice is", AUXRwSlice)
			resultIndex := -1 //结果下标
			for i, sv := range AUXRwSlice {
				if sv.seq < txExecSeq {
					resultIndex = i
				}
			}
			if resultIndex != -1 {
				s.log.Debug("txseq is", txExecSeq, "read key:", finalKey, " is ", AUXRwSlice[resultIndex], "resultIndex is", resultIndex, "AUXRwSlice is", AUXRwSlice)
				key.Value = AUXRwSlice[resultIndex].value
				txWrites = append(txWrites, key)
			} else {
				emptyTxWrite = append(emptyTxWrite, key)
			}
		}
	}

	if len(emptyTxWrite) == 0 {
		return txWrites, nil, true
	}
	return txWrites, emptyTxWrite, false
}

func (s *SnapshotImpl) applyAUXRwMap() {
	if s.AUXRwMap == nil {
		return
	}
	s.log.Debugf("apply AUXRwMap before seal,%+v", s.AUXRwMap)
	for finalKey, rwSlice := range s.AUXRwMap {
		final := 0
		for i, sv := range rwSlice {
			if sv.seq > rwSlice[final].seq {
				final = i
			}
		}
		s.writeTable.putByLock(finalKey, &sv{
			seq:   rwSlice[final].seq,
			value: rwSlice[final].value,
		})
	}
}
