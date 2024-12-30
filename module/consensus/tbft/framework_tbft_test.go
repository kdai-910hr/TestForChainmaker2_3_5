/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tbft

import (
	//"fmt"
	"os/exec"
	"testing"
	"time"

	consensus_utils "chainmaker.org/chainmaker/consensus-utils/v2"
	"chainmaker.org/chainmaker/consensus-utils/v2/testframework"
	"chainmaker.org/chainmaker/logger/v2"
	"chainmaker.org/chainmaker/protocol/v2/test"

	//"chainmaker.org/chainmaker/consensus-utils/v2/wal_service"
	"chainmaker.org/chainmaker/pb-go/v2/config"
	configPb "chainmaker.org/chainmaker/pb-go/v2/config"
	consensuspb "chainmaker.org/chainmaker/pb-go/v2/consensus"
	"chainmaker.org/chainmaker/protocol/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	blockchainId     = "chain1"
	nodeNums         = 4
	ConsensusEngines = make([]protocol.ConsensusEngine, nodeNums)
	CoreEngines      = make([]protocol.CoreEngine, nodeNums)
	consensusType    = consensuspb.ConsensusType_TBFT
)

func TestOnlyConsensus_TBFT(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", "rm -rf chain1 default.*")
	err := cmd.Run()
	require.Nil(t, err)

	err = testframework.InitLocalConfigs()
	require.Nil(t, err)
	defer testframework.RemoveLocalConfigs()

	testframework.SetTxSizeAndTxNum(200, 10*1024)

	// init LocalConfig
	testframework.InitLocalConfig(nodeNums)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	//create test_node_configs
	testNodeConfigs, err := testframework.CreateTestNodeConfig(ctrl, blockchainId, consensusType, nodeNums,
		nil, nil, func(cfg *configPb.ChainConfig) []byte { return nil })
	if err != nil {
		t.Errorf("%v", err)
	}

	tLogger := logger.GetLogger(chainId)
	for i := 0; i < nodeNums; i++ {
		// new CoreEngine
		CoreEngines[i] = testframework.NewCoreEngineForTest(testNodeConfigs[i], tLogger)
	}
	for i := 0; i < nodeNums; i++ {
		netService := testframework.NewNetServiceForTest()
		tc := &consensus_utils.ConsensusImplConfig{
			ChainId:     testNodeConfigs[i].ChainID,
			NodeId:      testNodeConfigs[i].NodeId,
			Ac:          testNodeConfigs[i].Ac,
			ChainConf:   testNodeConfigs[i].ChainConf,
			NetService:  netService,
			Core:        CoreEngines[i],
			Signer:      testNodeConfigs[i].Signer,
			LedgerCache: testNodeConfigs[i].LedgerCache,
			MsgBus:      testNodeConfigs[i].MsgBus,
			Logger:      newMockLogger(),
		}

		// set wal write mode to non
		if tc.ChainConf.ChainConfig().Consensus == nil {
			tc.ChainConf.ChainConfig().Consensus = &config.ConsensusConfig{
				ExtConfig: make([]*config.ConfigKeyValue, 0),
			}
		} else if tc.ChainConf.ChainConfig().Consensus.ExtConfig == nil {
			tc.ChainConf.ChainConfig().Consensus.ExtConfig = make([]*config.ConfigKeyValue, 0)
		}

		// saved wal by default
		/*
			tc.ChainConf.ChainConfig().Consensus.ExtConfig = append(tc.ChainConf.ChainConfig().Consensus.ExtConfig,
				&config.ConfigKeyValue{
					Key:   wal_service.WALWriteModeKey,
					Value: fmt.Sprintf("%v", int(wal_service.NonWalWrite)),
				},
			)
		*/
		consensus, errs := New(tc)
		if errs != nil {
			t.Errorf("%v", errs)
		}

		ConsensusEngines[i] = consensus
	}

	tf, err := testframework.NewTestClusterFramework(blockchainId, consensusType, nodeNums, testNodeConfigs, ConsensusEngines, CoreEngines)
	require.Nil(t, err)

	// set log
	l := &logger.LogConfig{
		SystemLog: logger.LogNodeConfig{
			FilePath:        "./default.log",
			LogLevelDefault: "DEBUG",
			LogLevels:       map[string]string{"consensus": "DEBUG", "core": "DEBUG", "net": "DEBUG", "consistent": "DEBUG"},
			LogInConsole:    false,
			ShowColor:       true,
		},
	}
	logger.SetLogConfig(l)

	tf.Start()
	time.Sleep(60 * time.Second)
	tf.Stop()

	cmd = exec.Command("/bin/sh", "-c", "cat default.*|grep TPS")
	out, err := cmd.CombinedOutput()
	require.Nil(t, err)
	require.NotNil(t, out)
}

func newMockLogger() protocol.Logger {
	return &test.GoLogger{}
}
