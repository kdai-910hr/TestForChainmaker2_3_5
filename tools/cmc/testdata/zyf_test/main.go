package main

import (
	"fmt"
	"os"
	"strconv"
	"sync"

	"chainmaker.org/chainmaker/pb-go/v2/common"
	sdk "chainmaker.org/chainmaker/sdk-go/v2"
)

const (
	//createContractTimeout = 10000
	//claimVersion          = "1.0.0"
	//claimByteCodePath     = "fact.7z"
	sdkConfPath = "../sdk_config.yml"
	// TxFileName            = "tx.json"
)

var (
	contractName = "fact" // 合约名称
	// DataSkew         = 1.00001
	// AccountCount     = uint64(TxCount)
	TxCount, _ = strconv.Atoi(os.Args[2])
)

func main() {
	// 初始化链客户端（需要配置）
	client, err := sdk.NewChainClient(
		sdk.WithConfPath(sdkConfPath),
	)
	if err != nil {
		panic(err)
	}
	lineTx(client)
}

// 测试存储操作
func testSave(client *sdk.ChainClient, fileHash string, fileName string, time int, withSyncResult bool) error {
	kvs := []*common.KeyValuePair{
		{
			Key:   "method",
			Value: []byte("save"),
		},
		{
			Key:   "file_hash",
			Value: []byte(fileHash),
		},
		{
			Key:   "file_name",
			Value: []byte(fileName),
		},
		{
			Key:   "time",
			Value: []byte(strconv.Itoa(time)),
		},
	}

	_, err := invokeContract(client, contractName, "invoke_contract", "", kvs, withSyncResult)
	if err != nil {
		return fmt.Errorf("deposit failed: %v", err)
	}
	//fmt.Printf("Deposit %d to %s success", amount, account)
	return nil
}

// 测试查询文件
func testGetFile(client *sdk.ChainClient, fileHash string, withSyncResult bool) error {
	kvs := []*common.KeyValuePair{
		{
			Key:   "method",
			Value: []byte("findByFileHash"),
		},
		{
			Key:   "file_hash",
			Value: []byte(fileHash),
		},
	}

	resp, err := invokeContract(client, contractName, "invoke_contract", "", kvs, withSyncResult)
	if err != nil {
		return fmt.Errorf("get balance failed: %v", err)
	}

	// 实际使用中需要解析合约返回结果
	// 这里假设直接返回文件名称
	fmt.Printf("FileName of %s is %s", fileHash, string(resp.ContractResult.Result))
	return nil
}

// 通用合约调用方法
func invokeContract(client *sdk.ChainClient, contractName, method, txId string, kvs []*common.KeyValuePair, withSyncResult bool) (*common.TxResponse, error) {
	resp, err := client.InvokeContract(contractName, method, txId, kvs, -1, withSyncResult)
	if err != nil {
		return resp, fmt.Errorf("invoke contract failed: %v", err)
	}

	if resp.Code != common.TxStatusCode_SUCCESS {
		return resp, fmt.Errorf("contract response error: [code:%d]/[msg:%s]", resp.Code, resp.Message)
	}

	//fmt.Printf("Operation success! Contract result: %s\n", resp.ContractResult.Result)
	return resp, nil
}

func lineTx(client *sdk.ChainClient) {
	var wg sync.WaitGroup

	for i := 0; i < TxCount; i++ {
		wg.Add(2)
		fileHash := strconv.Itoa(i)
		fileName := "HelloWorld " + fileHash
		go func(i int) {
			defer wg.Done()
			if err := testSave(client, fileHash, fileName, i, true); err != nil {
				fmt.Printf("Save test failed: %v\n", err)
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			if err := testGetFile(client, fileHash, true); err != nil {
				fmt.Printf("Get test failed: %v\n", err)
			}
		}(i)
	}
	wg.Wait()
}

// func generateTransactions(dataSkew float64, accountCount, txCount uint64) []Transaction {
// 	r := rand.New(rand.NewSource(1))
// 	zipf := rand.NewZipf(r, dataSkew, 1.0, accountCount)
// 	txSet := make([]Transaction, int(txCount))

// 	for i := 0; i < int(txCount); i++ {
// 		from := zipf.Uint64()
// 		to := from
// 		for to == from {
// 			to = zipf.Uint64()
// 		}
// 		toAccount := fmt.Sprintf("%09d", to)
// 		fromAccount := fmt.Sprintf("%09d", from)
// 		txSet[i] = Transaction{from: fromAccount, to: toAccount}
// 	}
// 	return txSet
// }

// func generateTransactions(dataSkew float64, accountCount int64, txCount int) []Transaction {
// 	r := rand.New(rand.NewSource(100))
// 	//zipf := rand.NewZipf(r, dataSkew, 1.0, accountCount)
// 	txSet := make([]Transaction, int(txCount))

// 	for i := 0; i < int(txCount); i++ {
// 		//from := r.Int63n(accountCount)
// 		from := int64(i + 1)
// 		to := r.Int63n(accountCount)
// 		for to == from {
// 			to = r.Int63n(accountCount)
// 		}
// 		toAccount := fmt.Sprintf("%09d", to)
// 		fromAccount := fmt.Sprintf("%09d", from)
// 		txSet[i] = Transaction{from: fromAccount, to: toAccount}
// 	}
// 	return txSet
// }
