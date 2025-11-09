package main

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	//"chainmaker.org/chainmaker/common/v2/random/uuid"
	"chainmaker.org/chainmaker/pb-go/v2/common"
	sdk "chainmaker.org/chainmaker/sdk-go/v2"
)

const (
	createContractTimeout = 10000
	claimVersion          = "1.0.0"
	claimByteCodePath     = "bank.7z"
	sdkConfPath           = "sdk_config.yml"
	TxFileName            = "tx.json"
)

var (
	// bankContractName = "bank" + strconv.FormatInt(time.Now().UnixNano(), 10)
	bankContractName = "bank" // 合约名称
	DataSkew         = 1.0
	AccountCount     = uint64(TxCount)
	TxCount, _       = strconv.Atoi(os.Args[2])
)

type Transaction struct {
	from string
	to   string
}

type shipTransaction struct {
	consignee string
	shipper   string
	carrier   string
}

func main() {
	// 初始化链客户端（需要配置）
	client, err := sdk.NewChainClient(
		sdk.WithConfPath(sdkConfPath),
	)

	if err != nil {
		panic(err)
	}
	switch os.Args[1] {
	case "benchmark":
		batchInterval, err := strconv.Atoi(os.Args[3])
		if err != nil {
			fmt.Printf("Invalid batch interval: %v\n", err)
			return
		}
		benchmark(client, batchInterval)
	case "benchmark2":
		batchInterval, _ := strconv.Atoi(os.Args[3])
		DataSkew = 0.0
		heights := make([]int, 0, 16) // 创建一个切片来存储每次循环的高度
		for i := 0; i <= 15; i++ {
			height := getHeight(client)
			heights = append(heights, height) // 将高度添加到切片中
			fmt.Println(height)
			fmt.Printf("begin benchmark with skew %f \n", DataSkew)
			benchmark(client, batchInterval)
			DataSkew += 0.1
			for {
				s, _ := client.GetPoolStatus()
				if s.CommonTxNumInQueue+s.CommonTxNumInPending == 0 {
					break
				}
				time.Sleep(6 * time.Second)
			}
		}
		// 输出所有收集到的高度值
		fmt.Println("All heights:", heights)
	case "ship":
		bankContractName = "ship"
		batchInterval, _ := strconv.Atoi(os.Args[3])
		DataSkew = 1.3
		heights := make([]int, 0, 16) // 创建一个切片来存储每次循环的高度
		for i := 0; i <= 2; i++ {
			height := getHeight(client)
			heights = append(heights, height) // 将高度添加到切片中
			fmt.Println(height)
			fmt.Printf("begin benchmark with skew %f \n", DataSkew)
			ship(client, batchInterval)
			DataSkew += 0.1
			for {
				s, _ := client.GetPoolStatus()
				fmt.Println(s)
				//fmt.Printf("%d txs remain\n" ,s.ConfigTxNumInQueue+s.CommonTxNumInPending)
				if s.CommonTxNumInQueue+s.CommonTxNumInPending == 0 {
					break
				}
				time.Sleep(5 * time.Second)
			}
		}
		// 输出所有收集到的高度值
		fmt.Println("All heights:", heights)
	case "line":
		lineTx(client)
	}
}

func benchmark(client *sdk.ChainClient, batchInterval int) {

	txSet := generateTransactions(DataSkew, 1000000, TxCount)

	batchSize := 5000
	totalTx := len(txSet)

	for i := 0; i < totalTx; i += batchSize {
		end := i + batchSize
		if end > totalTx {
			end = totalTx
		}

		batch := txSet[i:end]
		var wg sync.WaitGroup
		fmt.Printf("begin deal with from %d to %d \n ", i, end)
		for _, tx := range batch {
			wg.Add(1)
			go func(tx Transaction) {
				defer wg.Done()
				err := testTransfer(client, tx.from, tx.to, 1, false)
				//err := testDeposit(client, tx.from, 1, false)
				if err != nil {
					fmt.Printf("transfer failed: %v", err)
					return
				}
			}(tx)
		}

		wg.Wait()
		for {
			s, _ := client.GetPoolStatus()
			if s.CommonTxNumInQueue+s.CommonTxNumInPending < 35000 {
				break
			} else {
				fmt.Printf("tx pool almost full %d\n", s.CommonTxNumInQueue+s.CommonTxNumInPending)
			}
			//time.Sleep(batchInterval * time.Second)
			time.Sleep(time.Duration(batchInterval) * time.Second)
		}
		//if i+batchSize < totalTx && i > 5000 && batchInterval > 0 {
		//      time.Sleep(batchInterval)
		//}
	}
}

func ship(client *sdk.ChainClient, batchInterval int) {
	// 修改为使用generateShipTransactions函数生成专用的ship交易数据
	txSet := generateShipTransactions(DataSkew, 1000000, TxCount)

	batchSize := 5000
	totalTx := len(txSet)

	for i := 0; i < totalTx; i += batchSize {
		end := i + batchSize
		if end > totalTx {
			end = totalTx
		}

		batch := txSet[i:end]
		var wg sync.WaitGroup
		fmt.Printf("begin send with from %d to %d \n ", i, end)
		// 修改为使用shipTransaction类型
		for _, tx := range batch {
			wg.Add(1)
			go func(tx shipTransaction) {
				defer wg.Done()
				// 随机选择执行loadCargo或unloadCargo操作
				if rand.Intn(2) == 0 {
					// 修改参数传递，使用shipTransaction的字段，并添加三个新参数
					err := testLoadCargo(client, tx.consignee, tx.shipper, tx.carrier, "1", "1", "1", false)
					if err != nil {
						fmt.Printf("load cargo failed: %v", err)
						return
					}
				} else {
					// 修改参数传递，使用shipTransaction的字段，并添加三个新参数
					err := testUnloadCargo(client, tx.consignee, tx.shipper, tx.carrier, "1", "1", "1", false)
					if err != nil {
						fmt.Printf("unload cargo failed: %v", err)
						return
					}
				}
			}(tx)
		}

		wg.Wait()
		s, _ := client.GetPoolStatus()
		fmt.Printf("send finish %d tx in poll\n", s.CommonTxNumInQueue+s.CommonTxNumInPending)
		for {
			s,_ = client.GetPoolStatus()
			if s.CommonTxNumInQueue+s.CommonTxNumInPending < 35000 {
				break
			} else {
				fmt.Printf("tx pool almost full %d\n", s.CommonTxNumInQueue+s.CommonTxNumInPending)
			}
			//time.Sleep(batchInterval * time.Second)
			time.Sleep(time.Duration(batchInterval) * time.Second)
		}
	}
}

func generateTransactions(dataSkew float64, accountCount int, txCount int) []Transaction {
	txSet := make([]Transaction, int(txCount))
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	z, _ := NewZipfian(r, dataSkew, accountCount)

	for i := 0; i < int(txCount); i++ {
		// 使用Zipf分布生成from账户
		from := int64(z.Uint64())
		// 确保from在有效范围内
		to := int64(z.Uint64())
		for to == from {
			to = int64(z.Uint64())
			// 确保to在有效范围内
		}

		toAccount := fmt.Sprintf("%09d", to)
		fromAccount := fmt.Sprintf("%09d", from)
		txSet[i] = Transaction{from: fromAccount, to: toAccount}
	}
	return txSet
}

func generateShipTransactions(dataSkew float64, accountCount int, txCount int) []shipTransaction {
	txSet := make([]shipTransaction, int(txCount))
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	z, _ := NewZipfian(r, dataSkew, accountCount)

	for i := 0; i < int(txCount); i++ {
		// 使用Zipf分布生成consignee账户
		consignee := int64(z.Uint64())
		// 确保consignee在有效范围内
		shipper := int64(z.Uint64())
		for shipper == consignee {
			shipper = int64(z.Uint64())
			// 确保shipper不与consignee重复
		}
		carrier := int64(z.Uint64())
		for carrier == consignee || carrier == shipper {
			carrier = int64(z.Uint64())
			// 确保carrier与consignee和shipper都不重复
		}

		consigneeAccount := fmt.Sprintf("%09d", consignee)
		shipperAccount := fmt.Sprintf("%09d", shipper)
		carrierAccount := fmt.Sprintf("%09d", carrier)

		txSet[i] = shipTransaction{consignee: consigneeAccount, shipper: shipperAccount, carrier: carrierAccount}
	}
	return txSet
}

func getHeight(client *sdk.ChainClient) int {
	blockinfo, _ := client.GetLastBlock(false)
	return int(blockinfo.GetBlock().Header.BlockHeight)
}

// Zipfian 支持 0 < s 的有限 Zipf 分布
type Zipfian struct {
	r   *rand.Rand
	s   float64
	n   int
	cdf []float64
}

// NewZipfian 创建有限 Zipf 分布
//
//	r   : *rand.Rand 实例
//	s   : 倾斜度 (0 <= s)
//	n   : 上界 (1 … n)
func NewZipfian(r *rand.Rand, s float64, n int) (*Zipfian, error) {
	if s < 0 || n <= 0 {
		return nil, errors.New("s must be non-negative and n must be positive")
	}
	z := &Zipfian{r: r, s: s, n: n}

	// 当s为0时，使用均匀分布，不需要计算CDF
	if s == 0 {
		return z, nil
	}

	z.cdf = make([]float64, n+1)
	// 计算调和数 H_{n,s}
	var sum float64
	for k := 1; k <= n; k++ {
		sum += math.Pow(float64(k), -s)
	}
	// 预计算 CDF
	for k := 1; k <= n; k++ {
		z.cdf[k] = z.cdf[k-1] + math.Pow(float64(k), -s)/sum
	}
	return z, nil
}

// Uint64 返回 1 … n 之间的随机整数
func (z *Zipfian) Uint64() uint64 {
	// 当s为0时，使用均匀分布
	if z.s == 0 {
		return uint64(z.r.Intn(z.n) + 1)
	}

	u := z.r.Float64()
	// 二分查找
	return uint64(sort.Search(z.n, func(i int) bool {
		return z.cdf[i+1] >= u
	}) + 1)
}

// 测试转账操作
func testTransfer(client *sdk.ChainClient, from, to string, amount int, withSyncResult bool) error {
	kvs := []*common.KeyValuePair{
		{
			Key:   "method",
			Value: []byte("transfer"),
		},
		{
			Key:   "from",
			Value: []byte(from),
		},
		{
			Key:   "to",
			Value: []byte(to),
		},
		{
			Key:   "amount",
			Value: []byte(strconv.Itoa(amount)),
		},
	}

	_, err := invokeContract(client, bankContractName, "invoke_contract", "", kvs, withSyncResult)
	if err != nil {
		return fmt.Errorf("transfer failed: %v", err)
	}
	//fmt.Printf("Transfer %d from %s to %s success", amount, from, to)
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

// 测试装载货物操作
func testLoadCargo(client *sdk.ChainClient, consignee, shipper, carrier, cargoAmount, freightAmount, goodsAmount string, withSyncResult bool) error {
	kvs := []*common.KeyValuePair{
		{
			Key:   "method",
			Value: []byte("loadCargo"),
		},
		{
			Key:   "consignee",
			Value: []byte(consignee),
		},
		{
			Key:   "shipper",
			Value: []byte(shipper),
		},
		{
			Key:   "carrier",
			Value: []byte(carrier),
		},
		{
			Key:   "cargo_amount",
			Value: []byte(cargoAmount),
		},
		{
			Key:   "freight_amount",
			Value: []byte(freightAmount),
		},
		{
			Key:   "goods_amount",
			Value: []byte(goodsAmount),
		},
	}

	_, err := invokeContract(client, bankContractName, "invoke_contract", "", kvs, withSyncResult)
	if err != nil {
		return fmt.Errorf("load cargo failed: %v", err)
	}
	return nil
}

// 测试卸载货物操作
func testUnloadCargo(client *sdk.ChainClient, consignee, shipper, carrier, cargoAmount, freightAmount, goodsAmount string, withSyncResult bool) error {
	kvs := []*common.KeyValuePair{
		{
			Key:   "method",
			Value: []byte("unloadCargo"),
		},
		{
			Key:   "consignee",
			Value: []byte(consignee),
		},
		{
			Key:   "shipper",
			Value: []byte(shipper),
		},
		{
			Key:   "carrier",
			Value: []byte(carrier),
		},
		{
			Key:   "cargo_amount",
			Value: []byte(cargoAmount),
		},
		{
			Key:   "freight_amount",
			Value: []byte(freightAmount),
		},
		{
			Key:   "goods_amount",
			Value: []byte(goodsAmount),
		},
	}

	_, err := invokeContract(client, bankContractName, "invoke_contract", "", kvs, withSyncResult)
	if err != nil {
		return fmt.Errorf("unload cargo failed: %v", err)
	}
	return nil
}

