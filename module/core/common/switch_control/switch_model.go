package switch_control

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"

	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
)

type ModelParams struct {
	FeatureNames []string    // 特征名称（与系数顺序对应）
	Means        []float64   // 特征均值（用于标准化）
	Stds         []float64   // 特征标准差（用于标准化）
	Coefs        [][]float64 // 系数矩阵：[算法索引][特征索引]
	Intercepts   []float64   // 截距项：[算法索引]
	AlgoNames    []ControlType    // 算法名称映射
}

var GlobalModel = ModelParams{
	FeatureNames: []string{
		"num_vertices",        // x0
		"num_edges",           // x1
		"density",             // x2
		"avg_in_degree",       // x3
		"avg_out_degree",      // x4
		"max_in_degree",       // x5
		"max_out_degree",      // x6
		"longest_path_length", // x7（均为0）
		"topo_order_length",   // x8（均为0）
	},
	Means: []float64{4940.28125, 2640.9375, 0.00010595939999024887, 0.528841961408034, 0.528841961408034, 1.6875, 1.78125, 0.0, 0.0},
	Stds:  []float64{447.8283526067074, 2764.9903304249997, 0.00011072018437734551, 0.5532930105378792, 0.5532930105378792, 0.46351240544347894, 0.41339864235384227, 1.0, 1.0},
	Coefs: [][]float64{
		{0.16253380073035775, -0.363648031507852, -0.3654434943759953, -0.36430789201973934, -0.36430789201973934, -0.1918014428190454, -0.21769480937294608, 0.0, 0.0},
		{-0.1686255534347498, -0.9570744571022328, -0.9556862984179303, -0.9564978214541238, -0.9564978214541238, -0.3440967372079361, -0.06785798559165754, 0.0, 0.0},
		{0.10721790925085396, 0.2065100534020053, 0.17767953889624646, 0.19239688677575628, 0.19239688677575628, 0.5156148799810769, 0.27142317207926386, 0.0, 0.0},
		{-0.10112615654646209, 1.1142124352080796, 1.1434502538976794, 1.128408826698107, 1.128408826698107, 0.02028330004590362, 0.01412962288533881, 0.0, 0.0},
	},
	Intercepts: []float64{-0.018464373191848755, -0.8960187300376167, 0.7884847363385492, 0.12599836689091418},
	AlgoNames:  []ControlType{DEFAULTControl, BatchControl, PartDAGControl, StaleControl},
}

func ExtractDAGFeaturesParallel(dag *commonPb.DAG) ([]float64) {
	vertexes := dag.Vertexes
	numVertices := len(vertexes)
	if numVertices == 0 {
		return nil
	}

	// 初始化入度（原子类型，避免跨分片竞争）、出度（无竞争）
	inDegree := make([]atomic.Int32, numVertices)
	outDegree := make([]int, numVertices)
	var edgesAtomic atomic.Int32 // 总边数

	// 并行任务1：计算出度和总边数
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		localEdges := 0
		for i, v := range vertexes {
			outDeg := len(v.Neighbors)
			outDegree[i] = outDeg
			localEdges += outDeg
		}
		edgesAtomic.Add(int32(localEdges))
	}()

	// 并行任务2：分片计算入度
	wg.Add(1)
	go func() {
		defer wg.Done()
		shardSize := (int(numVertices) + runtime.NumCPU() - 1) / runtime.NumCPU()
		var shardWg sync.WaitGroup
		for s := 0; s < runtime.NumCPU(); s++ {
			start, end := s*shardSize, (s+1)*shardSize
			if end > numVertices {
				end = numVertices
			}
			shardWg.Add(1)
			go func(s, e int) {
				defer shardWg.Done()
				for i := s; i < e; i++ {
					for _, neighborIdx := range vertexes[i].Neighbors {
						if neighborIdx >= 0 && int(neighborIdx) < numVertices {
							inDegree[neighborIdx].Add(1) // 原子累加入度
						}
					}
				}
			}(start, end)
		}
		shardWg.Wait()
	}()

	wg.Wait()
	numEdges := int(edgesAtomic.Load())

	// 计算最终特征（顺序严格匹配ModelParams）
	features := make([]float64, 9)
	features[0] = float64(numVertices) // num_vertices
	features[1] = float64(numEdges)    // num_edges
	if numVertices > 1 {
		features[2] = float64(numEdges) / (float64(numVertices) * float64(numVertices-1)) // density
	}
	// 计算平均入度、最大入度
	sumIn, maxIn := 0, 0
	for _, d := range inDegree {
		val := int(d.Load())
		sumIn += val
		if val > maxIn {
			maxIn = val
		}
	}
	features[3] = float64(sumIn) / float64(numVertices) // avg_in_degree
	features[5] = float64(maxIn)                        // max_in_degree
	// 计算平均出度、最大出度
	sumOut, maxOut := 0, 0
	for _, d := range outDegree {
		sumOut += d
		if d > maxOut {
			maxOut = d
		}
	}
	features[4] = float64(sumOut) / float64(numVertices) // avg_out_degree
	features[6] = float64(maxOut)                        // max_out_degree
	features[7], features[8] = 0.0, 0.0

	return features
}

func DeriveAlgorithm(dag *commonPb.DAG) (ControlType) {
	// 步骤1：并行提取DAG特征
	features := ExtractDAGFeaturesParallel(dag)

	// 步骤2：特征标准化
	scaledFeatures := make([]float64, len(features))
	for i := range features {
		if GlobalModel.Stds[i] < 1e-10 {
			scaledFeatures[i] = 0.0
			continue
		}
		scaledFeatures[i] = (features[i] - GlobalModel.Means[i]) / GlobalModel.Stds[i]
	}

	// 步骤3：并行计算4个算法得分
	scores := make(map[ControlType]float64, 4)
	for algoIdx := range GlobalModel.AlgoNames {
		algoName := GlobalModel.AlgoNames[algoIdx]
		score := GlobalModel.Intercepts[algoIdx]
		for featIdx := range scaledFeatures {
			score += GlobalModel.Coefs[algoIdx][featIdx] * scaledFeatures[featIdx]
		}
		scores[algoName] = score
	}

	// 步骤4：选择最高分算法
	maxScore := math.Inf(-1)
	bestAlgo := DEFAULTControl
	for algo, score := range scores {
		if score > maxScore {
			maxScore = score
			bestAlgo = algo
		}
	}

	return bestAlgo
}