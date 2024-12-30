module tbft

go 1.15

require (
	chainmaker.org/chainmaker/chainconf/v2 v2.3.4
	chainmaker.org/chainmaker/common/v2 v2.3.4
	chainmaker.org/chainmaker/consensus-utils/v2 v2.3.5
	chainmaker.org/chainmaker/localconf/v2 v2.3.4
	chainmaker.org/chainmaker/logger/v2 v2.3.4
	chainmaker.org/chainmaker/lws v1.2.1
	chainmaker.org/chainmaker/pb-go/v2 v2.3.5
	chainmaker.org/chainmaker/protocol/v2 v2.3.5
	chainmaker.org/chainmaker/utils/v2 v2.3.5
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/prometheus/client_golang v1.9.0
	github.com/stretchr/testify v1.8.0
	go.uber.org/zap v1.19.1
)

replace (
	github.com/libp2p/go-libp2p-core => chainmaker.org/chainmaker/libp2p-core v1.0.0
	github.com/lucas-clemente/quic-go v0.26.0 => chainmaker.org/third_party/quic-go v1.2.2
	github.com/marten-seemann/qtls-go1-16 => chainmaker.org/third_party/qtls-go1-16 v1.1.0
	github.com/marten-seemann/qtls-go1-17 => chainmaker.org/third_party/qtls-go1-17 v1.1.0
	github.com/marten-seemann/qtls-go1-18 => chainmaker.org/third_party/qtls-go1-18 v1.1.0
	github.com/marten-seemann/qtls-go1-19 => chainmaker.org/third_party/qtls-go1-19 v1.0.0
	google.golang.org/grpc => google.golang.org/grpc v1.26.0
)
