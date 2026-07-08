package push

import (
	"context"
	"sync"

	conf "github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/protocol/msggateway"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/discovery"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/utils/datautil"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type OnlinePusher interface {
	GetConnsAndOnlinePush(ctx context.Context, msg *sdkws.MsgData,
		pushToUserIDs []string) (wsResults []*msggateway.SingleMsgToUserResults, err error)
	GetOnlinePushFailedUserIDs(ctx context.Context, msg *sdkws.MsgData, wsResults []*msggateway.SingleMsgToUserResults,
		pushToUserIDs *[]string) []string
}

type emptyOnlinePusher struct{}

func newEmptyOnlinePusher() *emptyOnlinePusher {
	return &emptyOnlinePusher{}
}

func (emptyOnlinePusher) GetConnsAndOnlinePush(ctx context.Context, msg *sdkws.MsgData,
	pushToUserIDs []string) (wsResults []*msggateway.SingleMsgToUserResults, err error) {
	log.ZInfo(ctx, "emptyOnlinePusher GetConnsAndOnlinePush", nil)
	return nil, nil
}
func (u emptyOnlinePusher) GetOnlinePushFailedUserIDs(ctx context.Context, msg *sdkws.MsgData,
	wsResults []*msggateway.SingleMsgToUserResults, pushToUserIDs *[]string) []string {
	log.ZInfo(ctx, "emptyOnlinePusher GetOnlinePushFailedUserIDs", nil)
	return nil
}

func NewOnlinePusher(disCov discovery.SvcDiscoveryRegistry, config *Config) OnlinePusher {
	switch config.Discovery.Enable {
	// 根据配置文件的配置情况, 集群的实际部署情况, 选择一个 OnlinePusher
	case conf.KUBERNETES:
		// 但是这里对于 k8s 情况还是用的默认的全部节点都发
		return NewDefaultAllNode(disCov, config)
	case "zookeeper":
		return NewDefaultAllNode(disCov, config)
	case conf.ETCD:
		return NewDefaultAllNode(disCov, config)
	default:
		return newEmptyOnlinePusher()
	}
}

// OnlinePusher 的 默认实现, 就是拿到所有 msggateway 实例的连接并 push
type DefaultAllNode struct {
	disCov discovery.SvcDiscoveryRegistry
	config *Config
}

func NewDefaultAllNode(disCov discovery.SvcDiscoveryRegistry, config *Config) *DefaultAllNode {
	//
	return &DefaultAllNode{disCov: disCov, config: config}
}

func (d *DefaultAllNode) GetConnsAndOnlinePush(
	ctx context.Context,
	msg *sdkws.MsgData,
	pushToUserIDs []string) (wsResults []*msggateway.SingleMsgToUserResults, err error) {
	// 通过 svcDis client 去根据服务名拿到所有该服务pod的连接(grpc.ClientConnInterface接口的实现, 实际上就是 grpc.ClientConn)
	// 这个是 DefaultAllNode 对 OnlinePusher 接口中 GetConnsAndOnlinePush 方法的实现,
	// 直接通过 服务注册中心拿到 msggateway 的所有实例的连接
	// 但是对于 k8s 集群中, 有这么一种情况, msggateway 有多个 pod, 用户的长连接建立的时候通过 ID 等选择一个 msggateway 实例
	// 所以我们还可以有一个 k8s 中的 OnlinePuhser 实现方式, 特别是 GetConnsAndOnlinePush 这个方法
	// 可以按照相同的规则, 计算出目标用户slice都分布在哪些节点上, 只获取这些节点的连接
	conns, err := d.disCov.GetConns(ctx, d.config.Share.RpcRegisterName.MessageGateway)

	if len(conns) == 0 {
		log.ZWarn(ctx, "get gateway conn 0 ", nil)
	} else {
		log.ZDebug(ctx, "get gateway conn", "conn length", len(conns))
	}

	if err != nil {
		return nil, err
	}

	var (
		mu         sync.Mutex
		wg         = errgroup.Group{}
		input      = &msggateway.OnlineBatchPushOneMsgReq{MsgData: msg, PushToUserIDs: pushToUserIDs}
		maxWorkers = d.config.RpcConfig.MaxConcurrentWorkers
	)

	// 最小启动 3 个 worker
	if maxWorkers < 3 {
		maxWorkers = 3
	}

	wg.SetLimit(maxWorkers)

	// Online push message
	// 先把所有的 节点的 conn 都拿到, 这个连接的获取是通过 OnlinePusher
	for _, conn := range conns {
		conn := conn // loop var safe
		ctx := ctx
		// WaitGroup 的 Go 方法可以在其中把 Add, 和 done 操作给做了, 最多三个同时做?
		wg.Go(func() error {
			// 分别和 msggateway 建立其 MsgGatewayClient
			msgClient := msggateway.NewMsgGatewayClient(conn)
			// 把要推送的 msg 打包进 OnlineBatchPushOneMsgReq 推送给 msggateway server 端
			reply, err := msgClient.SuperGroupOnlineBatchPushOneMsg(ctx, input)
			if err != nil {
				log.ZError(ctx, "SuperGroupOnlineBatchPushOneMsg ", err, "req:", input.String())
				return nil
			}

			log.ZDebug(ctx, "push result", "reply", reply)
			if reply != nil && reply.SinglePushResult != nil {
				// 切片的 append 方法是线程不安全的
				// Go 中的 slice 并发写入存在数据竞争（Data Race），append 本身不保证并发安全。
				mu.Lock()
				// 将 reply.SinglePushResult 展开添加进入全局的 wsResults
				wsResults = append(wsResults, reply.SinglePushResult...)
				mu.Unlock()
			}

			return nil
		})
	}

	_ = wg.Wait()

	// always return nil
	return wsResults, nil
}

func (d *DefaultAllNode) GetOnlinePushFailedUserIDs(_ context.Context, msg *sdkws.MsgData,
	wsResults []*msggateway.SingleMsgToUserResults, pushToUserIDs *[]string) []string {

	onlineSuccessUserIDs := []string{msg.SendID}
	for _, v := range wsResults {
		//message sender do not need offline push
		if msg.SendID == v.UserID {
			continue
		}
		// mobile online push success
		if v.OnlinePush {
			onlineSuccessUserIDs = append(onlineSuccessUserIDs, v.UserID)
		}

	}

	return datautil.SliceSub(*pushToUserIDs, onlineSuccessUserIDs)
}

type K8sStaticConsistentHash struct {
	disCov discovery.SvcDiscoveryRegistry
	config *Config
}

func NewK8sStaticConsistentHash(disCov discovery.SvcDiscoveryRegistry, config *Config) *K8sStaticConsistentHash {
	return &K8sStaticConsistentHash{disCov: disCov, config: config}
}

func (k *K8sStaticConsistentHash) GetConnsAndOnlinePush(ctx context.Context, msg *sdkws.MsgData,
	pushToUserIDs []string) (wsResults []*msggateway.SingleMsgToUserResults, err error) {

	var usersHost = make(map[string][]string)
	for _, v := range pushToUserIDs {
		tHost, err := k.disCov.GetUserIdHashGatewayHost(ctx, v)
		if err != nil {
			log.ZError(ctx, "get msg gateway hash error", err)
			return nil, err
		}
		tUsers, tbl := usersHost[tHost]
		if tbl {
			tUsers = append(tUsers, v)
			usersHost[tHost] = tUsers
		} else {
			usersHost[tHost] = []string{v}
		}
	}
	log.ZDebug(ctx, "genUsers send hosts struct:", "usersHost", usersHost)
	var usersConns = make(map[grpc.ClientConnInterface][]string)
	for host, userIds := range usersHost {
		tconn, _ := k.disCov.GetConn(ctx, host)
		usersConns[tconn] = userIds
	}
	var (
		mu         sync.Mutex
		wg         = errgroup.Group{}
		maxWorkers = k.config.RpcConfig.MaxConcurrentWorkers
	)
	if maxWorkers < 3 {
		maxWorkers = 3
	}
	wg.SetLimit(maxWorkers)
	for conn, userIds := range usersConns {
		tcon := conn
		tuserIds := userIds
		wg.Go(func() error {
			input := &msggateway.OnlineBatchPushOneMsgReq{MsgData: msg, PushToUserIDs: tuserIds}
			msgClient := msggateway.NewMsgGatewayClient(tcon)
			reply, err := msgClient.SuperGroupOnlineBatchPushOneMsg(ctx, input)
			if err != nil {
				return nil
			}
			log.ZDebug(ctx, "push result", "reply", reply)
			if reply != nil && reply.SinglePushResult != nil {
				mu.Lock()
				wsResults = append(wsResults, reply.SinglePushResult...)
				mu.Unlock()
			}
			return nil
		})
	}
	_ = wg.Wait()
	return wsResults, nil
}
func (k *K8sStaticConsistentHash) GetOnlinePushFailedUserIDs(_ context.Context, _ *sdkws.MsgData,
	wsResults []*msggateway.SingleMsgToUserResults, _ *[]string) []string {
	var needOfflinePushUserIDs []string
	for _, v := range wsResults {
		if !v.OnlinePush {
			needOfflinePushUserIDs = append(needOfflinePushUserIDs, v.UserID)
		}
	}
	return needOfflinePushUserIDs
}
