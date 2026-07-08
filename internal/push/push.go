package push

import (
	"context"

	"github.com/openimsdk/open-im-server/v3/internal/push/offlinepush"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/cache/redis"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/controller"
	pbpush "github.com/openimsdk/protocol/push"
	"github.com/openimsdk/tools/db/redisutil"
	"github.com/openimsdk/tools/discovery"
	"google.golang.org/grpc"
)

type pushServer struct {
	pbpush.UnimplementedPushMsgServiceServer
	database      controller.PushDatabase
	disCov        discovery.SvcDiscoveryRegistry
	offlinePusher offlinepush.OfflinePusher
	pushCh        *ConsumerHandler // 这是一个关键的
	offlinePushCh *OfflinePushConsumerHandler
}

type Config struct {
	RpcConfig          config.Push
	RedisConfig        config.Redis
	KafkaConfig        config.Kafka
	NotificationConfig config.Notification
	Share              config.Share
	WebhooksConfig     config.Webhooks
	LocalCacheConfig   config.LocalCache
	Discovery          config.Discovery
	FcmConfigPath      string

	runTimeEnv string
}

func (p pushServer) DelUserPushToken(ctx context.Context,
	req *pbpush.DelUserPushTokenReq) (resp *pbpush.DelUserPushTokenResp, err error) {
	if err = p.database.DelFcmToken(ctx, req.UserID, int(req.PlatformID)); err != nil {
		return nil, err
	}
	return &pbpush.DelUserPushTokenResp{}, nil
}

// 我们看看 push 服务, 单独的一个 push 实例干了什么事情
func Start(ctx context.Context, config *Config, client discovery.SvcDiscoveryRegistry, server *grpc.Server) error {
	// 创建 一个 redis.UniversalClient
	rdb, err := redisutil.NewRedisClient(ctx, config.RedisConfig.Build())
	if err != nil {
		return err
	}
	// TODO: 等待阅读
	cacheModel := redis.NewThirdCache(rdb)

	//
	offlinePusher, err := offlinepush.NewOfflinePusher(&config.RpcConfig, cacheModel, config.FcmConfigPath)
	if err != nil {
		return err
	}

	//
	database := controller.NewPushDatabase(cacheModel, &config.KafkaConfig)

	// 专门用来处理 kafka ToPush Topic 消费得来的消息
	consumer, err := NewConsumerHandler(ctx, config, database, offlinePusher, rdb, client)
	if err != nil {
		return err
	}

	offlinePushConsumer, err := NewOfflinePushConsumerHandler(config, offlinePusher)
	if err != nil {
		return err
	}

	// 把 push-rpc 服务注册到 grpc.Server
	// 实际上就是 把 pushServer 的 各个 rpc 方法 调用名 -> handler 映射注册到
	// grpc.Server 中
	// grpc.Server 从 grpc.ClientConnInterface 中取 grpc 调用
	// 依据 注册的 handler 表路由到 具体的 rpc 方法, 并在方法前后进行 interceptor 包装

	// push rpc server 的 database(不仅仅是 MongoDB), 服务发现客户端, offlinePusher
	// kafka ToPush Topic 的消费组: push
	// kafka ToOfflinePush Topic 消费组
	// 这些会用到的都进行组合, 就可以调用 push rpc server 上的所有方法了
	pbpush.RegisterPushMsgServiceServer(server, &pushServer{
		database:      database,      // controller 层的 Database, 并非 database 层面的
		disCov:        client,        // 服务发现的 客户端
		offlinePusher: offlinePusher, //
		pushCh:        consumer,      // ConsumerHandler 内含 ConsumerGroup, ConsumerGroupHandler
		// C
		offlinePushCh: offlinePushConsumer,
	})

	// 启动一个 goroutine, 将 push 服务 handler 绑定给 MConsumerGroup
	// RegisterHandleAndConsumer 会用底层的 sarama.ConsumerGroup每次消费一个给到 push 服务的 ToPush 消费 handler
	// handler 中应该会将 从 kafka 取得的

	// 启动一个 push 服务的消费进程, 注册 ToPush Topic 消息的处理 handler
	go consumer.pushConsumerGroup.RegisterHandleAndConsumer(ctx, consumer)

	go offlinePushConsumer.OfflinePushConsumerGroup.RegisterHandleAndConsumer(ctx, offlinePushConsumer)

	return nil
}
