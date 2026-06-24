// Copyright © 2023 OpenIM. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package msg

import (
	"context"

	"github.com/openimsdk/open-im-server/v3/pkg/notification"
	"github.com/openimsdk/open-im-server/v3/pkg/rpcli"

	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/cache/redis"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/database/mgo"
	"github.com/openimsdk/open-im-server/v3/pkg/common/webhook"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/db/mongoutil"
	"github.com/openimsdk/tools/db/redisutil"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/controller"
	"github.com/openimsdk/open-im-server/v3/pkg/rpccache"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/conversation"
	"github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/tools/discovery"
	"google.golang.org/grpc"
)

type MessageInterceptorFunc func(ctx context.Context, globalConfig *Config, req *msg.SendMsgReq) (*sdkws.MsgData, error)

// MessageInterceptorChain defines a chain of message interceptor functions.
type MessageInterceptorChain []MessageInterceptorFunc

type Config struct {
	RpcConfig config.Msg // 主要是这个, 这些 config 统一放在 pkg/common/config 目录
	// 到这个包中进行组合, 组合到 msg-rpc server 的 Config 结构体
	RedisConfig        config.Redis
	MongodbConfig      config.Mongo
	KafkaConfig        config.Kafka
	NotificationConfig config.Notification
	Share              config.Share
	WebhooksConfig     config.Webhooks
	LocalCacheConfig   config.LocalCache
	Discovery          config.Discovery
}

// msgServer 实现 protocol/msg 里 protoc-gen-go-grpc 生成的 MsgServer。
// 消息类型在 msg.pb.go，由 protoc-gen-go 生成。本文件只组装依赖并注册服务。
// 各个 RPC 方法按功能写在同包的其他文件里，例如 send.go、revoke.go。
//
// 必须按值嵌入 UnimplementedMsgServer。MsgServer 含有未导出方法
// mustEmbedUnimplementedMsgServer，别的包写不出这个方法，不嵌入就无法实现接口。
// 本包声明了同名方法的 RPC 会盖住空实现；没声明的仍走空实现，调用时返回 Unimplemented。
type msgServer struct {
	// 定义了 msgServer, 用于实现 MsgServer 接口的
	// 绑定的方法实现了 MsgServer 接口
	// 结构体中组合的字段是实现方法时需要用到的
	// 比较重要的是 MsgDatabase, controller.CommonMsgDatabase
	msg.UnimplementedMsgServer
	RegisterCenter discovery.SvcDiscoveryRegistry // 服务发现，Start 用它拿其他 RPC 的连接。
	// 这里 controller.CommonMsgDatabase 是一个接口
	// 只约定了 Msg Controller 要向上层提供什么方法
	// 具体实现不进行规定
	// 但是我们可以看到同一个包下有具体的实现, 用到了 database.Msg 接口(数据访问层接口) 实现是
	// 同目录下 mongo 包下的 MsgMgo 结构体
	MsgDatabase            controller.CommonMsgDatabase // 消息文档、序号缓存，以及发往 Kafka 的生产者。
	UserLocalCache         *rpccache.UserLocalCache     // 用户 RPC 的进程内缓存，Redis 只负责失效通知。
	FriendLocalCache       *rpccache.FriendLocalCache   // 好友关系 RPC 的进程内缓存。
	GroupLocalCache        *rpccache.GroupLocalCache    // 群 RPC 的进程内缓存。
	ConversationLocalCache *rpccache.ConversationLocalCache
	Handlers               MessageInterceptorChain
	notificationSender     *notification.NotificationSender // 发系统通知。直接调用本进程的 SendMsg，不走网络。
	msgNotificationSender  *MsgNotificationSender           // 撤回、已读、删除等消息类通知，同样直接调用 SendMsg。
	config                 *Config                          // 本包定义的 Config
	webhookClient          *webhook.Client
	conversationClient     *rpcli.ConversationClient // 会话 RPC。@ 消息时直接调用，不经过本地缓存。
}

func (m *msgServer) addInterceptorHandler(interceptorFunc ...MessageInterceptorFunc) {
	m.Handlers = append(m.Handlers, interceptorFunc...)

}

// Start 组装 msgServer 并注册到传入的 grpc.Server。它不把实例返回给调用方。
// 这里的 Conifg 是 msg server 定义的 Config
// 这里的 server 是 grpc.NewServer 创建的 Server
func Start(ctx context.Context, config *Config, client discovery.SvcDiscoveryRegistry, server *grpc.Server) error {
	// 我们要在创建 Mongo Client 的时候指明 是不是 ReplicaSet MongoMode
	// 注意, 这里传入进去的 是 Build() 过后的, Build() 过后的才是 mongoutil 中的 Config 类型
	// 多了 ReplicaSet 等 配置
	// 我们可以 Build() 后进行修改
	mgocli, err := mongoutil.NewMongoDB(ctx, config.MongodbConfig.Build())
	if err != nil {
		return err
	}
	rdb, err := redisutil.NewRedisClient(ctx, config.RedisConfig.Build())
	if err != nil {
		return err
	}
	// 消息正文在 Mongo。Redis 做消息缓存，后面挡着同一份 Mongo。
	msgDocModel, err := mgo.NewMsgMongo(mgocli.GetDB())
	if err != nil {
		return err
	}
	msgModel := redis.NewMsgCache(rdb, msgDocModel)
	// 下面两份 Mongo 存的是序号，不是会话服务的会话资料。
	seqConversation, err := mgo.NewSeqConversationMongo(mgocli.GetDB())
	if err != nil {
		return err
	}
	seqConversationCache := redis.NewSeqConversationCacheRedis(rdb, seqConversation)
	seqUser, err := mgo.NewSeqUserMongo(mgocli.GetDB())
	if err != nil {
		return err
	}
	seqUserCache := redis.NewSeqUserCacheRedis(rdb, seqUser)
	// MsgToMQ 使用这里的 Kafka 生产者，写入 ToRedisTopic。消费在 msgtransfer 进程。
	// 这个 msgDatabase 实际上持有很多实例
	// mongoDB 实例, redis等等
	// 只有 msg-rpc server 会用到 CommonMsgDatabase
	// msgDatabase 是 controller 层的
	// 内含 Kafka 等, 封装了一堆依赖 Kafka, Mongo DB 等服务的 函数, 向上层提供
	// rpc 这一层就会用到 controller 层封装好的 方法
	msgDatabase, err := controller.NewCommonMsgDatabase(msgDocModel, msgModel, seqUserCache, seqConversationCache, &config.KafkaConfig)
	if err != nil {
		return err
	}
	// 从 etcd / k8s 的注册中心实现 拿其他 RPC 的连接，再交给 pkg/rpcli 里的封装客户端。
	// k8s client getConn 返回的是 grpc.ClientConn,
	userConn, err := client.GetConn(ctx, config.Share.RpcRegisterName.User)
	if err != nil {
		return err
	}
	groupConn, err := client.GetConn(ctx, config.Share.RpcRegisterName.Group)
	if err != nil {
		return err
	}
	friendConn, err := client.GetConn(ctx, config.Share.RpcRegisterName.Friend)
	if err != nil {
		return err
	}
	conversationConn, err := client.GetConn(ctx, config.Share.RpcRegisterName.Conversation)
	if err != nil {
		return err
	}
	conversationClient := rpcli.NewConversationClient(conversationConn)
	// 本地缓存的数据在本进程内存里。Redis 只用来订阅删除通知，其他服务改数据时让这里丢掉缓存。

	// 部分 rpc 服务 client 端外面套了一层 cache, 看看是什么情况
	s := &msgServer{
		MsgDatabase:            msgDatabase,
		RegisterCenter:         client,
		UserLocalCache:         rpccache.NewUserLocalCache(rpcli.NewUserClient(userConn), &config.LocalCacheConfig, rdb),
		GroupLocalCache:        rpccache.NewGroupLocalCache(rpcli.NewGroupClient(groupConn), &config.LocalCacheConfig, rdb),
		ConversationLocalCache: rpccache.NewConversationLocalCache(conversationClient, &config.LocalCacheConfig, rdb),
		FriendLocalCache:       rpccache.NewFriendLocalCache(rpcli.NewRelationClient(friendConn), &config.LocalCacheConfig, rdb),
		config:                 config,
		webhookClient:          webhook.NewWebhookClient(config.WebhooksConfig.URL),
		conversationClient:     conversationClient,
	}

	// 本服务发出的通知直接调用 s.SendMsg，不经过 gRPC。
	s.notificationSender = notification.NewNotificationSender(&config.NotificationConfig, notification.WithLocalSendMsg(s.SendMsg))
	s.msgNotificationSender = NewMsgNotificationSender(config, notification.WithLocalSendMsg(s.SendMsg))

	// 将 msgServer 注册到 grpc.Server , 这里还是没有启动
	msg.RegisterMsgServer(server, s)

	return nil
}

// conversationAndGetRecvID 从会话里取出消息接收方。单聊和通知会话取对话的另一方，群聊取群 ID。
func (m *msgServer) conversationAndGetRecvID(conversation *conversation.Conversation, userID string) string {
	if conversation.ConversationType == constant.SingleChatType ||
		conversation.ConversationType == constant.NotificationChatType {
		if userID == conversation.OwnerUserID {
			return conversation.UserID
		} else {
			return conversation.OwnerUserID
		}
	} else if conversation.ConversationType == constant.ReadGroupChatType {
		return conversation.GroupID
	}
	return ""
}
