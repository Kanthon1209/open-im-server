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

package msgtransfer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/openimsdk/tools/discovery/etcd"
	"github.com/openimsdk/tools/utils/jsonutil"
	"github.com/openimsdk/tools/utils/network"

	"github.com/openimsdk/open-im-server/v3/pkg/common/prommetrics"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/cache/redis"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/database/mgo"
	"github.com/openimsdk/tools/db/mongoutil"
	"github.com/openimsdk/tools/db/redisutil"
	"github.com/openimsdk/tools/utils/datautil"

	conf "github.com/openimsdk/open-im-server/v3/pkg/common/config"
	discRegister "github.com/openimsdk/open-im-server/v3/pkg/common/discoveryregister"
	kdisc "github.com/openimsdk/open-im-server/v3/pkg/common/discoveryregister"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/controller"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mw"
	"github.com/openimsdk/tools/system/program"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type MsgTransfer struct {
	// This consumer aggregated messages, subscribed to the topic:toRedis,
	//  the message is stored in redis, Incr Redis, and then the message is sent to toPush topic for push,
	// and the message is sent to toMongo topic for persistence
	historyCH *OnlineHistoryRedisConsumerHandler
	//This consumer handle message to mongo
	historyMongoCH *OnlineHistoryMongoConsumerHandler
	ctx            context.Context
	cancel         context.CancelFunc
}

type Config struct {
	MsgTransfer    conf.MsgTransfer
	RedisConfig    conf.Redis
	MongodbConfig  conf.Mongo
	KafkaConfig    conf.Kafka
	Share          conf.Share
	WebhooksConfig conf.Webhooks
	Discovery      conf.Discovery
}

func Start(ctx context.Context, index int, config *Config) error {
	// 依赖 Config 启动 msgtransfer 实例

	log.CInfo(ctx, "MSG-TRANSFER server is initializing", "prometheusPorts",
		config.MsgTransfer.Prometheus.Ports, "index", index)

	mgocli, err := mongoutil.NewMongoDB(ctx, config.MongodbConfig.Build())
	if err != nil {
		return err
	}
	rdb, err := redisutil.NewRedisClient(ctx, config.RedisConfig.Build())
	if err != nil {
		return err
	}
	client, err := discRegister.NewDiscoveryRegister(&config.Discovery, &config.Share, nil)
	if err != nil {
		return err
	}
	client.AddOption(mw.GrpcClient(), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"LoadBalancingPolicy": "%s"}`, "round_robin")))

	msgDocModel, err := mgo.NewMsgMongo(mgocli.GetDB())
	if err != nil {
		return err
	}
	msgModel := redis.NewMsgCache(rdb, msgDocModel)
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
	// msgtransfer 服务的 controller 层 Database, 向上层
	msgTransferDatabase, err := controller.NewMsgTransferDatabase(msgDocModel, msgModel, seqUserCache, seqConversationCache, &config.KafkaConfig)
	if err != nil {
		return err
	}
	// history ConsumerHandler
	// 专门用来处理 ToRedis 的
	// Batcher 的 do 方法也是在构建 ConsumerHandler 的时候进行定义
	historyCH, err := NewOnlineHistoryRedisConsumerHandler(ctx, client, config, msgTransferDatabase)
	if err != nil {
		return err
	}
	historyMongoCH, err := NewOnlineHistoryMongoConsumerHandler(&config.KafkaConfig, msgTransferDatabase, config)
	if err != nil {
		return err
	}

	msgTransfer := &MsgTransfer{
		historyCH:      historyCH, // 是封装消费 ToRedis Topic ConsumerGroup 的 ConsumerHandler
		historyMongoCH: historyMongoCH,
	}
	return msgTransfer.Start(index, config) // ConsumerHandler 也只是写好了 ConsumeClaim
	// 把消费的 消息放到 Batcher 中
	// Batcher 会有一个单独的 goroutine 去跑主逻辑
	// Batcher 会被 ConsumerHandler 持有
}

func (m *MsgTransfer) Start(index int, cfg *Config) error {
	m.ctx, m.cancel = context.WithCancel(context.Background())
	var (
		netDone = make(chan struct{}, 1)
		netErr  error
	)

	// 三个 消费者 goroutine 绑定 Handler, 并开启消费循环
	// 用 msgtransfer 组件的 ToRedisTopicConsumerHandler 的 RegisterHandleAndConsumer 方法
	// 单独一个 goroutine,

	// 这些 业务特有的 ConsumerHandler, 都是 内有组合一个 MConsumerGroup
	// MConsumerGroup 内嵌的 ConsumerGroup, 方法提升得到 ConsumerGroup 的方法, Consume 等等
	// MConsumerGroup 在基础上提供了 RegisterHandleAndConsumer
	// 将对应的 Topic 的 ConsumerGroup 和对应的 handler 绑定到一起
	// 有意思的是, 这里, 业务 specific 的 ConsumerHandler 就是 ConsumerGroup.Consume() 方法中 要求的 ConsumerGroupHandler 接口的实现者
	// 而这个 ConsumerHandler 还组合 内嵌了 sarama.ConsumerGroup 的 MConsumerGroup,
	// 内嵌的实例的方法的参数是自己... 这样在逻辑上其实有点乱
	// 总之, 业务 specific 的 ConsumerHandler 实现了 ConsumerGroupHandler 要求的三个方法进行了实现
	go m.historyCH.historyConsumerGroup.RegisterHandleAndConsumer(m.ctx, m.historyCH)
	// 启动 消费循环, ConsumeClaim 中只是不断把
	go m.historyMongoCH.historyConsumerGroup.RegisterHandleAndConsumer(m.ctx, m.historyMongoCH)

	go m.historyCH.HandleUserHasReadSeqMessages(m.ctx)
	// 这个又是在干什么

	// 总的来说, Batcher.Start() 会检查 sharding 函数, Key 函数, 处理函数有没有设置好
	// 然后启动各个 Worker, 用 Batcher.WaitGroup
	// 各个 Worker 以 goroutine 方式启动
	// ** 在这里, 启动 Batcher 
	err := m.historyCH.redisMessageBatches.Start() // ConsumerGroup.Consume()
	// 只是不断把 ConsumerClaim 中消费的消息放到 Batcher 中
	// Batcher 需要启动一个消费循环处理
	if err != nil {
		return err
	}

	client, err := kdisc.NewDiscoveryRegister(&cfg.Discovery, &cfg.Share, nil)
	if err != nil {
		return errs.WrapMsg(err, "failed to register discovery service")
	}

	registerIP, err := network.GetRpcRegisterIP("")
	if err != nil {
		return err
	}

	getAutoPort := func() (net.Listener, int, error) {
		registerAddr := net.JoinHostPort(registerIP, "0")
		listener, err := net.Listen("tcp", registerAddr)
		if err != nil {
			return nil, 0, errs.WrapMsg(err, "listen err", "registerAddr", registerAddr)
		}
		_, portStr, _ := net.SplitHostPort(listener.Addr().String())
		port, _ := strconv.Atoi(portStr)
		return listener, port, nil
	}

	if cfg.MsgTransfer.Prometheus.AutoSetPorts && cfg.Discovery.Enable != conf.ETCD {
		return errs.New("only etcd support autoSetPorts", "RegisterName", "api").Wrap()
	}

	if cfg.MsgTransfer.Prometheus.Enable {
		var (
			listener       net.Listener
			prometheusPort int
		)

		if cfg.MsgTransfer.Prometheus.AutoSetPorts {
			listener, prometheusPort, err = getAutoPort()
			if err != nil {
				return err
			}

			etcdClient, ok := client.(*etcd.SvcDiscoveryRegistryImpl)
			if !ok {
				return errs.New("only etcd support autoSetPorts").Wrap()
			}

			target, err := jsonutil.JsonMarshal(prommetrics.BuildDefaultTarget(registerIP, prometheusPort))
			if err != nil {
				return errs.Wrap(err)
			}
			err = etcdClient.SetWithLease(context.TODO(), prommetrics.BuildDiscoveryKey(prommetrics.MessageTransferKeyName, index), target, prommetrics.TTL)
			if err != nil {
				return errs.WrapMsg(err, "etcd put err")
			}
		} else {
			prometheusPort, err = datautil.GetElemByIndex(cfg.MsgTransfer.Prometheus.Ports, index)
			if err != nil {
				return err
			}
			listener, err = net.Listen("tcp", fmt.Sprintf(":%d", prometheusPort))
			if err != nil {
				return errs.WrapMsg(err, "listen err", "addr", fmt.Sprintf(":%d", prometheusPort))
			}
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.ZPanic(m.ctx, "MsgTransfer Start Panic", errs.ErrPanic(r))
				}
			}()
			if err := prommetrics.TransferInit(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				netErr = errs.WrapMsg(err, "prometheus start error", "prometheusPort", prometheusPort)
				netDone <- struct{}{}
			}
		}()
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	select {
	case <-sigs:
		program.SIGTERMExit()
		// graceful close kafka client.
		m.cancel()
		m.historyCH.redisMessageBatches.Close()
		m.historyCH.Close()
		m.historyCH.historyConsumerGroup.Close()
		m.historyMongoCH.historyConsumerGroup.Close()
		return nil
	case <-netDone:
		m.cancel()
		m.historyCH.redisMessageBatches.Close()
		m.historyCH.Close()
		m.historyCH.historyConsumerGroup.Close()
		m.historyMongoCH.historyConsumerGroup.Close()
		close(netDone)
		return netErr
	}
}
