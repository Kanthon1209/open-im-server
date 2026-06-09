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

package kafka

import (
	"context"
	"errors"

	"github.com/IBM/sarama"
	"github.com/openimsdk/tools/log"
)

type MConsumerGroup struct {
	sarama.ConsumerGroup // 内嵌一个 interface 类型
	// 我们在初始化的时候, 对这个 内嵌的结构体进行赋值, 可以通过 去掉包名的类型名进行赋值
	groupID string
	topics  []string
}

// msgtransfer, push,
// msg -> mongo
// offlinepush
// push 都是在这里创建 ConsumerGroup

// msgtransfer 包中
// 1. ConsumerGroup 消费 ToRedis Topic, 入到 Redis 中, 并 Producer 生产到 ToMongo & ToPush 两个 Topic 中
// 2. ConsumerGroup 消费 ToMongo Topic 落库
// push 包中
// 1. ConsumerGroup 消费 ToPush Topic, 执行推送逻辑, rpc 发送到 msggateway 的 hub_server
// 如果所有客户端都离线, 那么用 sarama.Producer 生产到 ToOfflinePush Topic
// 2. ConsumerGroup 消费 ToOfflinePush Topic
func NewMConsumerGroup(conf *Config, groupID string, topics []string, autoCommitEnable bool) (*MConsumerGroup, error) {
	// build sarama.Config
	// sarama.Cofig 创建 sarama.ConsumerGroup, 嵌入到 MConsumerGroup
	// 这里会创建已给 sarama.Config 其中设置消费起始Offset ? 和 自动提交 offset 开关
	//
	config, err := BuildConsumerGroupConfig(conf, sarama.OffsetNewest, autoCommitEnable)
	if err != nil {
		return nil, err
	}
	// 其实就是 sarama.NewConsumerGroup 一层简单封装, 没有什么必要感觉
	group, err := NewConsumerGroup(config, conf.Addr, groupID)
	if err != nil {
		return nil, err
	}
	return &MConsumerGroup{
		ConsumerGroup: group, // 用类型名赋值内嵌结构体
		groupID:       groupID,
		topics:        topics,
	}, nil
}

// 从消息头里取出链路信息，还原成 context
func (mc *MConsumerGroup) GetContextFromMsg(cMsg *sarama.ConsumerMessage) context.Context {
	return GetContextWithMQHeader(cMsg.Headers)
}

// 主要是这个方法, 内嵌 ConsumerGroup 主要就是为了内嵌这个方法
// 再在这个 MConsumerGroup 外面套一个 特定业务的 ConsumeHandler
// ConsumeHandler 上 绑定 handler 方法需要的一切方法, 实现一个 handler 方法
// 通过 MConsumerGroup, 将 handler 传递到 sarama.ConsumerGroup.Consume 方法中
// 不断进行 Consume, 不断 JoinGroup ?
func (mc *MConsumerGroup) RegisterHandleAndConsumer(ctx context.Context, handler sarama.ConsumerGroupHandler) {
	// 把 handler 绑定到 sarama.ConsumerGroup

	for {
		// sarama.ConsumerGroup
		// MConsumerGroup 内嵌了 sarama.ConsumerGroup, 方法提升得到 Consume 方法
		err := mc.ConsumerGroup.Consume(ctx, mc.topics, handler)
		if errors.Is(err, sarama.ErrClosedConsumerGroup) {
			return
		}
		if errors.Is(err, context.Canceled) {
			return
		}
		if err != nil {
			log.ZWarn(ctx, "consume err", err, "topic", mc.topics, "groupID", mc.groupID)
		}
	}
}

// 关掉 sarama.ConsumerGroup
// 没有必要, 方法提升会得到这个方法, 我不理解, 这里为什么要重写这个方法
func (mc *MConsumerGroup) Close() error {
	return mc.ConsumerGroup.Close()
}
