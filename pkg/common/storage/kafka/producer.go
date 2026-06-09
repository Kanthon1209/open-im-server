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

	"github.com/IBM/sarama"
	"github.com/openimsdk/tools/errs"
	"google.golang.org/protobuf/proto"
)

// Producer represents a Kafka producer.
// 包装一个 sarama.SyncProducer
type Producer struct {
	addr     []string
	topic    string
	config   *sarama.Config
	producer sarama.SyncProducer
}

func NewKafkaProducer(config *sarama.Config, addr []string, topic string) (*Producer, error) {
	// sarama.NewSyncProducer 去创建一个 连接到 kafka 的
	producer, err := NewProducer(config, addr)
	if err != nil {
		return nil, err
	}

	// 还是进行了一层的封装, 封装了 将
	return &Producer{
		addr:     addr,
		topic:    topic,
		config:   config,
		producer: producer,
	}, nil
}

// SendMessage sends a message to the Kafka topic configured in the Producer.
func (p *Producer) SendMessage(ctx context.Context, key string, msg proto.Message) (int32, int64, error) {
	// 注意, 这里是生产端, 消费端才会有 ConsumerGroup, 才会连接 Coordinator
	// 进行 JoinGroup, rebalance, JoinGroup, SyncGroup
	// 和 partition leader replica 所在 broker 建立连接, 进行 Fetch
	// 拿到 HW ( leader partition replica 内存中) 前一个 批次的 消息 放到 claim.Messages() 这个 channel 中
	// 生产端, 用 bootstrap broker 拿到 meta data 就行了? 给对应 partition replica leader 发送消息
	// follower replica 进行复制

	// Marshal the protobuf message
	// 将 msg Marshal 到 []byte
	bMsg, err := proto.Marshal(msg)
	// 将 sdkws.MsgData 变成 []byte

	if err != nil {
		return 0, 0, errs.WrapMsg(err, "kafka proto Marshal err")
	}
	if len(bMsg) == 0 {
		return 0, 0, errs.WrapMsg(errEmptyMsg, "kafka proto Marshal err")
	}

	// Prepare Kafka message
	// 设置二进制消息对应的 key
	kMsg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(key),
		// StringEncoder 底层数据就是 string
		// type StringEncoder string 定义的是一个新类型, 新的方法绑定在这个新类型上
		Value: sarama.ByteEncoder(bMsg),
	}

	// Validate message key and value
	if kMsg.Key.Length() == 0 || kMsg.Value.Length() == 0 {
		return 0, 0, errs.Wrap(errEmptyMsg)
	}

	// Attach context metadata as headers
	header, err := GetMQHeaderWithContext(ctx)
	if err != nil {
		return 0, 0, err
	}
	// 将 sarama.ProducerMessage.Header 设置为 ?
	kMsg.Headers = header

	// Send the message
	// 生产消息到对应的 Topic 中, 并得到响应, 知道我们的消息到了哪一个分区, 以及位于该分区的 offset
	partition, offset, err := p.producer.SendMessage(kMsg)
	if err != nil {
		return 0, 0, errs.WrapMsg(err, "p.producer.SendMessage error")
	}

	return partition, offset, nil
}
