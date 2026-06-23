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

package cmd

import (
	"context"

	"github.com/openimsdk/open-im-server/v3/internal/rpc/msg"
	"github.com/openimsdk/open-im-server/v3/pkg/common/startrpc"
	"github.com/openimsdk/open-im-server/v3/version"
	"github.com/openimsdk/tools/system/program"
	"github.com/spf13/cobra"
)

type MsgRpcCmd struct {
	*RootCmd
	ctx       context.Context
	configMap map[string]any
	msgConfig *msg.Config
}

func NewMsgRpcCmd() *MsgRpcCmd {
	// 封装 cobra.Command
	// 封装 解析配置映射到 msgConfig 的过程
	var msgConfig msg.Config // 
	ret := &MsgRpcCmd{msgConfig: &msgConfig}

	// 将 配置文件名 和 对应的结构体做一个映射
	ret.configMap = map[string]any{
		OpenIMRPCMsgCfgFileName:  &msgConfig.RpcConfig,
		RedisConfigFileName:      &msgConfig.RedisConfig,
		MongodbConfigFileName:    &msgConfig.MongodbConfig,
		KafkaConfigFileName:      &msgConfig.KafkaConfig,
		ShareFileName:            &msgConfig.Share,
		NotificationFileName:     &msgConfig.NotificationConfig,
		WebhooksConfigFileName:   &msgConfig.WebhooksConfig,
		LocalCacheConfigFileName: &msgConfig.LocalCacheConfig,
		DiscoveryConfigFilename:  &msgConfig.Discovery,
	}
	// 会创建一个 cobra.Command 会解析命令后执行回调解析配置文件将配置文件中的
	// 具体配置内容绑定到对应的结构体
	ret.RootCmd = NewRootCmd(program.GetProcessName(), WithConfigMap(ret.configMap))
	// 创建一个 Root Context, 给 context 中的 version 进行赋值
	ret.ctx = context.WithValue(context.Background(), "version", version.Version)
	// 将 cobra.Command 的 RunE 设置

	// ret.Command 其实是变量提升, 这个 是 RootCommand 的
	ret.Command.RunE = func(cmd *cobra.Command, args []string) error {
		return ret.runE() // 执行这个就会执行 Start()
	}
	return ret
}

func (a *MsgRpcCmd) Exec() error {
	return a.Execute() // 这里的 MsgRpcCmd 的 Execute 方法来自内嵌的 RootCmd 的方法提升
	// 实际上呢, 这个 方法是在执行 cobra.Command 的 Execute 方法, 于是会启动 cobra.Command 的命令解析,
	// persistentPreRunE 中进行 配置绑定到 configMap 中映射的 配置实例
	// RunE 中将 config 实例, msgServer 的启动函数, 等等都传入到 startrpc 这个 公共 Start 方法中
}

func (a *MsgRpcCmd) runE() error {
	// 这里和 msggateway 启动过程不一样, msggateway 直接就是 msggateway
	// 的启动主函数

	// runE 是由 cobra.Command 启动的

	// 运行顺序是 preRunE -> RunE
	// 实际上只是完成了配置的读取等没有实际进行实例的构建等
	return startrpc.Start(
		a.ctx,
		&a.msgConfig.Discovery,
		&a.msgConfig.RpcConfig.Prometheus,
		a.msgConfig.RpcConfig.RPC.ListenIP, // listenIP 是
		a.msgConfig.RpcConfig.RPC.RegisterIP,
		a.msgConfig.RpcConfig.RPC.AutoSetPorts,
		a.msgConfig.RpcConfig.RPC.Ports,
		a.Index(),
		a.msgConfig.Share.RpcRegisterName.Msg,
		&a.msgConfig.Share,
		a.msgConfig,
		nil,
		msg.Start, // ** 这里很重要, 是 msg 服务的正式入口函数
	)
}
