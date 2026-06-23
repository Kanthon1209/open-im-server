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

package startrpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"syscall"
	"time"

	conf "github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/tools/discovery/etcd"
	"github.com/openimsdk/tools/utils/datautil"
	"github.com/openimsdk/tools/utils/jsonutil"
	"google.golang.org/grpc/status"

	kdisc "github.com/openimsdk/open-im-server/v3/pkg/common/discoveryregister"
	"github.com/openimsdk/open-im-server/v3/pkg/common/prommetrics"
	"github.com/openimsdk/tools/discovery"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mw"
	"github.com/openimsdk/tools/utils/network"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func getConfigRpcMaxRequestBody(value reflect.Value) *conf.MaxRequestBody {
	for value.Kind() == reflect.Pointer {
		value = value.Elem()
	}
	if value.Kind() == reflect.Struct {
		num := value.NumField()
		for i := 0; i < num; i++ {
			field := value.Field(i)
			if !field.CanInterface() {
				continue
			}
			for field.Kind() == reflect.Pointer {
				field = field.Elem()
			}
			switch elem := field.Interface().(type) {
			case conf.Share:
				return &elem.RPCMaxBodySize
			case conf.MaxRequestBody:
				return &elem
			}
			if field.Kind() == reflect.Struct {
				if elem := getConfigRpcMaxRequestBody(field); elem != nil {
					return elem
				}
			}
		}
	}
	return nil
}

// Start rpc server.
func Start[T any]( // 这里的泛型是为了限制 config T 必须和 rpcFn 中的的 config T 是同一个
	ctx context.Context,
	discovery *conf.Discovery,
	prometheusConfig *conf.Prometheus,
	listenIP, registerIP string, // 可以多个参数公用一个 类型
	autoSetPorts bool,
	rpcPorts []int,
	index int,
	rpcRegisterName string,
	share *conf.Share,
	config T,
	watchServiceNames []string,
	rpcFn func(
		ctx context.Context,
		config T,
		client discovery.SvcDiscoveryRegistry,
		server *grpc.Server) error, options ...grpc.ServerOption) error {

	// 使用到 读取到的配置进行一些配置

	// options 是本进程 gRPC 服务端的选项，创建 grpc.Server 时使用。
	// clientOptions 是本进程作为客户端去调用其他服务时的拨号选项。
	// 两边都要限制，否则只限制了别人打进来的请求，自己打出去的请求仍然不受限。

	// 这里是在准备 grpc.Client 以及 grpc.Server 的配置
	maxRequestBody := &share.RPCMaxBodySize
	var clientOptions []grpc.DialOption
	if maxRequestBody != nil {
		// 请求体上限：本服务最多接收多大，以及向外发送时最多发多大。
		if maxRequestBody.RequestMaxBodySize > 0 {
			options = append(options, grpc.MaxRecvMsgSize(maxRequestBody.RequestMaxBodySize))
			clientOptions = append(clientOptions, grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxRequestBody.RequestMaxBodySize)))
		}
		// 响应体上限：本服务最多发出多大的响应，以及调用别人时最多接收多大的响应。
		if maxRequestBody.ResponseMaxBodySize > 0 {
			options = append(options, grpc.MaxSendMsgSize(maxRequestBody.ResponseMaxBodySize))
			clientOptions = append(clientOptions, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRequestBody.ResponseMaxBodySize)))
		}
	}

	var (
		rpcTcpAddr     string
		netDone        = make(chan struct{}, 2) // 容量 2：业务端口和 Prometheus 端口各可以投递一次启动失败。
		netErr         error
		prometheusPort int
	)

	// 配置里的注册 IP 可能为空。空则选出一块本机网卡地址，供其他进程连接。
	registerIP, err := network.GetRpcRegisterIP(registerIP)
	if err != nil {
		return err
	}

	// autoSetPorts 为假时使用配置里的端口列表，index 来自命令行 -i，多实例时各取一项。
	// 为真时端口写 0，Listen 时由操作系统分配空闲端口。
	if !autoSetPorts {
		// 这个是从哪里读取端口
		rpcPort, err := datautil.GetElemByIndex(rpcPorts, index)
		if err != nil {
			return err
		}
		// 就是判断一下是不是空字符, 如果是空字符就用 0.0.0.0
		rpcTcpAddr = net.JoinHostPort(network.GetListenIP(listenIP), strconv.Itoa(rpcPort))
	} else {
		// 自动设置端口的时候, 把端口写成 0
		rpcTcpAddr = net.JoinHostPort(network.GetListenIP(listenIP), "0")
	}

	// 在 rpcTcpAddr 上监听，并返回操作系统实际绑定的端口。
	// 端口写 0 时，返回值和配置里的 0 不同，后面注册服务发现要用这个实际端口。
	getAutoPort := func() (net.Listener, int, error) {
		// 端口号 为 0 的时候由操作系统内核随机分配一个端口
		// 这里是闭包函数啊, 这里把 rpcTcpAddr 捕获了

		// 这里是在本地网络进行Listen, 本地可能有多个网卡, 所以要配置 listen ip
		listener, err := net.Listen("tcp", rpcTcpAddr)
		if err != nil {
			return nil, 0, errs.WrapMsg(err, "listen err", "rpcTcpAddr", rpcTcpAddr)
		}
		_, portStr, _ := net.SplitHostPort(listener.Addr().String())
		port, _ := strconv.Atoi(portStr)
		return listener, port, nil
	}

	// 自动端口要写回服务发现，当前只有 etcd 实现了带租约的写入。
	if autoSetPorts && discovery.Enable != conf.ETCD {
		return errs.New("only etcd support autoSetPorts", "rpcRegisterName", rpcRegisterName).Wrap()
	}
	// watchServiceNames 是启动时就要订阅的其他服务。消息服务传入 nil，不在这里额外订阅。
	// client 是
	// 总之, 这里会启动一个 服务注册中心
	// GetConn(服务名)：按名字要一条 gRPC 连接。Kubernetes 下拨 kubernetes:///服务名，解析成 Pod 的 IP:端口。
	// * GetConn 返回的 ClientConn 其中的解析器会启动一个watch服务去维护 ClientConn 中每一个 http2client
	// *  | ClientConn 中并非只有一个 连接, GetConn 按服务名返回的 ClientConn 封装了负载均衡和 watch
	// *  | 会有一个 k8sclient 和 api-server 建立连接, watch 对应服务的 service 的 endpoints
	// *  | 有变更会调用回调, 调用 ClientConn 的 UpdateStatus 让 ClientConn 重建到 pod 的 http2client 连接
	// GetConns(服务名)：要这个服务全部 Pod 的直连。watchEndpoints 维护的 connsMap 就是给它用的。
	// Register(服务名, IP, 端口)：把自己的地址登记出去。etcd 会写入；Kubernetes 的实现是空的，发现靠 Service。
	// AddOption：给之后拨号追加 gRPC 选项，例如包体大小和轮询。
	// Close：关掉它持有的连接。
	// IsSelfNode：判断一条连接是不是连到本 Pod。
	// GetUserIdHashGatewayHost：按用户 ID 找网关地址。这是 etcd 上的用法。

	// 根据配置 创建一个 k8s 实现或者一个 etcd 实现的 服务注册中心
	client, err := kdisc.NewDiscoveryRegister(discovery, share, watchServiceNames)
	if err != nil {
		return err
	}

	defer client.Close()
	// 本进程发出的 gRPC 调用：客户端拦截器、明文传输、按轮询在多个实例间分配。

	//
	client.AddOption(
		mw.GrpcClient(), // 增加 interceptor, 日志等功能
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"LoadBalancingPolicy": "%s"}`, "round_robin")))

	if len(clientOptions) > 0 {
		client.AddOption(clientOptions...)
	}

	// var reg *prometheus.Registry
	// var metric *grpcprometheus.ServerMetrics
	// 根据配置是否启动 prom 服务器, 并将对 rpc 调用进行收集的 Collector 更新放到 grpc 调用的 interceptor 中
	if prometheusConfig.Enable {
		// cusMetrics := prommetrics.GetGrpcCusMetrics(rpcRegisterName, share)
		// reg, metric, _ = prommetrics.NewGrpcPromObj(cusMetrics)
		// options = append(options, mw.GrpcServer(), grpc.StreamInterceptor(metric.StreamServerInterceptor()),
		//	grpc.UnaryInterceptor(metric.UnaryServerInterceptor()))

		// 服务端拦截器：公共中间件，再加上按方法名统计调用次数和状态码。
		// 这个不是给 prom 的 服务器用的, 是我们启动了 prometheus 的话
		// 就要在 grpc.Server 上应用一些配置, grpc.Dial 应用这些
		options = append(
			options,
			mw.GrpcServer(), // server 端的 option
			prommetricsUnaryInterceptor(rpcRegisterName),  // 会将 RPC 调用情况放到 prom 中
			prommetricsStreamInterceptor(rpcRegisterName), // Stream 版本的 grpc 调用这里的 interceptor 是空实现, 不做操作
		)

		var (
			listener net.Listener // 用来创建 TCPConn 的, 是 net.Conn 的实现
		)

		if autoSetPorts {
			// 从系统要一个 随机的 Port
			listener, prometheusPort, err = getAutoPort() // 获取 net.Listener
			if err != nil {
				return err
			}
			// 进行类型断言
			etcdClient, ok := client.(*etcd.SvcDiscoveryRegistryImpl) // 类型断言
			if !ok {
				return errs.New("only etcd support autoSetPorts").Wrap()
			}

			// 指标端口也是临时分配的，把抓取地址写进 etcd，并带上租约，进程退出后自动过期。
			target, err := jsonutil.JsonMarshal(prommetrics.BuildDefaultTarget(registerIP, prometheusPort))
			if err != nil {
				return errs.Wrap(err)
			}
			err = etcdClient.SetWithLease(ctx, prommetrics.BuildDiscoveryKey(rpcRegisterName, index), target, prommetrics.TTL)
			if err != nil {
				return errs.WrapMsg(err, "etcd put err")
			}
		} else {
			// 使用配置里的指标端口。同一个 index 对应同一套 RPC 端口和指标端口。
			prometheusPort, err = datautil.GetElemByIndex(prometheusConfig.Ports, index)
			if err != nil {
				return err
			}
			// 监听 tcp
			listener, err = net.Listen("tcp", fmt.Sprintf(":%d", prometheusPort))
			if err != nil {
				return errs.WrapMsg(err, "listen err", "rpcTcpAddr", rpcTcpAddr)
			}
		}

		// 这里是通用的启动流程中, 我们根据当前的服务名判断要给当前服务的 prometheus 服务器增加什么
		// cs 就是 collectors
		cs := prommetrics.GetGrpcCusMetrics(rpcRegisterName, share)
		// 指标 HTTP 服务单独跑。失败时通知下面的 select，让 Start 把错误返回给调用方。

		// 这里正式启动 prom 服务器, 一个 server, 将服务的 指标 注册进 registry, 给 handler 处理返回给 调用方
		go func() {
			if err := prommetrics.RpcInit(cs, listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				netErr = errs.WrapMsg(err, fmt.Sprintf("rpc %s prometheus start err: %d", rpcRegisterName, prometheusPort))
				netDone <- struct{}{}
			}
			//metric.InitializeMetrics(srv)
			// Create a HTTP server for prometheus.
			// httpServer = &http.Server{Handler: promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), Addr: fmt.Sprintf("0.0.0.0:%d", prometheusPort)}
			// if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			//	netErr = errs.WrapMsg(err, "prometheus start err", httpServer.Addr)
			//	netDone <- struct{}{}
			// }
		}()
	} else {
		// 只有启动了 Prometheus 才会挂载 UnaryInterceptor 和 StreamInterceptor
		options = append(options, mw.GrpcServer())
		// 没有启动 prom, 这里就空空的, 不会获取 Collector, 不会获取端口, 不会去给 服务端 加上 interceptor
		//
	}

	// 监听业务 RPC 端口。这次返回的 port 才是要注册到服务发现的端口。
	// 看看本服务要绑定到什么 ip, 什么端口, 获取对应的 net.Listener
	listener, port, err := getAutoPort()
	if err != nil {
		return err
	}

	log.CInfo(ctx, "RPC server is initializing", "rpcRegisterName", rpcRegisterName, "rpcPort", port,
		"prometheusPort", prometheusPort)

	defer listener.Close()
	srv := grpc.NewServer(options...)
	// grpc.Server.Serve(Listener) 才会启动

	// 各服务自己的启动逻辑。消息服务的 rpcFn 是 msg.Start：
	// 连接 Mongo、Redis，创建 Kafka 生产者，组装 msgServer，并 RegisterMsgServer。

	// 将各个服务的 服务端grpc方法注册到 grpc.Server 中
	// 比如对于 msg-rpc, 构建 MsgServer, 注册到 grpc.Server
	// 然后 grpc.Server.Serve(Listener) 来真正启动
	// 利用 svcdisCient 去创建 msg-server 等 必要的组成组件, 然后注册到 grpc.Server 上
	// 这里这个 client 是 discovery.SvcDiscoveryRegistry 用来服务发现的
	err = rpcFn(ctx, config, client, srv) // 这里其实还没有正式启动, 这里做的是把服务组装好
	// 挂载到已经建好的 grpc.Server 上
	// 但是 grpc.Server 的启动还要 一个 Listener
	if err != nil {
		return err
	}

	// 业务服务注册完成后，其他进程才能按 rpcRegisterName 发现这个实例。
	// 注册用 registerIP，监听用的是 listenIP；两者可以不同，例如监听 0.0.0.0、注册内网地址。

	// 将启动的服务注册到 服务注册中心
	// k8s client Register 方法实现是空的, 因为k8s 情况下不需要进程主动上报, k8s 会分配 pod, 启动服务
	//
	err = client.Register(
		ctx,
		rpcRegisterName,
		registerIP,
		port,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}

	// Serve 会阻塞，所以放到单独的 goroutine。当前 goroutine 留在下面等退出信号。
	go func() {
		err := srv.Serve(listener) // 正式启动
		// 对 listener 进行 Accept
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			netErr = errs.WrapMsg(err, "rpc start err: ", rpcTcpAddr)
			netDone <- struct{}{}
		}
	}()

	// ---- ↓ 优雅退出 ↓ ----
	// 收到 SIGTERM 时最多等 10 秒处理完已接入的 RPC，然后正常返回。
	// 业务端口或 Prometheus 启动失败则把 netErr 返回给 runE。

	sigs := make(chan os.Signal, 1) // 创建一个 os.Signal 类型的 channel
	signal.Notify(sigs, syscall.SIGTERM)

	select { // 一次等待
	// 这里两个 case 都会挂到 对应的 channel 的 waitq 上
	// 一旦 close channel, 会到 waitq 上告诉所有 等待的不用等了
	case <-sigs: // 系统传 syscall.SIGTERM 才会走这里
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		// ctx 最多 10 秒就会给 Done() 进行 close 操作
		defer cancel()
		if err := gracefulStopWithCtx(ctx, srv.GracefulStop); err != nil {
			return err
		}
		return nil
	case <-netDone: // prom 启动失败会
		return netErr
	}
}

func gracefulStopWithCtx(ctx context.Context, f func()) error {
	done := make(chan struct{}, 1)
	go func() {
		// f 是调用 grpc.Server.GracefulStop
		f()
		close(done) // 把 done channel 关闭
		// 优雅退出函数执行完就 close() channel 相当于发送信号了
	}()

	select {
	case <-ctx.Done():
		// timeout 的时候会 close() channel , 这里会导通
		// 或者 Cancel 函数调用也会
		return errs.New("timeout, ctx graceful stop")
	case <-done:
		// 优雅退出成功了, 就返回 nil
		return nil
	}
}

func prommetricsUnaryInterceptor(rpcRegisterName string) grpc.ServerOption {
	// 为了干净好看, 另外, 这个方法没有进行复用, 只是这个函数内部使用, 所以定义在函数的内部
	getCode := func(err error) int {
		if err == nil {
			return 0
		}
		rpcErr, ok := err.(interface{ GRPCStatus() *status.Status })
		if !ok {
			return -1
		}
		return int(rpcErr.GRPCStatus().Code())
	}
	// 添加 interceptor 也是一个 grpc.ServerOption
	return grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		// 在这里, 对 rpc 方法进行 prom 指标计数
		prommetrics.RPCCall(rpcRegisterName, info.FullMethod, getCode(err))
		return resp, err
	})
}

func prommetricsStreamInterceptor(rpcRegisterName string) grpc.ServerOption {
	return grpc.ChainStreamInterceptor()
}
