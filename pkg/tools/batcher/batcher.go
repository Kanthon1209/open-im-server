package batcher

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/utils/idutil"
)

var (
	DefaultDataChanSize = 1000
	DefaultSize         = 100
	DefaultBuffer       = 100
	DefaultWorker       = 5
	DefaultInterval     = time.Second
)

type Config struct {
	size       int           // Number of message aggregations
	buffer     int           // The number of caches running in a single coroutine
	dataBuffer int           // The size of the main data channel
	worker     int           // Number of coroutines processed in parallel
	interval   time.Duration // Time of message aggregations
	syncWait   bool          // Whether to wait synchronously after distributing messages have been consumed
}

type Option func(c *Config)

func WithSize(s int) Option {
	return func(c *Config) {
		c.size = s
	}
}

func WithBuffer(b int) Option {
	return func(c *Config) {
		c.buffer = b
	}
}

func WithWorker(w int) Option {
	return func(c *Config) {
		c.worker = w
	}
}

func WithInterval(i time.Duration) Option {
	return func(c *Config) {
		c.interval = i
	}
}

func WithSyncWait(wait bool) Option {
	return func(c *Config) {
		c.syncWait = wait
	}
}

func WithDataBuffer(size int) Option {
	return func(c *Config) {
		c.dataBuffer = size
	}
}

type Batcher[T any] struct {
	config *Config

	globalCtx context.Context
	cancel    context.CancelFunc
	// Do 方法是实际的业务数据消费逻辑
	Do         func(ctx context.Context, channelID int, val *Msg[T])
	OnComplete func(lastMessage *T, totalCount int)
	Sharding   func(key string) int
	Key        func(data *T) string
	HookFunc   func(triggerID string, messages map[string][]*T, totalCount int, lastMessage *T)
	data       chan *T
	// 每一个 Worker channel
	chArrays []chan *Msg[T]
	wait     sync.WaitGroup
	counter  sync.WaitGroup
}

func emptyOnComplete[T any](*T, int) {}
func emptyHookFunc[T any](string, map[string][]*T, int, *T) {
}

func New[T any](opts ...Option) *Batcher[T] {
	b := &Batcher[T]{
		OnComplete: emptyOnComplete[T],
		HookFunc:   emptyHookFunc[T],
	}
	config := &Config{
		size:     DefaultSize,
		buffer:   DefaultBuffer,
		worker:   DefaultWorker,
		interval: DefaultInterval,
	}
	for _, opt := range opts {
		opt(config)
	}
	b.config = config
	b.data = make(chan *T, DefaultDataChanSize)
	b.globalCtx, b.cancel = context.WithCancel(context.Background())

	b.chArrays = make([]chan *Msg[T], b.config.worker)
	for i := 0; i < b.config.worker; i++ {
		b.chArrays[i] = make(chan *Msg[T], b.config.buffer)
	}
	return b
}

func (b *Batcher[T]) Worker() int {
	return b.config.worker
}

// Batcher 中, 先校验 Batcher 的 各个必要方法是否得到了绑定
// 然后再根据配置的 worker, 逐个启动 worker goroutine 不断从 shard 中拿消息
// 进行处理
func (b *Batcher[T]) Start() error {
	if b.Sharding == nil { // 需要有个 shading 函数
		// 用于将消息分配给一个 具体的 Worker 的
		return errs.New("Sharding function is required").Wrap()
	}
	if b.Do == nil { // 需要处理函数
		return errs.New("Do function is required").Wrap()
	}
	if b.Key == nil { // 需要 消息 -> key 的映射函数
		return errs.New("Key function is required").Wrap()
	}
	b.wait.Add(b.config.worker) // 设置 worker 数量的 WaitGroup
	for i := 0; i < b.config.worker; i++ {
		// Batcher.chArrays[i] 就是 i 号 Worker 对应的待处理数据 channel
		// 数据类型是 *Msg[T]
		// Batcher.run 函数也是要初始化的时候设置, 对于每一个业务来说, 业务数据类型是特定的
		// 业务数据的处理逻辑也是特定的, 但是 batcher 这里 开 worker 的 逻辑是通用的
		// 启动 worker goroutine, 在其中使用 Do 方法对 channel 中的数据进行处理
		go b.run(i, b.chArrays[i]) // 显然这个 run 函数中会对 b.wait 这个 WaitGroup 进行修改
		// 这里用到了 Batcher 的 chArrays, 显然 chArrays 中的内容就是 Batcher.scheduler 来进行填充的了
	}
	b.wait.Add(1)
	go b.scheduler() // schedular 也占用一个
	// scheduler 会
	return nil
}

func (b *Batcher[T]) Put(ctx context.Context, data *T) error {
	if data == nil {
		return errs.New("data can not be nil").Wrap()
	}
	select {
	case <-b.globalCtx.Done():
		return errs.New("data channel is closed").Wrap()
	case <-ctx.Done():
		return ctx.Err()
	case b.data <- data:
		return nil
	}
}

// data 是 外部数据源, 或者说是总数据 channel
func (b *Batcher[T]) scheduler() {
	//
	ticker := time.NewTicker(b.config.interval)
	// Track whether b.data was closed by an external caller so the
	// cleanup below does not close it a second time. The only routes
	// out of this function that leave b.data open are the nil-message
	// and ticker paths; the ok == false branch means someone already
	// closed the channel, and calling close(b.data) again there would
	// panic with "close of closed channel" (#3653).
	externallyClosed := false
	// 闭包用到的 externallyClosed、b、ticker 都是外层变量本身。defer 注册时不拷贝它们。函数真正退出、这段函数开始执行时，读到的是当时的值。第 202 行把 externallyClosed 设成 true 再 return，defer 里看到的就是 true，因此不会再 close(b.data)。

	// defer func(closed bool){}(externallyClosed) 这种
	// 写成参数，才是注册时的快照。参数在 defer 这一行立刻求值
	defer func() {
		ticker.Stop() // 结束的时候, 停止掉 ticker
		for _, ch := range b.chArrays {
			close(ch) // 对 Batcher 中的每一个 shard( 本质上是一个 channel )
			// 关闭掉每一个 shard channel
			// for range
		}
		if !externallyClosed { // 这里有意思, 我们关心的闭包这里
			// 这里拿到的是快照还是
			close(b.data)
		}
		b.wait.Done()
	}()
	// 多个 defer 按注册的反序执行。

	// defer 排在返回值已经写好之后、函数真正回到调用方之前。没有具名返回值时，defer 里没有名字可以改。
	// 有具名返回值时可以改，调用方拿到的是改过的结果

	// string -> slice[*T]

	// 这个应该是, shard...
	vals := make(map[string][]*T)
	count := 0
	var lastAny *T

	for {
		select {
		case data, ok := <-b.data:
			// data channel 如果关闭了, 这里也会导通
			// 不过 data 拿到的是 类型零值, false
			if !ok {
				// If the data channel is closed unexpectedly
				externallyClosed = true
				// 这样 defer func ()() 执行的时候就会 执行对应的逻辑
				// data 是一个 channel, 如果没有被关闭, 那我们结束的时候要关闭一下
				// channel 不能进行重复关闭
				return
			}
			// 这种就是拿到数据了, 并不是 channel 关闭了拿到了零值
			// 因为 不 OK 直接就会在上面进行返回
			if data == nil {
				if count > 0 {
					b.distributeMessage(vals, count, lastAny)
				}
				return
			}

			//
			key := b.Key(data)                  // 获取 key
			vals[key] = append(vals[key], data) // 往 shard 中放数据
			lastAny = data

			count++
			if count >= b.config.size { // 如果总消息数量已经大于设置的 size 了
				// 那么立刻分发
				b.distributeMessage(vals, count, lastAny)
				vals = make(map[string][]*T) // 重新创建一个 map[string][]*T, 但是这样有点开销把
				// 如果追求性能提升, 应该是将 slice 置空?
				count = 0
			}

		// 到定时器时间了
		case <-ticker.C:
			if count > 0 {
				// 到了规定的时间, 如果有数据, 那么就进行分发
				b.distributeMessage(vals, count, lastAny)
				vals = make(map[string][]*T)
				count = 0
			}
		}
	}
}

type Msg[T any] struct {
	key       string
	triggerID string
	val       []*T
}

func (m Msg[T]) Key() string {
	return m.key
}

func (m Msg[T]) TriggerID() string {
	return m.triggerID
}

func (m Msg[T]) Val() []*T {
	return m.val
}

func (m Msg[T]) String() string {
	var sb strings.Builder
	sb.WriteString("Key: ")
	sb.WriteString(m.key)
	sb.WriteString(", Values: [")
	for i, v := range m.val {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%v", *v))
	}
	sb.WriteString("]")
	return sb.String()
}

func (b *Batcher[T]) distributeMessage(messages map[string][]*T, totalCount int, lastMessage *T) {
	triggerID := idutil.OperationIDGenerator()
	b.HookFunc(triggerID, messages, totalCount, lastMessage)
	for key, data := range messages {
		if b.config.syncWait {
			b.counter.Add(1)
		}
		channelID := b.Sharding(key)
		b.chArrays[channelID] <- &Msg[T]{key: key, triggerID: triggerID, val: data}
	}
	if b.config.syncWait {
		b.counter.Wait()
	}
	b.OnComplete(lastMessage, totalCount)
}

// 这是一个 Worker, 在单独一个 goroutine 中不断从自己的 shard 中取数据
// 并执行 Do 函数
func (b *Batcher[T]) run(channelID int, ch <-chan *Msg[T]) {
	defer b.wait.Done() // 将 WaitGroup 计数--
	// 从 channel 中读取消息, 并调用 处理函数
	// 直到 channel 被关闭, 且缓冲区中的数据读取完毕后, 会出这个 for 循环
	for messages := range ch {
		// 这里的 Context 并不是上游传递过来的
		b.Do(context.Background(), channelID, messages)
		if b.config.syncWait {
			// Msg[T] 是一批消息的封装
			// syncWait 开启后, 我们处理完这一批消息, 会给 Conuter--
			// 那么调度方把这一批放到各个 Worker shard 后会停
			// 等待各个 Worker
			b.counter.Done()
		}
	}

	// 如果改成 select 并在 Context().Done() 时直接 return，通道里已经放进来、
	// 还没被这次循环取走的 *Msg[T] 就不会再进入 Do。
	// 这种现象叫 取消时未排空（cancel without drain）。
	// 队列里这些还没被处理的数据被丢掉，叫 丢弃已入队消息（dropped queued messages）。
	// 业务上看就是这批消息没被处理
}

func (b *Batcher[T]) Close() {
	// Batcher.Close() 方法并没有关掉通道, 而是给通道中传入一个 nil
	b.cancel() // Signal to stop put data
	b.data <- nil
	//wait all goroutines exit
	b.wait.Wait()
}
