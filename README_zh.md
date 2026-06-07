# xredis-client

<div align="center">
    <p>
        <span style="color: #8b949e; font-size: 0.95em; letter-spacing: 0.5px; line-height: 1.5;">
        <i>The next-generation, ultra-fast asynchronous C++20 Redis client library built entirely on stackless coroutines.</i>
        </span>
    </p>
    <p>
        <img src="./images/code.gif" alt="xredis-client demo" style="max-width: 100%; width: 800px; height: auto;">
    </p>
</div>

---
<div align="center">
  <p>
    <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License: MIT"></a>
    <a href="https://en.cppreference.com/w/cpp/20"><img src="https://img.shields.io/badge/language-C%2B%2B20-blue.svg" alt="Language"></a>
    <a href="https://en.cppreference.com/w/cpp/compiler_support"><img src="https://img.shields.io/badge/compiler-GCC%2011%2B%20%7C%20Clang%2013%2B-blue.svg" alt="Compiler"></a>
  </p>
  | <a href="./README.md">English</a> | 
   <a href="./README_zh.md">简体中文</a> |
   <p></p> 
</div>

`xredis-client` 是一个提供简洁 C++20 co_await API和强大吞吐能力的Redis Client客户端，它的主要特点有：

- **[优美的DSL API](#QS)**: 像编写脚本语言一样简单地编写 Redis 流水线（Pipeline）和事务

- **[易于集成](#installation)**: 只需将 *header-only*文件拖入你的项目，即可直接使用

- **[极致性能](#PT)**: 基于io_uring、模板元编程以及使用环形缓冲区Ring buffer自动批合并命令，xredis-client 在保持 API 易用性的同时绝不牺牲性能. 详见 [性能测试对比小节](#PT).

- **Redis特性支持完善**: 全面支持 Redis 6.0+ RESP2/RESP3 协议和 TLS，包括针对 Redis Cluster 的自动透明的 MOVED / ASK 路由并提供类型安全的 co_await 返回结果

- **[线程安全且可扩展](#transparent-switch-between-redisclient-standalone-and-rediscluster)**: 通过模板参数，即可在单线程（极致性能）和多线程（共享实例）模式之间无缝切换，无需更改任何业务代码

- **[纯异步架构设计](#understanding-concurrency-models)**: 专为高并发场景设计的完全非阻塞命令接口

<a id="QS"></a>
## 🚀 Quick Start

`xredis-client`提供了许多内置的command函数让写redis命令更加简便

+ *Pipelining & Transactions (MULTI/EXEC)*
```cpp
using xredis::RedisValue;
using Redis = xredis::RedisClient<>;

xnet::task<> func(Redis& redis){
    std::string val2 = "hello xredis-client";
    auto [_1, _2, _3, exec_res] = co_await redis.multi()
                                                .set("user:{1001}", "val1")
                                                .set("item:{1001}", val2)
                                                .exec();
                                                // hash tag {...} for cluster mode
    if(exec_res.is_error()){
        std::cout << exec_res.as_error() << std::endl;
    }
    auto& arr = exec_res.as<RedisValue::array_t>(); // std::vector<RedisValue>
    co_return;
}
```
+ *Lazy Pipeline Building (Conditional Chaining)*:
```cpp
xnet::task<> func(Redis& redis){
    auto pipe = redis.get("xredis");    // start a lazy pipe until co_await

    if(/* your logic here*/){
        co_await pipe.mset("user:{1001}", "xredis-client", "item:{1001}", "val");
    }
    else{
        auto pipe2 = pipe.get("user:{1001}");
        auto [get1, get2] = co_await pipe2;
    }
    co_return;
}
```

## 安装和集成
环境要求:
- Linux kernel **5.15+** (Recommended 5.17+)
- `liburing-dev` >= 2.0
- A C++20 compiler
- OpenSSL >= 1.1.0 and Linux KTLS 如果需要TLS 并开启了宏 `XREDIS_ENABLE_TLS`
```cpp
#define XREDIS_ENABLE_TLS
```

---
1. #### Header-Only头文件集成 (推荐)

只需要下载以下两个头文件然后拖入工程里include即可:

```bash
wget https://raw.githubusercontent.com/FAF-D2/xredis-client/master/header-only/xnet.hpp

wget https://raw.githubusercontent.com/FAF-D2/xredis-client/master/header-only/xredis-client.hpp
```
如果没有安装liburing:
```bash
sudo apt install liburing-dev
```

复制这段代码到 test.cpp 进行测试

```cpp
#include"xnet.hpp"
#include"xredis-client.hpp"
#include<iostream>
using Redis = xredis::RedisClient<>;

xnet::task<int> some_task(){
    std::cout << "your task here" << std::endl;
    co_return 12345;
}

xnet::detached_task coro_main(Redis& redis){
    std::cout << "hello xredis-client!" << std::endl;
    co_await redis.context().yield();
    co_await some_task();
    // xnet::fire(some_task()); // or spawn it
    std::cout << "coro main done" << std::endl;
    redis.close();
    co_return;
}

int main(){
    xnet::io_context ctx;
    Redis redis(ctx);

    coro_main(redis); // spawn task
    
    std::cout << "ctx run" << std::endl;
    ctx.run_until_complete();
    std::cout << "exit" << std::endl;
    return 0;
}
```

编译运行:

```bash
g++ test.cpp -O2 -std=c++20 -luring -o test && ./test

### hello xredis-client!
### ctx run
### your task here
### coro main done
### exit
```

2. #### 或者源码构建

`xredis-client`使用了大量模板所以推荐仅头文件构建（第一种方法）. 不过你也可以通过cmake来构建：

```bash
git clone https://github.com/FAF-D2/xredis-client.git

cd xredis-client

mkdir build && cd build

cmake .. -DCMAKE_BUILD_TYPE=Release && make -j
```

cmake构建完成后就可以在你主项目的CMakeLists.txt引入:
```cmake
add_subdirectory(path/to/xredis-client)

add_executable(my_app main.cpp)
# Link the library
target_link_libraries(my_app PRIVATE xredis-client)
```

然后在代码中include：
```cpp
#include"xredis-client.hpp" // umbrella header file

int main(){
    return 0;
}
```

3. #### TLS 支持

如果你需要和Redis进行TLS加密通信, 你需要安装 Openssl >= 1.1.0然后开启内核的KTLS:
- OpenSSL: Version 1.1.0 or higher.
- KTLS: 确保内核支持KTLS和KTLS模块已经加载:
```bash
sudo apt install libssl-dev
sudo modprobe ktls
lsmod | grep ktls
```
取决你是怎么引入`xredis-client`的, 按以下步骤开启TLS:
- 仅头文件方法:

```cpp
#define XREDIS_ENABLE_TLS
#include "xredis-client.hpp"
```

- CMake源码构建时:
```cmake
cmake .. -DXREDIS_ENABLE_TLS=ON
```

<a id="PT"></a>
# 🐎 性能测试和对比
`xredis-client` 没有为了API简便性去牺牲一点性能 !

底下是 `xredis-client`, `boost-redis`, `go-redis` 之间的小对比，测试代码详见 [benchmark](./benchmark/)

场景：
256 bytes 的key-value pair，开启 10k 并发协程几乎同时发送get()请求，本地回环

*(10k coroutines await get())* 
| Library | 平均延迟 | 总时长 | 吞吐 | API简洁度 |
|--------|------|-------|--------|--------|
| **xredis-client** | **~1.2 µs** | **12.49 ms** | **~800,000 ops/s** | Simple |
| Boost.Redis | ~2.0 µs | 20.68 ms | ~483,000 ops/s | Medium |
| go-redis | ~15 µs | 1.5 s | ~66,622 ops/s | Simple |

# 更多例子
API文档可以在这找到: [API doc](./docs/API.md)

### 连接Redis

一旦连接上后, redis client 就会开启自动重连机制, 所以你只需要co_await connnect()一次

```cpp
xnet::detached_task coro_main(Redis& redis) noexcept{
    xredis::ConnectionOption option;
    option.ip = "127.0.0.1";        // ip string, domain like "www.example.com" is not supported
    option.port = 6379;             // port uint16_t
    option.username = "default";    // AUTH USERNAME PASSWORD
    option.password = "any";        // AUTH PASSWORD
    option.clientname = "D2";       // SET CLIENTNAME YOUR_NAME
    option.db = 0;                  // default 0
    option.resp = 3;                // 2 or 3, default 3

#ifdef XREDIS_ENABLE_TLS
    option.tls.cacert = "../tls/ca.crt";
    option.tls.cert   = "../tls/client.crt";
    option.tls.key    = "../tls/client.key";
    option.tls.sni    = "localhost";
#endif

    xnet::io_result<bool> result = co_await redis.connect(option);
    if(result.err){
        printf("error: %d\n", result.err);
        co_return;
    }
    else{
        printf("connect success\n");
    }

    // spwan tasks
    // ... 
    // ...
    // wating tasks for completing or while true

    redis.close();
    // should close at the [end]
    // otherwise, the ctx.run_until_complete() wouldn't stop forever
    // do not call this function when co_awaiting other operation under multi-threadings
    // see API docs for more details
}
```
---

### 几乎无痛切换集群和单点模式. 

集群和单点的客户端几乎有一样的API除了一些命令没有key时会有差异. 集群RedisCluster<>内部会自己处理`MOVED`/`ASK`错误，所以只需要这么写就能享受简洁的切换：

```cpp
constexpr bool shared_between_threads = false; // if the Redis shared
constexpr size_t ring_buffer_size = 4096; // the maximum inflight operations before it returns

// using Redis = xredis::RedisClient<shared_between_threads, ring_buffer_size>;
using Redis = xredis::RedisCluster<shared_between_threads, ring_buffer_size>;

xnet::task<> func(Redis& redis){
    co_await redis.set("key").get("key2"); // no need for any changes
}

```
---

### Type-safe的返回值以及提供方便的函数 for debugging
`co_await` 一个redis命令会返回一个`RedisValue`. 虽然这个库期望你去写`as<T>()`的显式类型安全函数来保证安全访问, `RedisValue` 也提供了一些方便的debug函数方便在早期开发的时候检查redis返回值.可以详见[API documents](./docs/API.md)里的RedisValue小节

```cpp
auto res = co_await redis.get("mykey");

if(res.is_integer()){
    auto val = res.as<RedisValue::integer_t>();
    std::cout << "Value: " << val << std::endl;
}

// Use dump(), as_string(), or as_error() to avoid verbose if-checks 
// during early development stages.
if(res.is_error()){
    std::string_view error_msg = res.as_error();
    std::cout << error_msg << std::endl;
}
if(res.is_string_type()){
    std::string_view content = res.as_string();
    std::cout << content << std::endl 
}
std::string json_str = res.dump();
std::cout << "Raw content: " << json_str << std::endl;
```
---

### 动态参数下的命令传参

虽然目前大多数命令的参数都需要编译期确定从而提供参数检查和性能提升，但是也支持运行期参数，比如给定一个vector去MSET和MGET

目前由于Redis的命令太多的缘故，我还没来得及为每个命令加上迭代器版本的重载，所以需要调用通用的command_range()函数手动发命令：


```cpp
template<typename It>
concept StringViewIterator = std::convertible_to<std::iter_value_t<It>, std::string_view>;

// auto command_range(std::string_view cmd, StringViewIterator begin, size_t count);

// auto command_range(std::string_view cmd, std::string_view key, StringViewIterator begin, size_t count);

xnet::task<> func(Redis& redis){
    std::vector<std::string> keys = {"key1", "key2"};
    std::vector<std::string> keypair = {"key1", "val1", "key2", "val2"};
    auto [res1, res2] = co_await redis.command_range("mget", keys.begin(), keys.size())
                    .command_range("mset", keypair.begin(), keys.size());
}
```
---

### 熟悉协程并发模型
在xnet里提供了三种基础的task types:

| Task type | 可取消 | 可以被co_await | task的执行时机 | 用法 
|--------|------|-------|--------| ---- |
| task<T> | yes | yes | co_awaited or fired | `co_await task()`; `xnet::fire(task())`; `task.cancel_token()`|
| ptask<T> | no | yes | co_awaited or fired | `co_await ptask()`; `xnet::fire(ptask())` |
| detached_task | no | no | immediately | `detach_task()` |

以下代码展示了所有这些task的用法:
```cpp
#include"xnet.hpp"

xnet::io_context ctx;
int id = 0;
xnet::AsyncTimer timer(ctx);

xnet::task<> some_task(int timed){
    std::cout << "task " << id++ << " begin" << std::endl;

    auto res = co_await timer.timeout(timed);
    if(res.err){
        std::cout << "error code: " << res.err << std::endl;
        std::cout << (res.cancelled() ? "CANCELLED" : "OTHER ERROR") << std::endl;
    }
    else{
        std::cout << "task done" << std::endl;
    }
}

template<class T>
xnet::ptask<> pure_task(int timed, T token){
    std::cout << "pure task " << id++ << " begin" << std::endl;

    co_await timer.timeout(timed);
    token.cancel();

    std::cout << "pure task done" << std::endl;
}

xnet::detached_task fire_and_forget_task(int timed){
    std::cout << "detached task " << id++ << " begin" << std::endl;

    co_await timer.timeout(timed);
    co_await some_task(timed);

    auto task4 = some_task(10);
    xnet::fire(
        pure_task(5, task4.cancel_token())
    );
    co_await task4;

    std::cout << "detached task done" << std::endl;
}


int main(){
    xnet::fire(some_task(3));
    fire_and_forget_task(3);
    ctx.run_until_complete();
    return 0;
}
```
这个异步模型来自我的另一个project叫[`xnet`](https://github.com/FAF-D2/xnet)，如果你感兴趣的话也可以去这个项目里看看。`xredis-client`也是完全基于它开发的。它是一个linux下轻量的io_uring异步IO库,并且提供了一些方便的工具函数比如刚才提到的task type和结构化并发:
```cpp
auto res = co_await xnet::race(coro1, coro2, coro3);
auto res = co_await xnet::any(coro1, coro2, coro3);
auto res = co_await xnet::all(coro1, coro2, coro3);
auto res = co_await xnet::allSettled(coro1, coro2, coro3);
```

# 架构
```text
+-----------------------------------------------------------+
|                1. User Coroutine Space                    |
|             (Business logic, co_await API)                |
+-----------------------------------------------------------+
               ^                        |
               |  resume                |
               |                        |  command
               |                        v 
+-----------------------------------------------------------+
|                2. xredis-client DSL Layer                 |
|            (Serialization, Pipeline Management)           |
+-----------------------------------------------------------+
               ^                        |  Serialization
               |  pop ring              | 
               |                        |  push to ring  
               |  parse done            v
+-----------------------------------------------------------+
|               3. xredis-client Worker Layer               |
|                       [Ring buffer]                       |
|                             |                             |
|   Auto     +--[reader coroutine]                          |
| Reconnect -|               -/-                            |
|  Policy    +--             [writer coroutine]             |
+-----------------------------------------------------------+
               ^                        | Batch commands
               | parse                  | 
               |                        | writev
               | recv                   v 
+-----------------------------------------------------------+
|               4. IO Layer (xnet / io_uring)               |
|                    (Socket I/O, KTLS)                     |
+-----------------------------------------------------------+
```

# Contact
如果你有任何问题和建议，欢迎提discussion，issue，或者通过邮箱联系我：`yluo0000@uni.sydney.edu.au`