# APi doc

- **[Redis Client Instance:](#redis-client-instance)** `RedisClient<>`, `RedisCluster<>`


- **[RedisValue](#redisvalue)**: `type_t`, `as<type_t>()`, `set<>()`, `is_xx()`
- **[Connection Configuration:](#connection-configuration)** `ConnectionOption`, `ClusterConnectionOption` 


- **Redis Sub/Pub**: `RedisSubscriber` 


## Redis Client Instance
The library now provides two primary client types for interacting with Redis. Both are template-based to allow fine-grained control over resource allocation and threading behavior.
- `shared`: if the instance is shared across threads, if true, the ring will be mpsc, otherwise spsc.

- `xcapacity`: the maximum inflight operations allowed. For instance, if the redis server is down, you can waiting for `xcapacity` operations at most without error `RING OVERFLOW`. The `xcapacity` should be the power of 2

```cpp
template<bool shared = true, size_t xcapacity=4096>
class RedisClient;

template<bool shared = true, size_t xcapacity=4096>
class RedisCluster;

// Constructors
RedisClient(xnet::io_context&);
RedisCluster(xnet::io_context&);
```
Examples:
```cpp
// using Redis = RedisClient<false, 8192>;
using Redis = RedisCluster<true, 8192 * 4>;

int main(){
    xnet::io_context ctx(1024); // 1024 sqe size
    Redis redis(ctx);
}
```

### Functions:

---
#### `connect(option, timeout)`
- `option` -- see [ConnectionConfiguration](#connection-configuration) for more details

- `timeout` -- the timeout for connecting

- co_await result: `xnet::io_result<bool>`

**! important:** 

once the connect() is successful, the client instance will take care all of the re-connecting by starting the worker layer coroutines, so you might ensure the `connect()` be called once to prevent memory leak if successful.

if you do not need the instance anymore, call `close()` or let the destructor `~RedisClient()` happens to shutdown the backend coroutines.

```cpp
xnet::task<xnet::io_result<bool>> RedisClient::connect(const xredis::ConnectionOption& option, int timeout=10);

xnet::task<xnet::io_result<bool>> RedisClient::connect(const xredis::ClusterConnectionOpiton& option, int timeout=10);
```
Examples:
```cpp
xnet::detached_task coro_main(Redis& redis){
    xredis::ConnectionOption option;
    auto res = co_await redis.connect(opiton, 5);
    if(res.err){
        // error handling;
    }
}
```
---
#### `close()` -> void

The close will suggest the pipeline (worker layer) to shutdown, **acting exactly as the destructor**. 

**! important:** *No more co_await redis.command() should be called once the close() is called just like the destrutor*

```cpp
void RedisClient::close();

void RedisCluster::close();
```

Examples:
```cpp
xnet::detached_task coro_main(Redis& redis){
    xredis::ConnectionOption option;
    auto res = co_await redis.connect(opiton, 5);
    if(res.err){
        // error handling;
    }
    co_await redis.get("key");
    xnet::fire(redis.set("key", "val"));

    redis.close();
    
    co_await redis.get(); // User-After-free !!!
    // easy to neglect under multi-threadings environment
}

```

---

#### `command(...)` and `command_range(...)` -> CommandAwaiter<>

return a Awaiter that can be co_await or be used in a pipeline chain

Examples:
```cpp
std::vector<std::string> dynamic_keys = {"k1", "k2"}; 

co_await redis.command("SET", "key", "val")
                .mget("test1", "test2")
                .command_range("MGET", dynamic_keys.begin(), dynamic_keys.size());
```

**! important**: 

*do not `co_await redis.multi()` \*alone\* as it will switch to other coroutines to push command between `multi` and `exec`*

Wrong usage:
```cpp
xnet::task<> coro(Redis& redis){
    co_await redis.multi();
    if(...){
        co_await redis.set(...);
    }
    else{
        co_await redis.mset(...);
    }
    co_await redis.exec();
}

xnet::task<> other_coro(Redis& redis){
    co_await redis.set(...);
}
```
Recommended way:
```cpp
xnet::task<> coro(Redis& redis){
    auto pipe = redis.multi();
    if(...){
        co_await pipe.set(...).exec();
    }
    else{
        co_await pipe.mset(...).exec();
    }
}

xnet::task<> other_coro(Redis& redis){
    co_await redis.set(...);
}
```

## RedisValue
The co_await result of any commands is `RedisValue`, which is a tagged union represents all possible RESP2/RESP3 data types:

### Type Identification
You should always verify the type before accessing the underlying value to ensure safety.

| Method | Returns |
| :--- | :--- |
| `type()` | Returns the raw `TYPE` enum. |
| `is_error()` | Returns `true` if the value is a `SIMPLE_ERROR` or `BULK_ERROR`. |
| `is_string_type()`| Returns `true` if it's `SIMPLE_STRING`, `BULK_STRING`, or `VERBATIM_STRING`. |
| `is_<type>()` | Boolean checks for specific types (e.g., `is_array()`, `is_integer()`, `is_map()`). |

```cpp
enum TYPE: int{
    NULLPTR = 0x00000000,
    // vector
    ARRAY, PUSH,
    // map
    SET, MAP,
    // string
    SIMPLE_STRING, SIMPLE_ERROR, BULK_STRING, 
    BIG_NUMBER, BULK_ERROR, VERBATIM_STRING,
    // basic
    INTEGER, BOOL, DOUBLE,
    // attribute
    ATTRIBUTE
};
```

### Type Mapping Table
When calling `as<T>()` or `set<T>()`, use the following type mappings:

| Redis Type | C++ Type (`T`) | RESP Version | underlying type |
| :--- | :--- | :--- | :--- |
| Simple String | `RedisValue::simple_string_t`| 2 | std::string | 
| Simple Error | `RedisValue::simple_error_t`| 2 | std::string | 
| Integer | `RedisValue::integer_t`| 2 | int64_t |
| Bulk String |  `RedisValue::bulk_string_t` | 2 | std::string | 
| Array | `RedisValue::array_t` | 2 |std::vector\<RedisValue\> | 
| Null | `RedisValue::null_t` | 3 | nullptr |
| Boolean | `RedisValue::bool_t` | 3 | bool |
| Double | `RedisValue::double_t` | 3 | double |
| Big Number |  `RedisValue::big_number_t` | 3 | std::string | 
| Bulk Error |  `RedisValue::bulk_error_t` | 3 | std::string | 
| Verbatim String |  `RedisValue::verbatim_string_t` | 3 | std::string | 
| Map | `RedisValue::map_t` | 3 | std::vector\<std::pair\<RedisValue, RedisValue\>\> | 
| Set | `RedisValue::set_t`| 3 | std::vector\<RedisValue\> |
| Push | `RedisValue::push_t` | 3 |std::vector\<RedisValue\> | 
| Attribute | `RedisValue::attribute_t` | 3 |std::pair\<map_t, RedisValue\> | 

### Accessing Data
The underlying type might change at the future, so use `as<T>()` to extract the underlying C++ type.

- **`as<T>()`**: Returns a reference to the underlying data.

For *DEBUG purpose*:
- **`as_string()`**: Returns the std::string_view if the type is `simple_string_t`, `bulk_string_t`, `verbatim_string_t`, otherwise `""`

- **`as_error()`**: Returns the std::string_view if the type is `simple_error_t`, `bulk_error_t`, otherwise `""`

- **`dump()`**: Serialize the `RedisValue` to json format string and returns `std::string`


### Usage Example
```cpp
auto res = co_await redis.get("mykey");

if (res.is_error()){
    std::cerr << "Redis Error: " << res.as_error() << std::endl;
}
else if(res.is_integer()){
    auto val = res.as<RedisValue::integer_t>();
    std::cout << "Value: " << val << std::endl;
}
else if(res.is_array()){
    auto& array = res.as<RedisValue::array_t>();
    for(auto& item : array){
        std::string debug_string = item.dump();
        std::cout << debug_string << std::endl;
    }
}
```

## Connection Configuration

To initialize a client or cluster, you need to provide specific connection parameters.

### 1. `ConnectionOption` (For `RedisClient`)
The structure used to configure a single-node connection.

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `ip` | const char* | "127.0.0.1" | Server IP address (Domain names are currently not supported). |
| `port` | uint16_t | 6379 | Server listening port. |
| `username` | const char* | nullptr | ACL username for authentication. |
| `password` | const char* | nullptr | AUTH password. |
| `clientname` | const char* | nullptr | Client name set via `CLIENT SETNAME`. |
| `db` | int | 0 | Default database index to select. |
| `resp` | int | 3 | Redis protocol version (RESP2 or RESP3). |
| `tls` | `TLSOption` | - | TLS/SSL configuration (conditional). |

### 2. `ClusterConnectionOption` (For `RedisCluster`)
The structure used for seed node configuration in a clustered environment.

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `seeds_ip` | const char** | nullptr | Pointer to an array of seed IP strings (do not support domain). |
| `ports` | const uint16_t*| nullptr | Pointer to an array of corresponding ports. |
| `num_seeds` | size_t | 0 | Number of provided seed nodes. |
| `username` | const char* | nullptr | Authentication username for cluster nodes. |
| `password` | const char* | nullptr | Authentication password for cluster nodes. |
| `clientname` | const char* | nullptr | Client name. |
| `resp` | int | 3 | Protocol version used for cluster nodes. |
| `tls` | `TLSOption` | - | TLS/SSL configuration (conditional). |

### 3. TLS Configuration (`TLSOption`)
If `XREDIS_ENABLE_TLS` is defined, the `tls` member is available for secure communication:

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `cacert` | const char* | nullptr | Path to the CA certificate file. |
| `cacertdir` | const char*  | nullptr | Path to the CA certificate directory |
| `cert` | const char* | nullptr | Path to the client certificate file. |
| `key` | const char* | nullptr | Path to the client private key file. |
| `sni` | const char* | nullptr | SNI (Server Name Indication) string for TLS handshake. |

Make sure the openssl >= 1.1.0 and KTLS is enabled by:
```bash
sudo modprobe ktls

lsmod | grep ktls
```
---
Examples:
```cpp
// Example: Configuring a standalone Redis client
xredis::ConnectionOption opt;
opt.ip = "192.168.1.10";
opt.port = 6379;
opt.password = "your_password";


#ifdef XREDIS_ENABLE_TLS
    opt.tls.cacert = "../tls/ca.crt";
    opt.tls.cert   = "../tls/client.crt";
    opt.tls.key    = "../tls/client.key";
    opt.tls.sni    = "localhost";
#endif

// Example: Configuring a Redis Cluster
xredis::ClusterConnectionOption cluster_opt;
const char* ips[] = {"10.0.0.1", "10.0.0.2"};
uint16_t ports[] = {7000, 7000};
cluster_opt.seeds_ip = ips;
cluster_opt.ports = ports;
cluster_opt.num_seeds = 2;
```

## Redis Subscriber

`xredis::RedisSubscriber` provides a lightweight, dedicated interface for Redis `Sub` operations. Unlike the standard `RedisClient`, the client must actively read incoming messages.

**! important**: *Do NOT send **subscribe** command with `RedisClient` or `RedisCluster`, while **publish** commands are allowed*

### Public API

| Method | Returns | Description |
| :--- | :--- | :--- |
| `connect(option)` | io_result<bool> | Establish the connection with specific options. |
| `subscribe(channels, ...)` | `ptask<io_result<bool>>` | Send subscribe request to the server |
| `unsubscribe(channels, ...)` | `ptask<io_result<bool>>` | Send unsubscribe request to the server |
| `psubscribe(patterns, ...)` | `ptask<io_result<bool>>` | Send subscribe request to the server with patterns. |
| `punsubscribe(patterns, ...)` | `ptask<io_result<bool>>` | Send unsubscribe request to the server with patterns |
| `read_one_packet(timed)` | `io_result<RedisValue>` | Wait for the next incoming Redis message. |
| `close()` | `void` | Close the subscription connection. |


Simply, the `RedisSubscriber` can be treated as the Redis version of raw socket so you are expected to manage the IO error *\*actively\**.

The `subscribe()` or `unsubscribe()` is similar to `socket.send()` and the `read_one_packet()` is similar to `socket.recv()`

Examples:
```cpp
xnet::task<> coro(xnet::io_context& ctx){
    xredis::RedisSubscriber subscriber(ctx);
    xredis::ConnectionOption option;

    option.clientname = "D2";
    option.resp = 3;

    auto result = co_await subscriber.connect(option);
    if(result){
        // error handling
    }

    auto res = co_await subscriber.subscribe(...);
    if(res.err){
        // sending failed
        // error handling
    }
    while(true){
        auto packet = co_await subscriber.read_one_packet();
        if(packet.err){
            std::cout << "error: " << packet.err << std::endl;
            break;
        }

        if(!(packet->is_array() || packet->is_push())){
            std::cout << "Unknown error "<< std::endl;
            break; 
        }
        std::cout << packet->dump() << std::endl;
        // RESP 2
        // auto& arr = packet->as<RedisValue::array_t>();
    
        // RESP 3
        auto& arr = packet->as<RedisValue::push_t>();
    }
}
```

The packet is an array (std::vector\<RedisValue\>): 

| Scenario | Packet Structure |
| :--- | :--- |
| **Subscription Confirmation** | `["subscribe", "channel_name", integer_count]` |
| **Unsubscription Confirmation** | `["unsubscribe", "channel_name", integer_count]` |
| **Message Received** | `["message", "channel_name", "payload_content"]` |
| **Pattern Message** | `["pmessage", "pattern", "channel_name", "payload_content"]` |