#ifndef redis_cluster_hpp
#define redis_cluster_hpp
#include"xnet.hpp"
#include"RedisValue.h"
#include"RedisConnection.h"
#include<atomic>
#include<cstdint>
#include<charconv>
#include<string_view>

namespace xredis{
    template<bool shared = true, size_t xcapacity=4096, size_t max_num_shards=512>
    class RedisCluster{
        using Pipeline = xredis::Pipe<shared, xcapacity>;

        struct Shard{
            // init --- 0
            // connecting --- 1
            // connected --- 2
            // dead --- 3

        #ifndef XNET_DISABLE_THREAD_SAFE
            using connected_flag_t = std::atomic<int>;

            template<bool s>
            int set_connecting() noexcept{
                int expected = 0;
                connected.compare_exchange_strong(expected, 1, std::memory_order_acq_rel);
                return expected;
            }
            template<bool s>
            int set_connected() noexcept{
                int expected = 1;
                connected.compare_exchange_strong(expected, 2, std::memory_order_acq_rel);
                return expected;
            }
            template<bool s>
            int wait_connected() noexcept{
                int status;
                do{
                    XNET_CPU_RELAX();
                    status = connected.load(std::memory_order_acquire);
                }while(status <= 1);
                return status;
            }
            template<bool s>
            int get_connected() noexcept{
                return connected.load(std::memory_order_acquire);
            }

            template<bool s>
            int set_dead() noexcept{
                return connected.exchange(3, std::memory_order_acq_rel);
            }
        #else
            using connected_flag_t = std::conditional_t<shared, std::atomic<int>, int>;
            
            template<bool s, typename std::enable_if<s, bool>::type = true>
            int set_connecting() noexcept{
                int expected = 0;
                connected.compare_exchange_strong(expected, 1, std::memory_order_acq_rel);
                return expected;
            }
            template<bool s, typename std::enable_if<s, bool>::type = true>
            int set_connected() noexcept{
                int expected = 1;
                connected.compare_exchange_strong(expected, 2, std::memory_order_acq_rel);
                return expected;
            }
            template<bool s, typename std::enable_if<s, bool>::type = true>
            int wait_connected() noexcept{
                int status;
                do{
                    XNET_CPU_RELAX();
                    status = connected.load(std::memory_order_acquire);
                }while(status <= 1);
                return status;
            }
            template<bool s, typename std::enable_if<s, bool>::type = true>
            int get_connected() noexcept{
                return connected.load(std::memory_order_acquire);
            }
            template<bool s, typename std::enable_if<s, bool>::type = true>
            int set_dead() noexcept{
                return connected.exchange(3, std::memory_order_acq_rel);
            }

            template<bool s, typename std::enable_if<!s, bool>::type = true>
            int set_connecting() noexcept{
                int expected = 0;
                if(connected == expected){
                    connected = 1;
                    return 0;
                }
                return connected;
            }
            template<bool s, typename std::enable_if<!s, bool>::type = true>
            int set_connected() noexcept{
                connected = 2;
                return 1;
            }
            template<bool s, typename std::enable_if<!s, bool>::type = true>
            int wait_connected() noexcept{
                return 2;
            }
            template<bool s, typename std::enable_if<!s, bool>::type = true>
            int get_connected() noexcept{
                return connected;
            }
            template<bool s, typename std::enable_if<!s, bool>::type = true>
            int set_dead() noexcept{
                return std::exchange(connected, 3);
            }
        #endif

            connected_flag_t connected;
            uint16_t port;
            uint32_t ip;
            Pipeline* pipe;

            Shard(uint32_t ip, uint16_t port)
            noexcept: connected(0), port(port), ip(ip), pipe(nullptr)
            {}

            ~Shard(){
                int status = this->set_dead<shared>();
                if(status == 2){
                    this->pipe->template set_alive<shared>(false);
                    this->pipe->client.shutdown();
                }
            }

            void close() noexcept{
                int status = this->set_dead<shared>();
                if(status == 2){
                    this->pipe->template set_alive<shared>(false);
                    this->pipe->client.shutdown();
                }
            }
        };

        struct Router{
        #ifndef XNET_DISABLE_THREAD_SAFE
            using conn_pointer_t = std::atomic<Shard*>;

            template<bool s>
            void set_slot(size_t slot_idx, Shard* shard, std::memory_order order = std::memory_order_release) noexcept{
                this->slot[slot_idx].store(shard, order);
            }
            template<bool s>
            Shard* get_slot(size_t slot_idx, std::memory_order order = std::memory_order_acquire) noexcept{
                return this->slot[slot_idx].load(order);
            }

            template<bool s>
            void set_hash(size_t hash_idx, Shard* shard, std::memory_order order = std::memory_order_release) noexcept{
                this->shards[hash_idx].store(shard, order);
            }
            template<bool s>
            Shard* get_hash(size_t hash_idx, std::memory_order order = std::memory_order_acquire) noexcept{
                return this->shards[hash_idx].load(order);
            }
            template<bool s>
            bool cas_hash(size_t hash_idx, Shard*& expected, Shard* new_shard, std::memory_order order = std::memory_order_acq_rel) noexcept{
                return this->shards[hash_idx].compare_exchange_strong(expected, new_shard, order);
            }
            template<bool s>
            Shard* exchange_hash(size_t hash_idx, Shard* new_shard, std::memory_order order = std::memory_order_acq_rel) noexcept{
                return this->shards[hash_idx].exchange(new_shard, order);
            }
        #else
            using conn_pointer_t = std::conditional_t<shared, std::atomic<Shard*>, Shard*>;

            template<bool s, typename std::enable_if<s, bool>::type = true>
            void set_slot(size_t slot_idx, Shard* shard, std::memory_order order = std::memory_order_release) noexcept{
                this->slot[slot_idx].store(shard, order);
            }
            template<bool s, typename std::enable_if<s, bool>::type = true>
            Shard* get_slot(size_t slot_idx, std::memory_order order = std::memory_order_acquire) noexcept{
                return this->slot[slot_idx].load(order);
            }

            template<bool s, typename std::enable_if<s, bool>::type = true>
            void set_hash(size_t hash_idx, Shard* shard, std::memory_order order = std::memory_order_release) noexcept{
                this->shards[hash_idx].store(shard, order);
            }
            template<bool s,  typename std::enable_if<s, bool>::type = true>
            Shard* get_hash(size_t hash_idx, std::memory_order order = std::memory_order_acquire) noexcept{
                return this->shards[hash_idx].load(order);
            }
            template<bool s, typename std::enable_if<s, bool>::type = true>
            bool cas_hash(size_t hash_idx, Shard*& expected, Shard* new_shard, std::memory_order order = std::memory_order_acq_rel) noexcept{
                return this->shards[hash_idx].compare_exchange_strong(expected, new_shard, order);
            }
            template<bool s, typename std::enable_if<s, bool>::type = true>
            Shard* exchange_hash(size_t hash_idx, Shard* new_shard, std::memory_order order = std::memory_order_acq_rel) noexcept{
                return this->shards[hash_idx].exchange(new_shard, order);
            }

            template<bool s, typename std::enable_if<!s, bool>::type = true>
            void set_slot(size_t slot_idx, Shard* shard, std::memory_order order = std::memory_order_release) noexcept{
                this->slot[slot_idx] = shard;
            }
            template<bool s, typename std::enable_if<!s, bool>::type = true>
            Shard* get_slot(size_t slot_idx, std::memory_order order = std::memory_order_acquire) noexcept{
                return this->slot[slot_idx];
            }

            template<bool s, typename std::enable_if<!s, bool>::type = true>
            void set_hash(size_t hash_idx, Shard* shard, std::memory_order order = std::memory_order_release) noexcept{
                this->shards[hash_idx] = shard;
            }
            template<bool s,  typename std::enable_if<!s, bool>::type = true>
            Shard* get_hash(size_t hash_idx, std::memory_order order = std::memory_order_acquire) noexcept{
                return this->shards[hash_idx];
            }
            template<bool s, typename std::enable_if<!s, bool>::type = true>
            bool cas_hash(size_t hash_idx, Shard*& expected, Shard* new_shard, std::memory_order order = std::memory_order_acq_rel) noexcept{
                this->shards[hash_idx] = new_shard;
                return true;
            }
            template<bool s, typename std::enable_if<!s, bool>::type = true>
            Shard* exchange_hash(size_t hash_idx, Shard* new_shard, std::memory_order order = std::memory_order_acq_rel) noexcept{
                return std::exchange(this->shards[hash_idx], new_shard);
            }
        #endif

            static size_t hash(uint32_t ip, uint16_t port) noexcept{
                uint64_t combined = (static_cast<uint64_t>(ip) << 16) | port;
        
                combined ^= combined >> 33;
                combined *= 0xff51afd7ed558ccdLLU;
                combined ^= combined >> 33;
                combined *= 0xc4ceb9fe1a85ec53LLU;
                combined ^= combined >> 33;
                
                return static_cast<size_t>(combined % num_shards);
            }

            static constexpr size_t num_slot = 16384;
            static constexpr size_t num_shards = max_num_shards;
            conn_pointer_t slot[num_slot];
            conn_pointer_t shards[num_shards] = {};  // fixed array hash table
        };

        Router router;
        xnet::io_context& ctx;
        xredis::ClusterLongLastOption option;
    public:
        RedisCluster(xnet::io_context& ctx) noexcept: router(), ctx(ctx), option()
        {}

        RedisCluster(const RedisCluster&) = delete;

        ~RedisCluster(){
            for(size_t i = 0; i < max_num_shards; i++){
                Shard* shard = this->router.template get_hash<shared>(i, std::memory_order_relaxed);
                if(shard){
                    delete shard;
                }
            }
        }

        xnet::task<xnet::io_result<bool>> connect(const xredis::ClusterConnectionOption& option, int timeout=10) noexcept{
            xnet::io_result<xredis::RedisValue> response = co_await xredis::connect_cluster(this->ctx, option, timeout);
            if(response.err){
                co_return {false, response.err};
            }

            auto& slots = response->as<xredis::RedisValue::array_t>();
            std::vector<Shard*> temp;
            temp.reserve(slots.size());
            for(size_t i = 0; i < slots.size(); i++){
                RedisValue& slot = slots[i];
                auto& arr = slot.as<xredis::RedisValue::array_t>();
                if(arr.size() != 3 || !arr[0].is_integer() || !arr[1].is_integer() || !arr[2].is_array()){
                    co_return {false, EPROTO};
                }
                size_t begin = arr[0].as<xredis::RedisValue::integer_t>();
                size_t end = arr[1].as<xredis::RedisValue::integer_t>();
                auto& info = arr[2].as<xredis::RedisValue::array_t>();
                if(!(info[0].is_simple_string() || info[0].is_bulk_string()) || !info[1].is_integer()){
                    co_return {false, EPROTO};
                }
                std::string_view ip_str = info[0].as_string();
                uint16_t port = static_cast<uint16_t>(info[1].as<xredis::RedisValue::integer_t>());
                uint32_t ip = xredis::iptouint32(ip_str.data(), ip_str.size());
                if(ip == 0){
                    co_return {false, EPROTO};
                }

                size_t index = 0;
                Shard* shard = nullptr;
                for(; index < temp.size(); index++){
                    shard = temp[index];
                    if(shard->ip == ip && shard->port == port){
                        break;
                    }
                }
                if(index == temp.size()){
                    shard = new Shard(ip, port);
                    temp.emplace_back(shard);
                }
                for(size_t slot_idx = begin; slot_idx <= end; slot_idx++){
                    this->router.template set_slot<shared>(slot_idx, shard, std::memory_order_relaxed);
                }
            }
            this->option.set_option(option);
            for(Shard* shard : temp){
                size_t hash_idx = Router::hash(shard->ip, shard->port);
                size_t i = 0;
                for(; i < max_num_shards; i++){
                    size_t idx = (hash_idx + i) % max_num_shards;
                    Shard* cur = this->router.template get_hash<shared>(idx);
                    if(!cur){
                        this->router.template set_hash<shared>(idx, shard, std::memory_order_relaxed);
                        break;
                    }
                }
                if(i == max_num_shards){
                    co_return {false, ENOMEM};
                }
            }
            #ifdef XNET_DISABLE_THREAD_SAFE
                if constexpr(shared){
                    std::atomic_thread_fence(std::memory_order_release);
                }
            #endif
            co_return {true, 0};
        }

        Pipeline* get_pipe(uint16_t slot) noexcept{
            return this->try_connect(*this->router.template get_slot<shared>(slot));
        }

        void close() noexcept{
            for(size_t i = 0; i < max_num_shards; i++){
                Shard* shard = this->router.template exchange_hash<shared>(i, nullptr, std::memory_order_relaxed);
                if(shard){
                    delete shard;
                }
            }
            #ifdef XNET_DISABLE_THREAD_SAFE
                if constexpr(shared){
                    std::atomic_thread_fence(std::memory_order_release);
                }
            #endif
        }
        xredis::ClusterConnectionOption connection_option() const noexcept { return this->option; }
        xnet::io_context& context() noexcept { return this->ctx; }
    private:
        Pipeline* try_connect(Shard& shard) noexcept{
            int status = shard.template set_connecting<shared>();
            if(status == 0){
                Pipeline* pipe = new Pipeline(this->ctx);
                shard.pipe = pipe;
                int status = shard.template set_connected<shared>();
                if(status == 1){
                    char buffer[16];
                    size_t len = xredis::uint32toip(buffer, shard.ip);
                    pipe->option.ip = std::string(buffer, len);
                    pipe->option.username = this->option.username;
                    pipe->option.password = this->option.password;
                    pipe->option.clientname = this->option.clientname;
                    pipe->option.resp = this->option.resp;
                    pipe->option.port = shard.port;
                    #ifdef XREDIS_ENABLE_TLS
                        pipe->option.cacert = this->option.cacert;
                        pipe->option.cacertdir = this->option.cacertdir;
                        pipe->option.cert = this->option.cert;
                        pipe->option.key = this->option.key;
                        pipe->option.sni = this->option.sni;
                    #endif
                    pipe->template set_alive<shared>(true);
                    worker(pipe, *this);
                    return pipe;
                }
                else{
                    pipe->ring.drain(xredis::details::result());
                    delete pipe;
                    return nullptr;
                }
            }
            else if(status == 1){
                status = shard.template wait_connected<shared>();
            }
            return status != 2 ? nullptr : shard.pipe;
        }

        // 0 --- moved error and parse success
        // 1 --- slot error and parse success
        // 2 --- not moved or ask error
        // other --- parse error
        static int parse_error(std::string_view error_str, size_t& slot, uint32_t& ip, uint16_t& port) noexcept{
            int type = -1;
            size_t prefix_len = 0;
            if(error_str.starts_with("MOVED ")){
                type = 0;
                prefix_len = 6;
            }
            else if(error_str.starts_with("ASK ")){
                type = 1;
                prefix_len = 4;
            }
            else{
                return 2;
            }

            size_t space = error_str.find_first_of(' ', prefix_len);
            if(space == error_str.npos){
                return EPROTO;
            }

            std::string_view slot_sv = error_str.substr(prefix_len, space - prefix_len);
            std::string_view addr_sv = error_str.substr(space + 1);
            size_t colon = addr_sv.find_first_of(':');
            if(colon == addr_sv.npos){
                return EPROTO;
            }

            std::string_view ip_str = addr_sv.substr(0, colon);
            std::string_view port_sv = addr_sv.substr(colon + 1);

            ip = xredis::iptouint32(ip_str.data(), ip_str.size());
            if(ip == 0){
                return EPROTO;
            }
            auto res1 = std::from_chars(slot_sv.begin(), slot_sv.end(), slot);
            if (res1.ec != std::errc{}){
                return EINVAL;
            }

            auto res2 = std::from_chars(port_sv.begin(), port_sv.end(), port);
            if (res2.ec != std::errc{}){
                return EINVAL;
            }

            return type;
        }

        Shard* find_shard(uint32_t ip, uint16_t port) noexcept{
            size_t start_idx = Router::hash(ip, port);
            for(size_t i = 0; i < max_num_shards; i++){
                size_t idx = (start_idx + i) % max_num_shards;
                Shard* expected = this->router.template get_hash<shared>(idx);

                if(expected){
                    if(expected->ip == ip && expected->port == port){
                        return expected;
                    }
                    continue;
                }
                Shard* new_shard = new Shard(ip, port);
                if(this->router.template cas_hash<shared>(idx, expected, new_shard)){
                    return new_shard;
                }
                else{
                    delete new_shard;
                    if(expected->ip == ip && expected->port == port){
                        return expected;
                    } 
                }
            }
            return nullptr;
        }

        // 0 --- success
        // other --- other error
        static int handle_response(Pipeline& pipe, xnet::io_result<RedisValue>& val, RedisCluster& cluster) noexcept{
            std::coroutine_handle<> handle;
            std::string_view payload;
            pipe.ring.pop(handle, payload);
            RedisValue& result_slot = xredis::details::result();
            if(!val->is_error()){
                result_slot = val.move();
                handle.resume();
                return 0;
            }
            else{
                std::string_view err_str = val->as_error();
                size_t slot;
                uint32_t ip;
                uint16_t port;
                size_t res = parse_error(err_str, slot, ip, port);
                if(res > 2){
                    return res;
                }
                else if(res == 0){
                    // MOVED
                    Shard* shard = cluster.find_shard(ip, port);
                    cluster.router.template set_slot<shared>(slot, shard);
                    Pipeline* pipe = cluster.try_connect(*shard);
                    if(pipe == nullptr){
                        result_slot.set<xredis::RedisValue::simple_error_t>(xredis::RedisClientError::CONN_ERR);
                        handle.resume();
                        return 0;
                    }
                    // forward handler
                    int pos = pipe->ring.push(handle, payload);
                    if(pos < 0){
                        result_slot.set<xredis::RedisValue::simple_error_t>(
                            pos == -1 ? xredis::RedisClientError::RING_OVERFLOW : xredis::RedisClientError::CONN_ERR
                        );
                        handle.resume();
                        return 0;
                    }
                    size_t one = 1;
                    [[maybe_unused]] ssize_t n = ::write(shard->pipe->event.fd(), &one, sizeof(one));
                }
                else if(res == 1){
                    // ASKING
                    Shard* shard = cluster.find_shard(ip, port);
                    Pipeline* pipe = cluster.try_connect(*shard);
                    if(pipe == nullptr){
                        result_slot.set<xredis::RedisValue::simple_error_t>(xredis::RedisClientError::CONN_ERR);
                        handle.resume();
                        return 0;
                    }
                    // forward handler
                    static constexpr std::string_view asking = "*1\r\n$6\r\nASKING\r\n";
                    std::coroutine_handle<> handles[] = {std::noop_coroutine(), handle};
                    std::string_view payloads[] = {asking, payload};
                    int pos = pipe->ring.push_n(handles, payloads, 2);
                    if(pos < 0){
                        result_slot.set<xredis::RedisValue::simple_error_t>(
                            pos == -1 ? xredis::RedisClientError::RING_OVERFLOW : xredis::RedisClientError::CONN_ERR
                        );
                        handle.resume();
                        return 0;
                    }
                    size_t two = 2;
                    [[maybe_unused]] ssize_t n = ::write(shard->pipe->event.fd(), &two, sizeof(two));
                }
                else if(res == 2){
                    result_slot = val.move();
                    handle.resume();
                    return 0;
                }
            }
            return 0;
        }

        static xnet::task<xnet::io_result<void>> writer(Pipeline& pipe) noexcept{
            size_t num_evs = 0;
            constexpr size_t num_buffers = XREDIS_PIPELINE_BATCH_SIZE;
            iovec iovs[num_buffers];
            while(true){
                {
                    auto result = co_await pipe.event.read(&num_evs, sizeof(num_evs), 0);
                    if(result.err){
                        co_return result.err;
                    }
                }
                {
                    while(true){
                        num_evs = num_buffers; // do not trust the eventfd and try to drain ring every time
                        size_t total_bytes = pipe.ring.prep_writev(iovs, num_evs);
                        if(total_bytes == 0){
                            // fake awake could happend when reconnecting
                            break;
                        }
                        size_t bytes = 0;
                        iovec* iov_ptr = iovs;
                        while(true){
                            auto result = co_await pipe.client.writev(iov_ptr, num_evs, 0);
                            if(result.err || *result == 0){
                                co_return result.err ? result.err : ECONNRESET;
                            }
                            bytes += *result;
                            if(bytes == total_bytes){
                                break;
                            }
                            // size_t& consumed = *result;
                            while(true){
                                if(*result >= iov_ptr[0].iov_len){
                                    *result -= iov_ptr[0].iov_len;
                                    iov_ptr++;
                                    num_evs--;
                                }
                                else{
                                    iov_ptr[0].iov_base = static_cast<char*>(iov_ptr[0].iov_base) + *result;
                                    iov_ptr[0].iov_len -= *result;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            co_return -1;
        }

        static xnet::task<xnet::io_result<void>> reader(Pipeline& pipe, RedisCluster& cluster) noexcept{
            while(true){
                auto result = co_await pipe.parser.parse();
                if(result.err){
                    co_return result.err;
                }
                size_t error = handle_response(pipe, result, cluster);
                if(error){
                    co_return error;
                }

                RedisValue values[XREDIS_PIPELINE_BATCH_SIZE];
                int errs[XREDIS_PIPELINE_BATCH_SIZE];
                xnet::io_result<size_t> num = pipe.parser.try_parse_n(values, errs, XREDIS_PIPELINE_BATCH_SIZE);
                for(size_t i = 0; i < *num; i++){
                    size_t error = handle_response(pipe, result, cluster);
                    if(error){
                        co_return error;
                    }
                }
                if(num.err){
                    co_return num.err;
                }
            }
            co_return -1;
        }

        static xnet::detached_task worker(Pipeline* pipe_ptr, RedisCluster& cluster) noexcept{
            constexpr size_t max_backoff = 32;
            Pipeline& pipe = *pipe_ptr;
            xnet::AsyncTimer timer(cluster.ctx);
            int timed = 1;
            while(true){
                while(true){
                    auto ret = co_await xredis::connect_spot(
                        pipe.client,
                        pipe.parser,
                        pipe.option,
                        10
                    );
                    if(!pipe.template get_alive<shared>()){
                        delete pipe_ptr;
                        co_return;
                    }
                    if(ret.err){
                        auto ret = co_await timer.timeout(timed);
                        if(!pipe.template get_alive<shared>()){
                            delete pipe_ptr;
                            co_return;
                        }
                        timed *= 2;
                        timed = timed <= max_backoff ? timed : max_backoff;
                        continue;
                    }
                    else{
                        timed = 1;
                        break;
                    }
                }
                {
                    // success
                    co_await xnet::race(reader(pipe, cluster), writer(pipe));
                    if(!pipe.template get_alive<shared>()){
                        pipe.ring.drain(xredis::details::result());
                        delete pipe_ptr;
                        co_return;
                    }
                }

                // error recovery
                pipe.ring.drain(xredis::details::result());
                pipe.client.close();
                pipe.parser.reset();
                [[maybe_unused]] size_t dummy;
                [[maybe_unused]] ssize_t n = ::read(pipe.event.fd(), &dummy, sizeof(dummy));
                pipe.ring.reset();
            }
        }
    public:
        template<class... Args>
        auto command(std::string_view cmd, std::string_view key, Args&&... args) noexcept{
            uint16_t slot = xredis::get_slot(key.data(), key.size());
            return xredis::RedisCommandOperation<RedisCluster, 1, true, false>(
                this,
                slot,
                xredis::build_commands(cmd, key, std::string_view(std::forward<Args>(args))...)
            );
        }

        template<std::input_iterator InputIt>
        auto command_range(std::string_view cmd, std::string_view key, InputIt begin, size_t count) noexcept{
            uint16_t slot = xredis::get_slot(key.data(), key.size());
            return xredis::RedisCommandOperation<RedisCluster, 1, true, false>(
                this,
                slot,
                xredis::build_commands_from_range(cmd, key, begin, count)
            );
        }

        auto publish(std::string_view channel, std::string_view message) noexcept{
            return this->command("PUBLISH", channel, message);
        }

        auto spublish(std::string_view shardchannel, std::string_view message) noexcept{
            return this->command("SPUBLISH", shardchannel, message);
        }

        // ---- String ----
        auto append(std::string_view key, std::string_view value) noexcept{
            return this->command("APPEND", key, value);
        }

        auto decr(std::string_view key) noexcept{
            return this->command("DECR", key);
        }

        auto decrby(std::string_view key, int64_t decrement) noexcept{
            return this->command("DECRBY", key, details::to_string(decrement));
        }

        auto get(std::string_view key) noexcept{
            return this->command("GET", key);
        }

        auto getdel(std::string_view key) noexcept{
            return this->command("GETDEL", key);
        }

        template<typename... Args>
        auto getex(std::string_view key, Args&&... args) noexcept{
            return this->command("GETEX", key, std::forward<Args>(args)...);
        }

        auto getrange(std::string_view key, int64_t start, int64_t end) noexcept{
            return this->command("GETRANGE", key, details::to_string(start), details::to_string(end));
        }

        auto incr(std::string_view key) noexcept{
            return this->command("INCR", key);
        }

        auto incrby(std::string_view key, int64_t increment) noexcept{
            return this->command("INCRBY", key, details::to_string(increment));
        }

        auto incrbyfloat(std::string_view key, double increment) noexcept{
            return this->command("INCRBYFLOAT", key, details::to_string(increment));
        }

        template<typename... Args>
        auto lcs(std::string_view key1, std::string_view key2, Args&&... args) noexcept{
            return this->command("LCS", key1, key2, std::forward<Args>(args)...);
        }

        template<typename... Keys>
        auto mget(std::string_view first_key, Keys&&... other_keys) noexcept{
            return this->command("MGET", first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Args>
        requires (sizeof...(Args) > 0 && sizeof...(Args) % 2 == 0)
        auto mset(Args&&... args) noexcept{
            return this->command("MSET", std::forward<Args>(args)...);
        }

        template<typename... Args>
        requires (sizeof...(Args) > 0 && sizeof...(Args) % 2 == 0)
        auto msetnx(Args&&... args) noexcept{
            return this->command("MSETNX", std::forward<Args>(args)...);
        }

        auto psetex(std::string_view key, int64_t milliseconds, std::string_view value) noexcept{
            return this->command("PSETEX", key, details::to_string(milliseconds), value);
        }

        template<typename... Args>
        auto set(std::string_view key, std::string_view value, Args&&... args) noexcept{
            return this->command("SET", key, value, std::forward<Args>(args)...);
        }

        auto setex(std::string_view key, int64_t seconds, std::string_view value) noexcept{
            return this->command("SETEX", key, details::to_string(seconds), value);
        }

        auto setnx(std::string_view key, std::string_view value) noexcept{
            return this->command("SETNX", key, value);
        }

        auto setrange(std::string_view key, int64_t offset, std::string_view value) noexcept{
            return this->command("SETRANGE", key, details::to_string(offset), value);
        }

        auto strlen(std::string_view key) noexcept{
            return this->command("STRLEN", key);
        }

        // ---- Hash ----
        template<typename... Fields>
        auto hdel(std::string_view key, std::string_view first_field, Fields&&... other_fields) noexcept{
            return this->command("HDEL", key, first_field, std::forward<Fields>(other_fields)...);
        }

        auto hexists(std::string_view key, std::string_view field) noexcept{
            return this->command("HEXISTS", key, field);
        }

        template<typename... Args>
        auto hexpire(std::string_view key, int64_t seconds, Args&&... fields) noexcept{
            return this->command("HEXPIRE", key, details::to_string(seconds), std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hexpireat(std::string_view key, int64_t timestamp, Args&&... fields) noexcept{
            return this->command("HEXPIREAT", key, details::to_string(timestamp), std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hexpiretime(std::string_view key, Args&&... fields) &noexcept{
            return this->command("HEXPIRETIME", key, std::forward<Args>(fields)...);
        }

        auto hget(std::string_view key, std::string_view field) noexcept{
            return this->command("HGET", key, field);
        }

        auto hgetall(std::string_view key) noexcept{
            return this->command("HGETALL", key);
        }

        auto hgetdel(std::string_view key, std::string_view field) noexcept{
            return this->command("HGETDEL", key, field);
        }

        template<typename... Args>
        auto hgetex(std::string_view key, std::string_view field, Args&&... args) noexcept{
            return this->command("HGETEX", key, field, std::forward<Args>(args)...);
        }

        auto hincrby(std::string_view key, std::string_view field, int64_t increment) noexcept{
            return this->command("HINCRBY", key, field, details::to_string(increment));
        }

        auto hincrbyfloat(std::string_view key, std::string_view field, double increment) noexcept{
            return this->command("HINCRBYFLOAT", key, field, details::to_string(increment));
        }

        auto hkeys(std::string_view key) noexcept{
            return this->command("HKEYS", key);
        }

        auto hlen(std::string_view key) noexcept{
            return this->command("HLEN", key);
        }

        template<typename... Fields>
        auto hmget(std::string_view key, std::string_view first_field, Fields&&... other_fields) noexcept{
            return this->command("HMGET", key, first_field, std::forward<Fields>(other_fields)...);
        }

        template<typename... Args>
        auto hpersist(std::string_view key, Args&&... fields) noexcept{
            return this->command("HPERSIST", key, std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hpexpire(std::string_view key, int64_t milliseconds, Args&&... fields) noexcept{
            return this->command("HPEXPIRE", key, details::to_string(milliseconds), std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hpexpireat(std::string_view key, int64_t milliseconds_timestamp, Args&&... fields) noexcept{
            return this->command("HPEXPIREAT", key, details::to_string(milliseconds_timestamp), std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hpexpiretime(std::string_view key, Args&&... fields) noexcept{
            return this->command("HPEXPIRETIME", key, std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hpttl(std::string_view key, Args&&... fields) noexcept{
            return this->command("HPTTL", key, std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hrandfield(std::string_view key, Args&&... args) noexcept{
            return this->command("HRANDFIELD", key, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto hscan(std::string_view key, int64_t cursor, Args&&... args) noexcept{
            return this->command("HSCAN", key, details::to_string(cursor), std::forward<Args>(args)...);
        }

        template<typename... Args>
        requires (sizeof...(Args) >= 2 && sizeof...(Args) % 2 == 0)
        auto hset(std::string_view key, Args&&... args) noexcept{
            return this->command("HSET", key, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto hsetex(std::string_view key, Args&&... args) noexcept{
            return this->command("HSETEX", key, std::forward<Args>(args)...);
        }

        auto hsetnx(std::string_view key, std::string_view field, std::string_view value) noexcept{
            return this->command("HSETNX", key, field, value);
        }

        auto hstrlen(std::string_view key, std::string_view field) noexcept{
            return this->command("HSTRLEN", key, field);
        }

        template<typename... Args>
        auto httl(std::string_view key, Args&&... fields) noexcept{
            return this->command("HTTL", key, std::forward<Args>(fields)...);
        }

        auto hvals(std::string_view key) noexcept{
            return this->command("HVALS", key);
        }

        // ---- List ----
        auto blmove(std::string_view source, std::string_view destination, std::string_view wherefrom, std::string_view whereto, double timeout) noexcept{
            return this->command("BLMOVE", source, destination, wherefrom, whereto, details::to_string(timeout));
        }

        template<typename... Args>
        auto blmpop(double timeout, int64_t numkeys, std::string_view first_key, Args&&... args) noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return xredis::RedisCommandOperation<RedisCluster, 1, true, false>(
                this,
                slot,
                xredis::build_commands(
                    std::string_view("BLMPOP"),
                    details::to_string(timeout),
                    details::to_string(numkeys),
                    first_key, std::string_view(std::forward<Args>(args))...
                )
            );
        }

        template<typename... Keys>
        auto blpop(std::string_view first_key, Keys&&... other_keys_and_timeout) noexcept{
            return this->command("BLPOP", first_key, std::forward<Keys>(other_keys_and_timeout)...);
        }

        template<typename... Keys>
        auto brpop(std::string_view first_key, Keys&&... other_keys_and_timeout) noexcept{
            return this->command("BRPOP", first_key, std::forward<Keys>(other_keys_and_timeout)...);
        }

        auto lindex(std::string_view key, int64_t index) noexcept{
            return this->command("LINDEX", key, details::to_string(index));
        }

        auto linsert(std::string_view key, std::string_view where, std::string_view pivot, std::string_view element) noexcept{
            return this->command("LINSERT", key, where, pivot, element);
        }

        auto llen(std::string_view key) noexcept{
            return this->command("LLEN", key);
        }

        auto lmove(std::string_view source, std::string_view destination, std::string_view wherefrom, std::string_view whereto) noexcept{
            return this->command("LMOVE", source, destination, wherefrom, whereto);
        }

        template<typename... Args>
        auto lmpop(int64_t numkeys, std::string_view first_key, Args&&... args) noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return xredis::RedisCommandOperation<RedisCluster, 1, true, false>(
                this,
                slot,
                xredis::build_commands(
                    std::string_view("LMPOP"),
                    details::to_string(numkeys),
                    first_key, std::string_view(std::forward<Args>(args))...
                )
            );
        }

        template<typename... Args>
        auto lpop(std::string_view key, Args&&... count) noexcept{
            return this->command("LPOP", key, std::forward<Args>(count)...);
        }

        template<typename... Args>
        auto lpos(std::string_view key, std::string_view element, Args&&... args) noexcept{
            return this->command("LPOS", key, element, std::forward<Args>(args)...);
        }

        template<typename... Vals>
        auto lpush(std::string_view key, std::string_view first_val, Vals&&... other_vals) noexcept{
            return this->command("LPUSH", key, first_val, std::forward<Vals>(other_vals)...);
        }

        template<typename... Vals>
        auto lpushx(std::string_view key, std::string_view first_val, Vals&&... other_vals) noexcept{
            return this->command("LPUSHX", key, first_val, std::forward<Vals>(other_vals)...);
        }

        auto lrange(std::string_view key, int64_t start, int64_t stop) noexcept{
            return this->command("LRANGE", key, details::to_string(start), details::to_string(stop));
        }

        auto lrem(std::string_view key, int64_t count, std::string_view element) noexcept{
            return this->command("LREM", key, details::to_string(count), element);
        }

        auto lset(std::string_view key, int64_t index, std::string_view element) noexcept{
            return this->command("LSET", key, details::to_string(index), element);
        }

        auto ltrim(std::string_view key, int64_t start, int64_t stop) noexcept{
            return this->command("LTRIM", key, details::to_string(start), details::to_string(stop));
        }

        auto rpop(std::string_view key) noexcept{
            return this->command("RPOP", key);
        }

        template<typename... Vals>
        auto rpush(std::string_view key, std::string_view first_val, Vals&&... other_vals) noexcept{
            return this->command("RPUSH", key, first_val, std::forward<Vals>(other_vals)...);
        }

        template<typename... Vals>
        auto rpushx(std::string_view key, std::string_view first_val, Vals&&... other_vals) noexcept{
            return this->command("RPUSHX", key, first_val, std::forward<Vals>(other_vals)...);
        }

        // ---- Set ----

        template<typename... Members>
        auto sadd(std::string_view key, std::string_view first_member, Members&&... other_members) noexcept{
            return this->command("SADD", key, first_member, std::forward<Members>(other_members)...);
        }

        auto scard(std::string_view key) noexcept{
            return this->command("SCARD", key);
        }

        template<typename... Keys>
        auto sdiff(std::string_view first_key, Keys&&... other_keys) noexcept{
            return this->command("SDIFF", first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Keys>
        auto sdiffstore(std::string_view destination, std::string_view first_key, Keys&&... other_keys) noexcept{
            return this->command("SDIFFSTORE", destination, first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Keys>
        auto sinter(std::string_view first_key, Keys&&... other_keys) noexcept{
            return this->command("SINTER", first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Args>
        auto sintercard(int64_t numkeys, std::string_view first_key, Args&&... args) noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return xredis::RedisCommandOperation<RedisCluster, 1, true, false>(
                this,
                slot,
                xredis::build_commands(
                    std::string_view("SINTERCARD"),
                    details::to_string(numkeys),
                    first_key, std::string_view(std::forward<Args>(args))...
                )
            );
        }
        
        template<typename... Keys>
        auto sinterstore(std::string_view destination, std::string_view first_key, Keys&&... other_keys) noexcept{
            return this->command("SINTERSTORE", destination, first_key, std::forward<Keys>(other_keys)...);
        }

        auto sismember(std::string_view key, std::string_view member) & noexcept{
            return this->command("SISMEMBER", key, member);
        }

        auto smembers(std::string_view key) noexcept{
            return this->command("SMEMBERS", key);
        }

        template<typename... Members>
        auto smismember(std::string_view key, std::string_view first_member, Members&&... other_members) noexcept{
            return this->command("SMISMEMBER", key, first_member, std::forward<Members>(other_members)...);
        }

        auto smove(std::string_view source, std::string_view destination, std::string_view member) noexcept{
            return this->command("SMOVE", source, destination, member);
        }

        template<typename... Args>
        auto spop(std::string_view key, Args&&... count) noexcept{
            return this->command("SPOP", key, std::forward<Args>(count)...);
        }

        template<typename... Args>
        auto srandmember(std::string_view key, Args&&... count) noexcept{
            return this->command("SRANDMEMBER", key, std::forward<Args>(count)...);
        }

        template<typename... Members>
        auto srem(std::string_view key, std::string_view first_member, Members&&... other_members) noexcept{
            return this->command("SREM", key, first_member, std::forward<Members>(other_members)...);
        }

        template<typename... Args>
        auto sscan(std::string_view key, int64_t cursor, Args&&... args) noexcept{
            return this->command("SSCAN", key, details::to_string(cursor), std::forward<Args>(args)...);
        }

        template<typename... Keys>
        auto sunion(std::string_view first_key, Keys&&... other_keys) noexcept{
            return this->command("SUNION", first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Keys>
        auto sunionstore(std::string_view destination, std::string_view first_key, Keys&&... other_keys) noexcept{
            return this->command("SUNIONSTORE", destination, first_key, std::forward<Keys>(other_keys)...);
        }

        // ---- ZSet ----
        template<typename... Args>
        auto zadd(std::string_view key, Args&&... args) noexcept{
            return this->command("ZADD", key, std::forward<Args>(args)...);
        }

        auto zcard(std::string_view key) noexcept{
            return this->command("ZCARD", key);
        }

        auto zcount(std::string_view key, double min, double max) noexcept{
            return this->command("ZCOUNT", key, details::to_string(min), details::to_string(max));
        }

        template<typename... Keys>
        auto zdiff(size_t numkeys, std::string_view first_key, Keys&&... other_keys) noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return xredis::RedisCommandOperation<RedisCluster, 1, true, false>(
                this,
                slot,
                xredis::build_commands(
                    std::string_view("ZDIFF"),
                    details::to_string(numkeys),
                    first_key, std::string_view(std::forward<Keys>(other_keys))...
                )
            );
        }

        template<typename... Keys>
        auto zdiffstore(std::string_view destination, size_t numkeys, std::string_view first_key, Keys&&... other_keys) noexcept{
            return this->command("ZDIFFSTORE", destination, details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }

        auto zincrby(std::string_view key, double increment, std::string_view member) noexcept{
            return this->command("ZINCRBY", key, details::to_string(increment), member);
        }

        template<typename... Keys>
        auto zinter(size_t numkeys, std::string_view first_key, Keys&&... other_keys) noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return xredis::RedisCommandOperation<RedisCluster, 1, true, false>(
                this,
                slot,
                xredis::build_commands(
                    std::string_view("ZINTER"),
                    details::to_string(numkeys),
                    first_key, std::string_view(std::forward<Keys>(other_keys))...
                )
            );
        }

        template<typename... Args>
        auto zintercard(size_t numkeys, std::string_view first_key, Args&&... args) noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return xredis::RedisCommandOperation<RedisCluster, 1, true, false>(
                this,
                slot,
                xredis::build_commands(
                    std::string_view("ZINTERCARD"),
                    details::to_string(numkeys),
                    first_key, std::string_view(std::forward<Args>(args))...
                )
            );
        }

        template<typename... Args>
        auto zmpop(size_t numkeys, std::string_view first_key, Args&&... args) noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return xredis::RedisCommandOperation<RedisCluster, 1, true, false>(
                this,
                slot,
                xredis::build_commands(
                    std::string_view("ZMPOP"),
                    details::to_string(numkeys),
                    first_key, std::string_view(std::forward<Args>(args))...
                )
            );
        }

        template<typename... Members>
        auto zmscore(std::string_view key, std::string_view first_member, Members&&... other_members) noexcept{
            return this->command("ZMSCORE", key, first_member, std::forward<Members>(other_members)...);
        }

        template<typename... Args>
        auto zrandmember(std::string_view key, Args&&... args) noexcept{
            return this->command("ZRANDMEMBER", key, std::forward<Args>(args)...);
        }

        template<typename... Options>
        auto zrange(std::string_view key, std::string_view start, std::string_view stop, Options&&... options) noexcept{
            return this->command("ZRANGE", key, start, stop, std::forward<Options>(options)...);
        }

        template<typename... Args>
        auto zrank(std::string_view key, std::string_view member, Args&&... args) noexcept{
            return this->command("ZRANK", key, member, std::forward<Args>(args)...);
        }

        template<typename... Members>
        auto zrem(std::string_view key, std::string_view first_member, Members&&... other_members) noexcept{
            return this->command("ZREM", key, first_member, std::forward<Members>(other_members)...);
        }

        template<typename... Args>
        auto zscan(std::string_view key, size_t cursor, Args&&... args) noexcept{
            return this->command("ZSCAN", key, details::to_string(cursor), std::forward<Args>(args)...);
        }

        auto zscore(std::string_view key, std::string_view member) noexcept{
            return this->command("ZSCORE", key, member);
        }

        template<typename... Keys>
        auto zunion(size_t numkeys, std::string_view first_key, Keys&&... other_keys) noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return xredis::RedisCommandOperation<RedisCluster, 1, true, false>(
                this,
                slot,
                xredis::build_commands(
                    std::string_view("ZUNION"),
                    details::to_string(numkeys),
                    first_key, std::string_view(std::forward<Keys>(other_keys))...
                )
            );
        }

        template<typename... Keys>
        auto zunionstore(std::string_view destination, size_t numkeys, std::string_view first_key, Keys&&... other_keys) noexcept{
            return this->command("ZUNIONSTORE", destination, details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }

        // ---- HyperLogLog ----

        template<typename... Args>
        auto pfadd(std::string_view key, Args&&... elements) noexcept{
            return this->command("PFADD", key, std::forward<Args>(elements)...);
        }

        template<typename... Keys>
        auto pfcount(std::string_view first_key, Keys&&... other_keys) noexcept{
            return this->command("PFCOUNT", first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... SrcKeys>
        auto pfmerge(std::string_view destkey, std::string_view first_srckey, SrcKeys&&... other_srckeys) noexcept{
            return this->command("PFMERGE", destkey, first_srckey, std::forward<SrcKeys>(other_srckeys)...);
        }

        // ---- Bitmap ----
        auto setbit(std::string_view key, size_t offset, std::string_view value) noexcept{
            return this->command("SETBIT", key, details::to_string(offset), value);
        }

        auto getbit(std::string_view key, size_t offset) noexcept{
            return this->command("GETBIT", key, details::to_string(offset));
        }

        template<typename... Args>
        auto bitcount(std::string_view key, int64_t start, int64_t stop, Args&&... args) noexcept{
            return this->command("BITCOUNT", key, details::to_string(start), details::to_string(stop), std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto bitpos(std::string_view key, std::string_view bit, Args&&... args) noexcept{
            return this->command("BITPOS", key, bit, std::forward<Args>(args)...);
        }

        template<typename... SrcKeys>
        auto bitop(std::string_view operation, std::string_view destkey, std::string_view first_srckey, SrcKeys&&... other_srckeys) noexcept{
            uint16_t slot = xredis::get_slot(destkey.data(), destkey.size());
            return xredis::RedisCommandOperation<RedisCluster, 1, true, false>(
                this,
                slot,
                xredis::build_commands(
                    std::string_view("BITOP"),
                    operation, destkey, first_srckey,
                    std::string_view(std::forward<SrcKeys>(other_srckeys))...
                )
            );
        }

        template<typename... Args>
        auto bitfield(std::string_view key, Args&&... args) noexcept{
            return this->command("BITFIELD", key, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto bitfield_ro(std::string_view key, Args&&... args) noexcept{
            return this->command("BITFIELD_RO", key, std::forward<Args>(args)...);
        }

        // ---- Stream ----
        template<typename... Args>
        auto xadd(std::string_view key, Args&&... args) noexcept{
            return this->command("XADD", key, std::forward<Args>(args)...);
        }

        template<typename... Ids>
        auto xack(std::string_view key, std::string_view group, std::string_view first_id, Ids&&... other_ids) noexcept{
            return this->command("XACK", key, group, first_id, std::forward<Ids>(other_ids)...);
        }

        template<typename... Args>
        auto xautoclaim(std::string_view key, std::string_view group, std::string_view consumer, size_t min_idle_time, std::string_view start, Args&&... args) noexcept{
            return this->command("XAUTOCLAIM", key, group, consumer, details::to_string(min_idle_time), start, std::forward<Args>(args)...);
        }

        template<typename... Ids>
        auto xdel(std::string_view key, std::string_view first_id, Ids&&... other_ids) noexcept{
            return this->command("XDEL", key, first_id, std::forward<Ids>(other_ids)...);
        }

        template<typename... Args>
        auto xgroup_create(std::string_view key, std::string_view group, std::string_view id, Args&&... args) noexcept{
            return this->command("XGROUP", "CREATE", key, group, id, std::forward<Args>(args)...);
        }

        auto xgroup_createconsumer(std::string_view key, std::string_view group, std::string_view consumer) noexcept{
            return this->command("XGROUP", "CREATECONSUMER", key, group, consumer);
        }

        auto xgroup_delconsumer(std::string_view key, std::string_view group, std::string_view consumer) noexcept{
            return this->command("XGROUP", "DELCONSUMER", key, group, consumer);
        }

        auto xgroup_destroy(std::string_view key, std::string_view group) noexcept{
            return this->command("XGROUP", "DESTROY", key, group);
        }

        template<typename... Args>
        auto xgroup_setid(std::string_view key, std::string_view group, std::string_view id, Args&&... args) noexcept{
            return this->command("XGROUP", "SETID", key, group, id, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto xinfo_consumers(std::string_view key, std::string_view group) noexcept{
            return this->command("XINFO", "CONSUMERS", key, group);
        }

        auto xinfo_groups(std::string_view key) noexcept{
            return this->command("XINFO", "GROUPS", key);
        }

        template<typename... Args>
        auto xinfo_stream(std::string_view key, Args&&... args) noexcept{
            return this->command("XINFO", "STREAM", key, std::forward<Args>(args)...);
        }

        auto xlen(std::string_view key) noexcept{
            return this->command("XLEN", key);
        }

        template<typename... Args>
        auto xpending(std::string_view key, std::string_view group, Args&&... args) noexcept{
            return this->command("XPENDING", key, group, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto xrange(std::string_view key, std::string_view start, std::string_view end, Args&&... args) noexcept{
            return this->command("XRANGE", key, start, end, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto xreadgroup(std::string_view group_keyword, std::string_view group, std::string_view consumer, Args&&... args) noexcept{
            return this->command("XREADGROUP", group_keyword, group, consumer, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto xtrim(std::string_view key, Args&&... args) noexcept{
            return this->command("XTRIM", key, std::forward<Args>(args)...);
        }

        // ---- Scripting ----

        template<typename... Args>
        auto eval(std::string_view script, size_t numkeys, Args&&... args) noexcept{
            return this->command("EVAL", script, details::to_string(numkeys), std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto evalsha(std::string_view sha1, size_t numkeys, Args&&... args) noexcept{
            return this->command("EVALSHA", sha1, details::to_string(numkeys), std::forward<Args>(args)...);
        }

        auto script_load(std::string_view script) noexcept{
            return this->command("SCRIPT", "LOAD", script);
        }

        // ---- Transaction ----
        template<typename... Keys>
        auto watch(std::string_view first_key, Keys&&... other_keys) noexcept{
            return this->command("WATCH", first_key, std::forward<Keys>(other_keys)...);
        }

        auto unwatch(std::string_view tag) noexcept{
            uint16_t slot = xredis::get_slot(tag.data(), tag.size());
            return xredis::RedisCommandOperation<RedisCluster, 1, true, false>(
                this,
                slot,
                xredis::build_commands(std::string_view("UNWATCH"))
            );
        }

        auto multi(std::string_view tag) noexcept{
            uint16_t slot = xredis::get_slot(tag.data(), tag.size());
            return xredis::RedisCommandOperation<RedisCluster, 1, true, true>(
                this,
                slot,
                xredis::build_commands(std::string_view("MULTI"))
            );
        }

        // ---- Generic ----

        template<typename... Args>
        auto copy(std::string_view source, std::string_view destination, Args&&... args) noexcept{
            return this->command("COPY", source, destination, std::forward<Args>(args)...);
        }

        template<typename... Keys>
        auto del(std::string_view first_key, Keys&&... other_keys) noexcept{
            return this->command("DEL", first_key, std::forward<Keys>(other_keys)...);
        }

        auto dump(std::string_view key)  noexcept{
            return this->command("DUMP", key);
        }

        auto exists(std::string_view key) noexcept{
            return this->command("EXISTS", key);
        }

        template<typename... Args>
        auto expire(std::string_view key, size_t seconds, Args&&... args) noexcept{
            return this->command("EXPIRE", key, details::to_string(seconds), std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto expireat(std::string_view key, size_t unix_time_seconds, Args&&... args) noexcept{
            return this->command("EXPIREAT", key, details::to_string(unix_time_seconds), std::forward<Args>(args)...);
        }

        auto expiretime(std::string_view key) noexcept{
            return this->command("EXPIRETIME", key);
        }

        auto keys(std::string_view pattern) noexcept{
            return this->command("KEYS", pattern);
        }

        auto move(std::string_view key, size_t db) noexcept{
            return this->command("MOVE", key, details::to_string(db));
        }

        auto persist(std::string_view key) noexcept{
            return this->command("PERSIST", key);
        }

        template<typename... Args>
        auto pexpire(std::string_view key, size_t milliseconds, Args&&... args) noexcept{
            return this->command("PEXPIRE", key, details::to_string(milliseconds), std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto pexpireat(std::string_view key, size_t unix_time_milliseconds, Args&&... args) noexcept{
            return this->command("PEXPIREAT", key, details::to_string(unix_time_milliseconds), std::forward<Args>(args)...);
        }

        auto pexpiretime(std::string_view key) noexcept{
            return this->command("PEXPIRETIME", key);
        }

        auto pttl(std::string_view key) noexcept{
            return this->command("PTTL", key);
        }

        auto rename(std::string_view key, std::string_view newkey) noexcept{
            return this->command("RENAME", key, newkey);
        }

        auto renamenx(std::string_view key, std::string_view newkey) noexcept{
            return this->command("RENAMENX", key, newkey);
        }

        template<typename... Args>
        auto scan(size_t cursor, Args&&... args) noexcept{
            return this->command("SCAN", details::to_string(cursor), std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto sort(std::string_view key, Args&&... args) noexcept{
            return this->command("SORT", key, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto sort_ro(std::string_view key, Args&&... args) noexcept{
            return this->command("SORT_RO", key, std::forward<Args>(args)...);
        }

        template<typename... Keys>
        auto touch(std::string_view first_key, Keys&&... other_keys) noexcept{
            return this->command("TOUCH", first_key, std::forward<Keys>(other_keys)...);
        }

        auto ttl(std::string_view key) noexcept{
            return this->command("TTL", key);
        }

        auto type(std::string_view key) noexcept{
            return this->command("TYPE", key);
        }

        template<typename... Keys>
        auto unlink(std::string_view first_key, Keys&&... other_keys) noexcept{
            return this->command("UNLINK", first_key, std::forward<Keys>(other_keys)...);
        }
    };
};

#endif