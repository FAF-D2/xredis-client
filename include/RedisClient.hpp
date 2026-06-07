#ifndef redis_client_hpp
#define redis_client_hpp
#include"xnet.hpp"
#include"RedisConnection.h"
#include"RedisParser.h"
#include"RedisValue.h"
#include"RedisCommander.hpp"
#include<string>
#include<string_view>
#include<iterator>


namespace xredis{
    template<bool shared = false, size_t xcapacity = 4096>
    class RedisClient{
        using Pipeline = xredis::Pipe<shared, xcapacity>;

        Pipeline* pipe;
        xnet::io_context& ctx;
    public:
        xnet::task<xnet::io_result<bool>> connect(const xredis::ConnectionOption& option, int timeout=10) noexcept{
            Pipeline* pipe = new Pipeline(this->ctx);
            auto response = co_await xredis::connect_spot(
                pipe->client,
                pipe->parser,
                option,
                timeout
            );
            if(response.err){
                delete pipe;
                co_return {false, response.err};
            }
            pipe->option.set_option(option);
            this->pipe = pipe;
            this->pipe->template set_alive<shared>(true);
            worker(this->pipe);
            co_return {true, 0};
        }

        void close() noexcept{
            auto pipe = std::exchange(this->pipe, nullptr);
            if(pipe){
                pipe->template set_alive<shared>(false);
                pipe->client.close();
            }
        }

        RedisClient(xnet::io_context& ctx) noexcept: pipe(nullptr), ctx(ctx)
        {}
        RedisClient(RedisClient&& other) noexcept: pipe(std::exchange(other.pipe, nullptr)), ctx(other.ctx)
        {}
        RedisClient& operator=(RedisClient&& other) = delete;

        ~RedisClient(){
            if(this->pipe){
                this->pipe->template set_alive<shared>(false);
                this->pipe->client.shutdown();
            }
        }
        
        xredis::ConnectionOption connection_option() const noexcept { return this->pipe->option; }
        xnet::io_context& context() noexcept { return this->ctx; }
    private:
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

        static xnet::task<xnet::io_result<void>> reader(Pipeline& pipe) noexcept{
            while(true){
                auto result = co_await pipe.parser.parse();
                if(result.err){
                    co_return result.err;
                }
                std::coroutine_handle<> handle;
                [[maybe_unused]] std::string_view payload;
                pipe.ring.pop(handle, payload);
                xredis::details::result() = result.move();
                handle.resume();

                RedisValue values[XREDIS_PIPELINE_BATCH_SIZE];
                int errs[XREDIS_PIPELINE_BATCH_SIZE];
                xnet::io_result<size_t> num = pipe.parser.try_parse_n(values, errs, XREDIS_PIPELINE_BATCH_SIZE);
                for(size_t i = 0; i < *num; i++){
                    std::coroutine_handle<> handle;
                    [[maybe_unused]] std::string_view payload;
                    pipe.ring.pop(handle, payload);
                    xredis::details::result() = std::move(values[i]);
                    handle.resume();
                }
                if(num.err){
                    co_return num.err;
                }
            }
            co_return -1;
        }

        static xnet::detached_task worker(Pipeline* pipe_ptr) noexcept{
            constexpr size_t max_backoff = 32;
            Pipeline& pipe = *pipe_ptr;
            xnet::AsyncTimer timer(pipe.client.context());
            int timed = 1;
            while(true){
                {
                    co_await xnet::race(reader(pipe), writer(pipe));
                    if(!pipe.template get_alive<shared>()){
                        pipe.ring.drain(xredis::details::result());
                        delete pipe_ptr;
                        co_return;
                    }
                }
                {
                    // error and reconnecting
                    pipe.ring.drain(xredis::details::result());
                    pipe.client.close();
                    pipe.parser.reset();
                    [[maybe_unused]] size_t dummy;
                    [[maybe_unused]] ssize_t n = ::read(pipe.event.fd(), &dummy, sizeof(dummy));
                    pipe.ring.reset();
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
                }
            }
        }
    public:
        template<class... Args>
        auto command(Args&&... args) noexcept{
            return xredis::CommandAwaiter<Pipeline>(this->pipe, xredis::build_commands(std::string_view(args)...));
        }
        template<std::input_iterator InputIt>
        auto command_range(std::string_view cmd, InputIt begin, size_t count) noexcept{
            return xredis::CommandAwaiter<Pipeline>(this->pipe, xredis::build_commands_from_range(cmd, begin, count));
        }
        template<std::input_iterator InputIt>
        auto command_range(std::string_view cmd, std::string_view key, InputIt begin, size_t count) noexcept{
            return xredis::CommandAwaiter<Pipeline>(this->pipe, xredis::build_commands_from_range(cmd, key, begin, count));
        }

        auto ping()  noexcept{
            return this->command("PING");
        }

        auto select(size_t index)  noexcept{
            return this->command("SELECT", details::to_string(index));
        }

        auto flushall(bool async = false) noexcept{
            return this->command("FLUSHALL", async ? "ASYNC" : "SYNC");
        }

        auto flushdb(bool async = false) noexcept{
            return this->command("FLUSHDB", async ? "ASYNC" : "SYNC");
        }

        auto dbsize() noexcept{
            return this->command("DBSIZE");
        }

        auto save() noexcept{
            return this->command("SAVE");
        }

        template<typename... Args>
        auto shutdown(Args&&... args) noexcept{
            return this->command("SHUTDOWN", std::forward<Args>(args)...);
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
            return this->command("BLMPOP", details::to_string(timeout), details::to_string(numkeys), first_key, std::forward<Args>(args)...);
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
            return this->command("LMPOP", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
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
            return this->command("SINTERCARD", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
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
            return this->command("ZDIFF", details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
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
            return this->command("ZINTER", details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Args>
        auto zintercard(size_t numkeys, std::string_view first_key, Args&&... args) noexcept{
            return this->command("ZINTERCARD", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto zmpop(size_t numkeys, std::string_view first_key, Args&&... args) noexcept{
            return this->command("ZMPOP", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
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
            return this->command("ZUNION", details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
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
            return this->command("BITOP", operation, destkey, first_srckey, std::forward<SrcKeys>(other_srckeys)...);
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
        auto xread(Args&&... args) noexcept{
            return this->command("XREAD", std::forward<Args>(args)...);
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

        auto unwatch(std::string_view tag = "") noexcept{
            return this->command("UNWATCH");
        }

        auto multi(std::string_view tag = "") noexcept{
            return this->command("MULTI");
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

        auto randomkey() noexcept{
            return this->command("RANDOMKEY");
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

        auto wait(size_t numreplicas, size_t timeout) noexcept{
            return this->command("WAIT", details::to_string(numreplicas), details::to_string(timeout));
        }

        auto waitaof(size_t numlocal, size_t numreplicas, size_t timeout) noexcept{
            return this->command("WAITAOF", details::to_string(numlocal), details::to_string(numreplicas), details::to_string(timeout));
        }
    };
};

#endif