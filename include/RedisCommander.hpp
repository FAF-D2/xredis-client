#ifndef redis_commander_hpp
#define redis_commander_hpp
#include"RedisValue.h"
#include"RedisConnection.h"
#include<string>
#include<string_view>
#include<utility>
#include<type_traits>
#include<charconv>
#include<bit>
#include<iterator>
#include<coroutine>
#include<array>

namespace xredis{
    namespace details{
        // For Command Awaiter
        inline xredis::RedisValue& result() noexcept{
            static thread_local xredis::RedisValue value;
            return value;
        }

        inline const size_t& event_signal() noexcept{
            static constexpr size_t one = 1;
            return one;
        }

        static inline constexpr size_t bit_table[128] = {
            1, 1, 3, 1, 7, 
            1, 9, 1, 31, 2, 
            63, 2, 99, 2, 255, 
            3, 511, 3, 999, 3, 
            2047, 4, 4095, 4, 8191, 
            4, 9999, 4, 32767, 5, 
            65535, 5, 99999, 5, 262143, 
            6, 524287, 6, 999999, 6, 
            2097151, 7, 4194303, 7, 8388607, 
            7, 9999999, 7, 33554431, 8, 
            67108863, 8, 99999999, 8, 268435455, 
            9, 536870911, 9, 999999999, 9, 
            2147483647, 10, 4294967295, 10, 8589934591, 
            10, 9999999999, 10, 34359738367, 11, 
            68719476735, 11, 99999999999, 11, 274877906943, 
            12, 549755813887, 12, 999999999999, 12, 
            2199023255551, 13, 4398046511103, 13, 8796093022207, 
            13, 9999999999999ULL, 13, 35184372088831, 14, 
            70368744177663, 14, 99999999999999ULL, 14, 281474976710655ULL, 
            15, 562949953421311, 15, 999999999999999ULL, 15, 
            2251799813685247, 16, 4503599627370495, 16, 9007199254740991ULL, 
            16, 9999999999999999ULL, 16, 36028797018963967ULL, 17, 
            72057594037927935, 17, 99999999999999999ULL, 17, 288230376151711743ULL, 
            18, 576460752303423487, 18, 999999999999999999ULL, 18, 
            2305843009213693951, 19, 4611686018427387903, 19, 9223372036854775807ULL, 
            19, 9999999999999999999ULL, 19
        };

        static inline constexpr size_t count_digits(size_t integral) noexcept{
            return integral == 0 ? 1 :
            static_cast<size_t>(
                bit_table[2 * (63 - std::countl_zero(integral)) + 1]
                + (integral > bit_table[2 * (63 - std::countl_zero(integral))])
            );
        }

        static inline void command_unpack(std::string& wbuffer, std::string_view arg, char* buf) noexcept{
            auto [ptr, err] = std::to_chars(buf, buf + 24, arg.size());
            wbuffer.append("$");
            wbuffer.append(buf, ptr - buf);
            wbuffer.append("\r\n");
            wbuffer.append(arg.data(), arg.size());
            wbuffer.append("\r\n");
        }

        template<typename IntType>
        requires std::is_integral_v<std::decay_t<IntType>>
        static inline std::string to_string(IntType integer) noexcept{
            char buf[24];
            auto [ptr, err] = std::to_chars(buf, buf + sizeof(buf), integer);
            return err != std::errc{} ? std::string() : std::string(buf, ptr - buf);
        }

        static inline std::string to_string(double doublev) noexcept{
            char buf[48];
            auto [ptr, err] = std::to_chars(buf, buf + sizeof(buf), doublev);
            return err != std::errc{} ? std::string() : std::string(buf, ptr - buf);
        }

        struct get_handler{
            std::coroutine_handle<> handler;

            bool await_ready() const noexcept { return false; }
            
            bool await_suspend(std::coroutine_handle<> h) noexcept{
                this->handler = h;
                return false;
            }

            std::coroutine_handle<> await_resume() noexcept{
                return this->handler;
            }
        };

        template<size_t... Is>
        static consteval auto generate_tuple_result_t(std::index_sequence<Is...>){
            return std::tuple<decltype((void)Is, RedisValue{})...>{};
        }
    }

    template<class... Args>
    inline std::string build_commands(Args&&... args) noexcept{
        // string view args
        constexpr size_t num_args = sizeof...(args);

        char buf[24];
        auto [ptr, err] = std::to_chars(buf, buf + 24, num_args);
        size_t cnt = ptr - buf;

        size_t args_size[] = {
            args.size()...
        };
        size_t command_size = 5 * num_args + 3 + cnt;
        for(auto s: args_size){
            command_size += s + details::count_digits(s);
        }
        std::string wbuffer;
        wbuffer.reserve(command_size);

        // *<argc>\r\n
        wbuffer.append("*");
        wbuffer.append(buf, cnt);
        wbuffer.append("\r\n");

        // $<len>\r\n<arg>\r\n
        (details::command_unpack(wbuffer, args, buf), ...);
        return wbuffer;
    }

    template<std::input_iterator InputIt>
    inline std::string build_commands_from_range(std::string_view cmd, InputIt begin, size_t count) noexcept{
        size_t num_args = 1 + count;
        char buf[24];
        auto [ptr, err] = std::to_chars(buf, buf + 24, num_args);
        size_t cnt = ptr - buf;

        size_t command_size = 6 * num_args + 3 + cnt + cmd.size();
        std::string wbuffer;
        wbuffer.reserve(command_size);
        // *<argc>\r\n
        wbuffer.append("*");
        wbuffer.append(buf, cnt);
        wbuffer.append("\r\n");

        details::command_unpack(wbuffer, cmd, buf);
        for(size_t i = 0; i < count; i++){
            std::string_view arg = *begin;
            ++begin;
            details::command_unpack(wbuffer, arg, buf);
        }
        return wbuffer;
    }

    template<std::input_iterator InputIt>
    inline std::string build_commands_from_range(std::string_view cmd, std::string_view key, InputIt begin, size_t count) noexcept{
        size_t num_args = 2 + count;
        char buf[24];
        auto [ptr, err] = std::to_chars(buf, buf + 24, num_args);
        size_t cnt = ptr - buf;
        size_t command_size = 6 * num_args + 3 + cnt + cmd.size() + key.size();
        std::string wbuffer;
        wbuffer.reserve(command_size);
        // *<argc>\r\n
        wbuffer.append("*");
        wbuffer.append(buf, cnt);
        wbuffer.append("\r\n");

        details::command_unpack(wbuffer, cmd, buf);
        details::command_unpack(wbuffer, key, buf);
        for(size_t i = 0; i < count; i++){
            std::string_view arg = *begin;
            ++begin;
            details::command_unpack(wbuffer, arg, buf);
        }
        return wbuffer;
    }

    template<class T, size_t num_ops, bool, bool>
    class RedisCommandOperation;

    template<class Pipe>
    class [[nodiscard]]CommandAwaiter{
        Pipe* pipe;
        std::coroutine_handle<> handler;
        std::string data;
    public:
        template<class String>
        CommandAwaiter(Pipe* pipe, String&& payload) 
        noexcept: pipe(pipe), handler(nullptr), data(std::forward<String>(payload))
        {}
        CommandAwaiter(CommandAwaiter&&) = default;
        ~CommandAwaiter() = default;
        
        // Do NOT use this function at user space as it is just for hook && any detection
        xnet::io_result<bool> cancel() noexcept {
            return {true, 0};
        }
        std::coroutine_handle<>& handle() noexcept{ return this->handler; }

        bool await_ready() const noexcept { return false; }

        template<class Promise>
        bool await_suspend(std::coroutine_handle<Promise> h) noexcept{
            this->handler = h;
            xredis::RedisValue& result = xredis::details::result();
            if constexpr(requires{ h.promise().xcoro_hook(this); }){
                if(!h.promise().xcoro_hook(this)){
                    result.set<xredis::RedisValue::simple_error_t>(xredis::RedisClientError::OP_CANCELLED);
                    return false;
                }
            }
            
            int pos = pipe->ring.push(h, this->data);
            if(pos >= 0){
                auto& ctx = pipe->event.context();
                bool prep_success = false;

                ctx.lock();
                io_uring* ring = ctx.native();
                io_uring_sqe* sqe = io_uring_get_sqe(ring);
                if(sqe != nullptr){
                    io_uring_prep_write(sqe, pipe->event.fd(), &details::event_signal(), sizeof(size_t), 0);
                    io_uring_sqe_set_data(sqe, nullptr);
                    prep_success = true;
                }
                ctx.unlock();

                if(!prep_success){
                    [[maybe_unused]] ssize_t n = ::write(pipe->event.fd(), &details::event_signal(), sizeof(size_t));
                }
                return true;
            }
            result.set<xredis::RedisValue::simple_error_t>(
                pos == -1 ? xredis::RedisClientError::RING_OVERFLOW : xredis::RedisClientError::CONN_ERR
            );
            return false;
        }

        xredis::RedisValue await_resume() noexcept{
            this->handler = nullptr;
            return std::move(xredis::details::result());
        }

        auto chain(std::string&& new_op) & noexcept{
            return RedisCommandOperation<Pipe, 2, false, false>(this->pipe, this->data, std::move(new_op));
        }
        auto chain(std::string&& new_op) && noexcept{
            return RedisCommandOperation<Pipe, 2, false, false>(this->pipe, std::move(this->data), std::move(new_op));
        }

        template<class... Args>
        auto command(Args&&... args) & noexcept{
            return this->chain(xredis::build_commands(std::string_view(args)...));
        }
        template<class... Args>
        auto command(Args&&... args) && noexcept{
            return std::move(*this).chain(xredis::build_commands(std::string_view(args)...));
        }
        template<std::input_iterator InputIt>
        auto command_range(std::string_view cmd, InputIt begin, size_t count) & noexcept{
            return this->chain(xredis::build_commands_from_range(cmd, begin, count));
        }
        template<std::input_iterator InputIt>
        auto command_range(std::string_view cmd, InputIt begin, size_t count) && noexcept{
            return std::move(*this).chain(xredis::build_commands_from_range(cmd, begin, count));
        }

        auto ping() & noexcept{
            return this->command("PING");
        }
        auto ping() && noexcept{
            return std::move(*this).command("PING");
        }

        auto select(size_t index) & noexcept{
            return this->command("SELECT", details::to_string(index));
        }
        auto select(size_t index) && noexcept{
            return std::move(*this).command("SELECT", details::to_string(index));
        }

        auto flushall(bool async = false) & noexcept{
            return this->command("FLUSHALL", async ? "ASYNC" : "SYNC");
        }
        auto flushall(bool async = false) && noexcept{
            return std::move(*this).command("FLUSHALL", async ? "ASYNC" : "SYNC");
        }

        auto flushdb(bool async = false) & noexcept{
            return this->command("FLUSHDB", async ? "ASYNC" : "SYNC");
        }
        auto flushdb(bool async = false) && noexcept{
            return std::move(*this).command("FLUSHDB", async ? "ASYNC" : "SYNC");
        }

        auto dbsize() & noexcept{
            return this->command("DBSIZE");
        }
        auto dbsize() && noexcept{
            return std::move(*this).command("DBSIZE");
        }

        auto save() & noexcept{
            return this->command("SAVE");
        }
        auto save() && noexcept{
            return std::move(*this).command("SAVE");
        }

        template<typename... Args>
        auto shutdown(Args&&... args) & noexcept{
            return this->command("SHUTDOWN", std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto shutdown(Args&&... args) && noexcept{
            return std::move(*this).command("SHUTDOWN", std::forward<Args>(args)...);
        }

        auto publish(std::string_view channel, std::string_view message) & noexcept{
            return this->command("PUBLISH", channel, message);
        }
        auto publish(std::string_view channel, std::string_view message) && noexcept{
            return std::move(*this).command("PUBLISH", channel, message);
        }

        auto spublish(std::string_view shardchannel, std::string_view message) & noexcept{
            return this->command("SPUBLISH", shardchannel, message);
        }
        auto spublish(std::string_view shardchannel, std::string_view message) && noexcept{
            return std::move(*this).command("SPUBLISH", shardchannel, message);
        }

        // ---- String ----
        auto append(std::string_view key, std::string_view value) & noexcept{
            return this->command("APPEND", key, value);
        }
        auto append(std::string_view key, std::string_view value) && noexcept{
            return std::move(*this).command("APPEND", key, value);
        }

        auto decr(std::string_view key) & noexcept{
            return this->command("DECR", key);
        }
        auto decr(std::string_view key) && noexcept{
            return std::move(*this).command("DECR", key);
        }

        auto decrby(std::string_view key, int64_t decrement) & noexcept{
            return this->command("DECRBY", key, details::to_string(decrement));
        }
        auto decrby(std::string_view key, int64_t decrement) && noexcept{
            return std::move(*this).command("DECRBY", key, details::to_string(decrement));
        }

        auto get(std::string_view key) & noexcept{
            return this->command("GET", key);
        }
        auto get(std::string_view key) && noexcept{
            return std::move(*this).command("GET", key);
        }

        auto getdel(std::string_view key) & noexcept{
            return this->command("GETDEL", key);
        }
        auto getdel(std::string_view key) && noexcept{
            return std::move(*this).command("GETDEL", key);
        }

        template<typename... Args>
        auto getex(std::string_view key, Args&&... args) & noexcept{
            return this->command("GETEX", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto getex(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("GETEX", key, std::forward<Args>(args)...);
        }

        auto getrange(std::string_view key, int64_t start, int64_t end) & noexcept{
            return this->command("GETRANGE", key, details::to_string(start), details::to_string(end));
        }
        auto getrange(std::string_view key, int64_t start, int64_t end) && noexcept{
            return std::move(*this).command("GETRANGE", key, details::to_string(start), details::to_string(end));
        }

        auto incr(std::string_view key) & noexcept{
            return this->command("INCR", key);
        }
        auto incr(std::string_view key) && noexcept{
            return std::move(*this).command("INCR", key);
        }

        auto incrby(std::string_view key, int64_t increment) & noexcept{
            return this->command("INCRBY", key, details::to_string(increment));
        }
        auto incrby(std::string_view key, int64_t increment) && noexcept{
            return std::move(*this).command("INCRBY", key, details::to_string(increment));
        }

        auto incrbyfloat(std::string_view key, double increment) & noexcept{
            return this->command("INCRBYFLOAT", key, details::to_string(increment));
        }
        auto incrbyfloat(std::string_view key, double increment) && noexcept{
            return std::move(*this).command("INCRBYFLOAT", key, details::to_string(increment));
        }

        template<typename... Args>
        auto lcs(std::string_view key1, std::string_view key2, Args&&... args) & noexcept{
            return this->command("LCS", key1, key2, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto lcs(std::string_view key1, std::string_view key2, Args&&... args) && noexcept{
            return std::move(*this).command("LCS", key1, key2, std::forward<Args>(args)...);
        }

        template<typename... Keys>
        auto mget(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("MGET", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto mget(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("MGET", first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Args>
        requires (sizeof...(Args) > 0 && sizeof...(Args) % 2 == 0)
        auto mset(Args&&... args) & noexcept{
            return this->command("MSET", std::forward<Args>(args)...);
        }
        template<typename... Args>
        requires (sizeof...(Args) > 0 && sizeof...(Args) % 2 == 0)
        auto mset(Args&&... args) && noexcept{
            return std::move(*this).command("MSET", std::forward<Args>(args)...);
        }

        template<typename... Args>
        requires (sizeof...(Args) > 0 && sizeof...(Args) % 2 == 0)
        auto msetnx(Args&&... args) & noexcept{
            return this->command("MSETNX", std::forward<Args>(args)...);
        }
        template<typename... Args>
        requires (sizeof...(Args) > 0 && sizeof...(Args) % 2 == 0)
        auto msetnx(Args&&... args) && noexcept{
            return std::move(*this).command("MSETNX", std::forward<Args>(args)...);
        }

        auto psetex(std::string_view key, int64_t milliseconds, std::string_view value) & noexcept{
            return this->command("PSETEX", key, details::to_string(milliseconds), value);
        }
        auto psetex(std::string_view key, int64_t milliseconds, std::string_view value) && noexcept{
            return std::move(*this).command("PSETEX", key, details::to_string(milliseconds), value);
        }

        template<typename... Args>
        auto set(std::string_view key, std::string_view value, Args&&... args) & noexcept{
            return this->command("SET", key, value, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto set(std::string_view key, std::string_view value, Args&&... args) && noexcept{
            return std::move(*this).command("SET", key, value, std::forward<Args>(args)...);
        }

        auto setex(std::string_view key, int64_t seconds, std::string_view value) & noexcept{
            return this->command("SETEX", key, details::to_string(seconds), value);
        }
        auto setex(std::string_view key, int64_t seconds, std::string_view value) && noexcept{
            return std::move(*this).command("SETEX", key, details::to_string(seconds), value);
        }

        auto setnx(std::string_view key, std::string_view value) & noexcept{
            return this->command("SETNX", key, value);
        }
        auto setnx(std::string_view key, std::string_view value) && noexcept{
            return std::move(*this).command("SETNX", key, value);
        }

        auto setrange(std::string_view key, int64_t offset, std::string_view value) & noexcept{
            return this->command("SETRANGE", key, details::to_string(offset), value);
        }
        auto setrange(std::string_view key, int64_t offset, std::string_view value) && noexcept{
            return std::move(*this).command("SETRANGE", key, details::to_string(offset), value);
        }

        auto strlen(std::string_view key) & noexcept{
            return this->command("STRLEN", key);
        }
        auto strlen(std::string_view key) && noexcept{
            return std::move(*this).command("STRLEN", key);
        }

        // ---- Hash ----
        template<typename... Fields>
        auto hdel(std::string_view key, std::string_view first_field, Fields&&... other_fields) & noexcept{
            return this->command("HDEL", key, first_field, std::forward<Fields>(other_fields)...);
        }
        template<typename... Fields>
        auto hdel(std::string_view key, std::string_view first_field, Fields&&... other_fields) && noexcept{
            return std::move(*this).command("HDEL", key, first_field, std::forward<Fields>(other_fields)...);
        }

        auto hexists(std::string_view key, std::string_view field) & noexcept{
            return this->command("HEXISTS", key, field);
        }
        auto hexists(std::string_view key, std::string_view field) && noexcept{
            return std::move(*this).command("HEXISTS", key, field);
        }

        template<typename... Args>
        auto hexpire(std::string_view key, int64_t seconds, Args&&... fields) & noexcept{
            return this->command("HEXPIRE", key, details::to_string(seconds), std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto hexpire(std::string_view key, int64_t seconds, Args&&... fields) && noexcept{
            return std::move(*this).command("HEXPIRE", key, details::to_string(seconds), std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hexpireat(std::string_view key, int64_t timestamp, Args&&... fields) & noexcept{
            return this->command("HEXPIREAT", key, details::to_string(timestamp), std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto hexpireat(std::string_view key, int64_t timestamp, Args&&... fields) && noexcept{
            return std::move(*this).command("HEXPIREAT", key, details::to_string(timestamp), std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hexpiretime(std::string_view key, Args&&... fields) & noexcept{
            return this->command("HEXPIRETIME", key, std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto hexpiretime(std::string_view key, Args&&... fields) && noexcept{
            return std::move(*this).command("HEXPIRETIME", key, std::forward<Args>(fields)...);
        }

        auto hget(std::string_view key, std::string_view field) & noexcept{
            return this->command("HGET", key, field);
        }
        auto hget(std::string_view key, std::string_view field) && noexcept{
            return std::move(*this).command("HGET", key, field);
        }

        auto hgetall(std::string_view key) & noexcept{
            return this->command("HGETALL", key);
        }
        auto hgetall(std::string_view key) && noexcept{
            return std::move(*this).command("HGETALL", key);
        }

        auto hgetdel(std::string_view key, std::string_view field) & noexcept{
            return this->command("HGETDEL", key, field);
        }
        auto hgetdel(std::string_view key, std::string_view field) && noexcept{
            return std::move(*this).command("HGETDEL", key, field);
        }

        template<typename... Args>
        auto hgetex(std::string_view key, std::string_view field, Args&&... args) & noexcept{
            return this->command("HGETEX", key, field, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto hgetex(std::string_view key, std::string_view field, Args&&... args) && noexcept{
            return std::move(*this).command("HGETEX", key, field, std::forward<Args>(args)...);
        }

        auto hincrby(std::string_view key, std::string_view field, int64_t increment) & noexcept{
            return this->command("HINCRBY", key, field, details::to_string(increment));
        }
        auto hincrby(std::string_view key, std::string_view field, int64_t increment) && noexcept{
            return std::move(*this).command("HINCRBY", key, field, details::to_string(increment));
        }

        auto hincrbyfloat(std::string_view key, std::string_view field, double increment) & noexcept{
            return this->command("HINCRBYFLOAT", key, field, details::to_string(increment));
        }
        auto hincrbyfloat(std::string_view key, std::string_view field, double increment) && noexcept{
            return std::move(*this).command("HINCRBYFLOAT", key, field, details::to_string(increment));
        }

        auto hkeys(std::string_view key) & noexcept{
            return this->command("HKEYS", key);
        }
        auto hkeys(std::string_view key) && noexcept{
            return std::move(*this).command("HKEYS", key);
        }

        auto hlen(std::string_view key) & noexcept{
            return this->command("HLEN", key);
        }
        auto hlen(std::string_view key) && noexcept{
            return std::move(*this).command("HLEN", key);
        }

        template<typename... Fields>
        auto hmget(std::string_view key, std::string_view first_field, Fields&&... other_fields) & noexcept{
            return this->command("HMGET", key, first_field, std::forward<Fields>(other_fields)...);
        }
        template<typename... Fields>
        auto hmget(std::string_view key, std::string_view first_field, Fields&&... other_fields) && noexcept{
            return std::move(*this).command("HMGET", key, first_field, std::forward<Fields>(other_fields)...);
        }

        template<typename... Args>
        auto hpersist(std::string_view key, Args&&... fields) & noexcept{
            return this->command("HPERSIST", key, std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto hpersist(std::string_view key, Args&&... fields) && noexcept{
            return std::move(*this).command("HPERSIST", key, std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hpexpire(std::string_view key, int64_t milliseconds, Args&&... fields) & noexcept{
            return this->command("HPEXPIRE", key, details::to_string(milliseconds), std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto hpexpire(std::string_view key, int64_t milliseconds, Args&&... fields) && noexcept{
            return std::move(*this).command("HPEXPIRE", key, details::to_string(milliseconds), std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hpexpireat(std::string_view key, int64_t milliseconds_timestamp, Args&&... fields) & noexcept{
            return this->command("HPEXPIREAT", key, details::to_string(milliseconds_timestamp), std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto hpexpireat(std::string_view key, int64_t milliseconds_timestamp, Args&&... fields) && noexcept{
            return std::move(*this).command("HPEXPIREAT", key, details::to_string(milliseconds_timestamp), std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hpexpiretime(std::string_view key, Args&&... fields) & noexcept{
            return this->command("HPEXPIRETIME", key, std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto hpexpiretime(std::string_view key, Args&&... fields) && noexcept{
            return std::move(*this).command("HPEXPIRETIME", key, std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hpttl(std::string_view key, Args&&... fields) & noexcept{
            return this->command("HPTTL", key, std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto hpttl(std::string_view key, Args&&... fields) && noexcept{
            return std::move(*this).command("HPTTL", key, std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hrandfield(std::string_view key, Args&&... args) & noexcept{
            return this->command("HRANDFIELD", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto hrandfield(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("HRANDFIELD", key, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto hscan(std::string_view key, int64_t cursor, Args&&... args) & noexcept{
            return this->command("HSCAN", key, details::to_string(cursor), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto hscan(std::string_view key, int64_t cursor, Args&&... args) && noexcept{
            return std::move(*this).command("HSCAN", key, details::to_string(cursor), std::forward<Args>(args)...);
        }

        template<typename... Args>
        requires (sizeof...(Args) >= 2 && sizeof...(Args) % 2 == 0)
        auto hset(std::string_view key, Args&&... args) & noexcept{
            return this->command("HSET", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        requires (sizeof...(Args) >= 2 && sizeof...(Args) % 2 == 0)
        auto hset(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("HSET", key, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto hsetex(std::string_view key, Args&&... args) & noexcept{
            return this->command("HSETEX", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto hsetex(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("HSETEX", key, std::forward<Args>(args)...);
        }

        auto hsetnx(std::string_view key, std::string_view field, std::string_view value) & noexcept{
            return this->command("HSETNX", key, field, value);
        }
        auto hsetnx(std::string_view key, std::string_view field, std::string_view value) && noexcept{
            return std::move(*this).command("HSETNX", key, field, value);
        }

        auto hstrlen(std::string_view key, std::string_view field) & noexcept{
            return this->command("HSTRLEN", key, field);
        }
        auto hstrlen(std::string_view key, std::string_view field) && noexcept{
            return std::move(*this).command("HSTRLEN", key, field);
        }

        template<typename... Args>
        auto httl(std::string_view key, Args&&... fields) & noexcept{
            return this->command("HTTL", key, std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto httl(std::string_view key, Args&&... fields) && noexcept{
            return std::move(*this).command("HTTL", key, std::forward<Args>(fields)...);
        }

        auto hvals(std::string_view key) & noexcept{
            return this->command("HVALS", key);
        }
        auto hvals(std::string_view key) && noexcept{
            return std::move(*this).command("HVALS", key);
        }

        // ---- List ----
        auto blmove(std::string_view source, std::string_view destination, std::string_view wherefrom, std::string_view whereto, double timeout) & noexcept{
            return this->command("BLMOVE", source, destination, wherefrom, whereto, details::to_string(timeout));
        }
        auto blmove(std::string_view source, std::string_view destination, std::string_view wherefrom, std::string_view whereto, double timeout) && noexcept{
            return std::move(*this).command("BLMOVE", source, destination, wherefrom, whereto, details::to_string(timeout));
        }

        template<typename... Args>
        auto blmpop(double timeout, int64_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            return this->command("BLMPOP", details::to_string(timeout), details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto blmpop(double timeout, int64_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            return std::move(*this).command("BLMPOP", details::to_string(timeout), details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }

        template<typename... Keys>
        auto blpop(std::string_view first_key, Keys&&... other_keys_and_timeout) & noexcept{
            return this->command("BLPOP", first_key, std::forward<Keys>(other_keys_and_timeout)...);
        }
        template<typename... Keys>
        auto blpop(std::string_view first_key, Keys&&... other_keys_and_timeout) && noexcept{
            return std::move(*this).command("BLPOP", first_key, std::forward<Keys>(other_keys_and_timeout)...);
        }

        template<typename... Keys>
        auto brpop(std::string_view first_key, Keys&&... other_keys_and_timeout) & noexcept{
            return this->command("BRPOP", first_key, std::forward<Keys>(other_keys_and_timeout)...);
        }
        template<typename... Keys>
        auto brpop(std::string_view first_key, Keys&&... other_keys_and_timeout) && noexcept{
            return std::move(*this).command("BRPOP", first_key, std::forward<Keys>(other_keys_and_timeout)...);
        }

        auto lindex(std::string_view key, int64_t index) & noexcept{
            return this->command("LINDEX", key, details::to_string(index));
        }
        auto lindex(std::string_view key, int64_t index) && noexcept{
            return std::move(*this).command("LINDEX", key, details::to_string(index));
        }

        auto linsert(std::string_view key, std::string_view where, std::string_view pivot, std::string_view element) & noexcept{
            return this->command("LINSERT", key, where, pivot, element);
        }
        auto linsert(std::string_view key, std::string_view where, std::string_view pivot, std::string_view element) && noexcept{
            return std::move(*this).command("LINSERT", key, where, pivot, element);
        }

        auto llen(std::string_view key) & noexcept{
            return this->command("LLEN", key);
        }
        auto llen(std::string_view key) && noexcept{
            return std::move(*this).command("LLEN", key);
        }

        auto lmove(std::string_view source, std::string_view destination, std::string_view wherefrom, std::string_view whereto) & noexcept{
            return this->command("LMOVE", source, destination, wherefrom, whereto);
        }
        auto lmove(std::string_view source, std::string_view destination, std::string_view wherefrom, std::string_view whereto) && noexcept{
            return std::move(*this).command("LMOVE", source, destination, wherefrom, whereto);
        }

        template<typename... Args>
        auto lmpop(int64_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            return this->command("LMPOP", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto lmpop(int64_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            return std::move(*this).command("LMPOP", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto lpop(std::string_view key, Args&&... count) & noexcept{
            return this->command("LPOP", key, std::forward<Args>(count)...);
        }
        template<typename... Args>
        auto lpop(std::string_view key, Args&&... count) && noexcept{
            return std::move(*this).command("LPOP", key, std::forward<Args>(count)...);
        }

        template<typename... Args>
        auto lpos(std::string_view key, std::string_view element, Args&&... args) & noexcept{
            return this->command("LPOS", key, element, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto lpos(std::string_view key, std::string_view element, Args&&... args) && noexcept{
            return std::move(*this).command("LPOS", key, element, std::forward<Args>(args)...);
        }

        template<typename... Vals>
        auto lpush(std::string_view key, std::string_view first_val, Vals&&... other_vals) & noexcept{
            return this->command("LPUSH", key, first_val, std::forward<Vals>(other_vals)...);
        }
        template<typename... Vals>
        auto lpush(std::string_view key, std::string_view first_val, Vals&&... other_vals) && noexcept{
            return std::move(*this).command("LPUSH", key, first_val, std::forward<Vals>(other_vals)...);
        }

        template<typename... Vals>
        auto lpushx(std::string_view key, std::string_view first_val, Vals&&... other_vals) & noexcept{
            return this->command("LPUSHX", key, first_val, std::forward<Vals>(other_vals)...);
        }
        template<typename... Vals>
        auto lpushx(std::string_view key, std::string_view first_val, Vals&&... other_vals) && noexcept{
            return std::move(*this).command("LPUSHX", key, first_val, std::forward<Vals>(other_vals)...);
        }

        auto lrange(std::string_view key, int64_t start, int64_t stop) & noexcept{
            return this->command("LRANGE", key, details::to_string(start), details::to_string(stop));
        }
        auto lrange(std::string_view key, int64_t start, int64_t stop) && noexcept{
            return std::move(*this).command("LRANGE", key, details::to_string(start), details::to_string(stop));
        }

        auto lrem(std::string_view key, int64_t count, std::string_view element) & noexcept{
            return this->command("LREM", key, details::to_string(count), element);
        }
        auto lrem(std::string_view key, int64_t count, std::string_view element) && noexcept{
            return std::move(*this).command("LREM", key, details::to_string(count), element);
        }

        auto lset(std::string_view key, int64_t index, std::string_view element) & noexcept{
            return this->command("LSET", key, details::to_string(index), element);
        }
        auto lset(std::string_view key, int64_t index, std::string_view element) && noexcept{
            return std::move(*this).command("LSET", key, details::to_string(index), element);
        }

        auto ltrim(std::string_view key, int64_t start, int64_t stop) & noexcept{
            return this->command("LTRIM", key, details::to_string(start), details::to_string(stop));
        }
        auto ltrim(std::string_view key, int64_t start, int64_t stop) && noexcept{
            return std::move(*this).command("LTRIM", key, details::to_string(start), details::to_string(stop));
        }

        auto rpop(std::string_view key) & noexcept{
            return this->command("RPOP", key);
        }
        auto rpop(std::string_view key) && noexcept{
            return std::move(*this).command("RPOP", key);
        }

        template<typename... Vals>
        auto rpush(std::string_view key, std::string_view first_val, Vals&&... other_vals) & noexcept{
            return this->command("RPUSH", key, first_val, std::forward<Vals>(other_vals)...);
        }
        template<typename... Vals>
        auto rpush(std::string_view key, std::string_view first_val, Vals&&... other_vals) && noexcept{
            return std::move(*this).command("RPUSH", key, first_val, std::forward<Vals>(other_vals)...);
        }

        template<typename... Vals>
        auto rpushx(std::string_view key, std::string_view first_val, Vals&&... other_vals) & noexcept{
            return this->command("RPUSHX", key, first_val, std::forward<Vals>(other_vals)...);
        }
        template<typename... Vals>
        auto rpushx(std::string_view key, std::string_view first_val, Vals&&... other_vals) && noexcept{
            return std::move(*this).command("RPUSHX", key, first_val, std::forward<Vals>(other_vals)...);
        }

        // ---- Set ----

        template<typename... Members>
        auto sadd(std::string_view key, std::string_view first_member, Members&&... other_members) & noexcept{
            return this->command("SADD", key, first_member, std::forward<Members>(other_members)...);
        }
        template<typename... Members>
        auto sadd(std::string_view key, std::string_view first_member, Members&&... other_members) && noexcept{
            return std::move(*this).command("SADD", key, first_member, std::forward<Members>(other_members)...);
        }

        auto scard(std::string_view key) & noexcept{
            return this->command("SCARD", key);
        }
        auto scard(std::string_view key) && noexcept{
            return std::move(*this).command("SCARD", key);
        }

        template<typename... Keys>
        auto sdiff(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("SDIFF", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto sdiff(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("SDIFF", first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Keys>
        auto sdiffstore(std::string_view destination, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("SDIFFSTORE", destination, first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto sdiffstore(std::string_view destination, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("SDIFFSTORE", destination, first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Keys>
        auto sinter(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("SINTER", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto sinter(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("SINTER", first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Args>
        auto sintercard(int64_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            return this->command("SINTERCARD", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto sintercard(int64_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            return std::move(*this).command("SINTERCARD", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }
        
        template<typename... Keys>
        auto sinterstore(std::string_view destination, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("SINTERSTORE", destination, first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto sinterstore(std::string_view destination, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("SINTERSTORE", destination, first_key, std::forward<Keys>(other_keys)...);
        }

        auto sismember(std::string_view key, std::string_view member) & noexcept{
            return this->command("SISMEMBER", key, member);
        }
        auto sismember(std::string_view key, std::string_view member) && noexcept{
            return std::move(*this).command("SISMEMBER", key, member);
        }

        auto smembers(std::string_view key) & noexcept{
            return this->command("SMEMBERS", key);
        }
        auto smembers(std::string_view key) && noexcept{
            return std::move(*this).command("SMEMBERS", key);
        }

        template<typename... Members>
        auto smismember(std::string_view key, std::string_view first_member, Members&&... other_members) & noexcept{
            return this->command("SMISMEMBER", key, first_member, std::forward<Members>(other_members)...);
        }
        template<typename... Members>
        auto smismember(std::string_view key, std::string_view first_member, Members&&... other_members) && noexcept{
            return std::move(*this).command("SMISMEMBER", key, first_member, std::forward<Members>(other_members)...);
        }

        auto smove(std::string_view source, std::string_view destination, std::string_view member) & noexcept{
            return this->command("SMOVE", source, destination, member);
        }
        auto smove(std::string_view source, std::string_view destination, std::string_view member) && noexcept{
            return std::move(*this).command("SMOVE", source, destination, member);
        }

        template<typename... Args>
        auto spop(std::string_view key, Args&&... count) & noexcept{
            return this->command("SPOP", key, std::forward<Args>(count)...);
        }
        template<typename... Args>
        auto spop(std::string_view key, Args&&... count) && noexcept{
            return std::move(*this).command("SPOP", key, std::forward<Args>(count)...);
        }

        template<typename... Args>
        auto srandmember(std::string_view key, Args&&... count) & noexcept{
            return this->command("SRANDMEMBER", key, std::forward<Args>(count)...);
        }
        template<typename... Args>
        auto srandmember(std::string_view key, Args&&... count) && noexcept{
            return std::move(*this).command("SRANDMEMBER", key, std::forward<Args>(count)...);
        }

        template<typename... Members>
        auto srem(std::string_view key, std::string_view first_member, Members&&... other_members) & noexcept{
            return this->command("SREM", key, first_member, std::forward<Members>(other_members)...);
        }
        template<typename... Members>
        auto srem(std::string_view key, std::string_view first_member, Members&&... other_members) && noexcept{
            return std::move(*this).command("SREM", key, first_member, std::forward<Members>(other_members)...);
        }

        template<typename... Args>
        auto sscan(std::string_view key, int64_t cursor, Args&&... args) & noexcept{
            return this->command("SSCAN", key, details::to_string(cursor), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto sscan(std::string_view key, int64_t cursor, Args&&... args) && noexcept{
            return std::move(*this).command("SSCAN", key, details::to_string(cursor), std::forward<Args>(args)...);
        }

        template<typename... Keys>
        auto sunion(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("SUNION", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto sunion(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("SUNION", first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Keys>
        auto sunionstore(std::string_view destination, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("SUNIONSTORE", destination, first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto sunionstore(std::string_view destination, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("SUNIONSTORE", destination, first_key, std::forward<Keys>(other_keys)...);
        }

        // ---- ZSet ----
        template<typename... Args>
        auto zadd(std::string_view key, Args&&... args) & noexcept{
            return this->command("ZADD", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto zadd(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("ZADD", key, std::forward<Args>(args)...);
        }

        auto zcard(std::string_view key) & noexcept{
            return this->command("ZCARD", key);
        }
        auto zcard(std::string_view key) && noexcept{
            return std::move(*this).command("ZCARD", key);
        }

        auto zcount(std::string_view key, double min, double max) & noexcept{
            return this->command("ZCOUNT", key, details::to_string(min), details::to_string(max));
        }
        auto zcount(std::string_view key, double min, double max) && noexcept{
            return std::move(*this).command("ZCOUNT", key, details::to_string(min), details::to_string(max));
        }

        template<typename... Keys>
        auto zdiff(size_t numkeys, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("ZDIFF", details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto zdiff(size_t numkeys, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("ZDIFF", details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Keys>
        auto zdiffstore(std::string_view destination, size_t numkeys, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("ZDIFFSTORE", destination, details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto zdiffstore(std::string_view destination, size_t numkeys, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("ZDIFFSTORE", destination, details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }

        auto zincrby(std::string_view key, double increment, std::string_view member) & noexcept{
            return this->command("ZINCRBY", key, details::to_string(increment), member);
        }
        auto zincrby(std::string_view key, double increment, std::string_view member) && noexcept{
            return std::move(*this).command("ZINCRBY", key, details::to_string(increment), member);
        }

        template<typename... Keys>
        auto zinter(size_t numkeys, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("ZINTER", details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto zinter(size_t numkeys, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("ZINTER", details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Args>
        auto zintercard(size_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            return this->command("ZINTERCARD", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto zintercard(size_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            return std::move(*this).command("ZINTERCARD", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto zmpop(size_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            return this->command("ZMPOP", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto zmpop(size_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            return std::move(*this).command("ZMPOP", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }

        template<typename... Members>
        auto zmscore(std::string_view key, std::string_view first_member, Members&&... other_members) & noexcept{
            return this->command("ZMSCORE", key, first_member, std::forward<Members>(other_members)...);
        }
        template<typename... Members>
        auto zmscore(std::string_view key, std::string_view first_member, Members&&... other_members) && noexcept{
            return std::move(*this).command("ZMSCORE", key, first_member, std::forward<Members>(other_members)...);
        }

        template<typename... Args>
        auto zrandmember(std::string_view key, Args&&... args) & noexcept{
            return this->command("ZRANDMEMBER", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto zrandmember(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("ZRANDMEMBER", key, std::forward<Args>(args)...);
        }

        template<typename... Options>
        auto zrange(std::string_view key, std::string_view start, std::string_view stop, Options&&... options) & noexcept{
            return this->command("ZRANGE", key, start, stop, std::forward<Options>(options)...);
        }
        template<typename... Options>
        auto zrange(std::string_view key, std::string_view start, std::string_view stop, Options&&... options) && noexcept{
            return std::move(*this).command("ZRANGE", key, start, stop, std::forward<Options>(options)...);
        }

        template<typename... Args>
        auto zrank(std::string_view key, std::string_view member, Args&&... args) & noexcept{
            return this->command("ZRANK", key, member, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto zrank(std::string_view key, std::string_view member, Args&&... args) && noexcept{
            return std::move(*this).command("ZRANK", key, member, std::forward<Args>(args)...);
        }

        template<typename... Members>
        auto zrem(std::string_view key, std::string_view first_member, Members&&... other_members) & noexcept{
            return this->command("ZREM", key, first_member, std::forward<Members>(other_members)...);
        }
        template<typename... Members>
        auto zrem(std::string_view key, std::string_view first_member, Members&&... other_members) && noexcept{
            return std::move(*this).command("ZREM", key, first_member, std::forward<Members>(other_members)...);
        }

        template<typename... Args>
        auto zscan(std::string_view key, size_t cursor, Args&&... args) & noexcept{
            return this->command("ZSCAN", key, details::to_string(cursor), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto zscan(std::string_view key, size_t cursor, Args&&... args) && noexcept{
            return std::move(*this).command("ZSCAN", key, details::to_string(cursor), std::forward<Args>(args)...);
        }

        auto zscore(std::string_view key, std::string_view member) & noexcept{
            return this->command("ZSCORE", key, member);
        }
        auto zscore(std::string_view key, std::string_view member) && noexcept{
            return std::move(*this).command("ZSCORE", key, member);
        }

        template<typename... Keys>
        auto zunion(size_t numkeys, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("ZUNION", details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto zunion(size_t numkeys, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("ZUNION", details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Keys>
        auto zunionstore(std::string_view destination, size_t numkeys, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("ZUNIONSTORE", destination, details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto zunionstore(std::string_view destination, size_t numkeys, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("ZUNIONSTORE", destination, details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }

        // ---- HyperLogLog ----

        template<typename... Args>
        auto pfadd(std::string_view key, Args&&... elements) & noexcept{
            return this->command("PFADD", key, std::forward<Args>(elements)...);
        }
        template<typename... Args>
        auto pfadd(std::string_view key, Args&&... elements) && noexcept{
            return std::move(*this).command("PFADD", key, std::forward<Args>(elements)...);
        }

        template<typename... Keys>
        auto pfcount(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("PFCOUNT", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto pfcount(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("PFCOUNT", first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... SrcKeys>
        auto pfmerge(std::string_view destkey, std::string_view first_srckey, SrcKeys&&... other_srckeys) & noexcept{
            return this->command("PFMERGE", destkey, first_srckey, std::forward<SrcKeys>(other_srckeys)...);
        }
        template<typename... SrcKeys>
        auto pfmerge(std::string_view destkey, std::string_view first_srckey, SrcKeys&&... other_srckeys) && noexcept{
            return std::move(*this).command("PFMERGE", destkey, first_srckey, std::forward<SrcKeys>(other_srckeys)...);
        }

        // ---- Bitmap ----
        auto setbit(std::string_view key, size_t offset, std::string_view value) & noexcept{
            return this->command("SETBIT", key, details::to_string(offset), value);
        }
        auto setbit(std::string_view key, size_t offset, std::string_view value) && noexcept{
            return std::move(*this).command("SETBIT", key, details::to_string(offset), value);
        }

        auto getbit(std::string_view key, size_t offset) & noexcept{
            return this->command("GETBIT", key, details::to_string(offset));
        }
        auto getbit(std::string_view key, size_t offset) && noexcept{
            return std::move(*this).command("GETBIT", key, details::to_string(offset));
        }

        template<typename... Args>
        auto bitcount(std::string_view key, int64_t start, int64_t stop, Args&&... args) & noexcept{
            return this->command("BITCOUNT", key, details::to_string(start), details::to_string(stop), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto bitcount(std::string_view key, int64_t start, int64_t stop, Args&&... args) && noexcept{
            return std::move(*this).command("BITCOUNT", key, details::to_string(start), details::to_string(stop), std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto bitpos(std::string_view key, std::string_view bit, Args&&... args) & noexcept{
            return this->command("BITPOS", key, bit, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto bitpos(std::string_view key, std::string_view bit, Args&&... args) && noexcept{
            return std::move(*this).command("BITPOS", key, bit, std::forward<Args>(args)...);
        }

        template<typename... SrcKeys>
        auto bitop(std::string_view operation, std::string_view destkey, std::string_view first_srckey, SrcKeys&&... other_srckeys) & noexcept{
            return this->command("BITOP", operation, destkey, first_srckey, std::forward<SrcKeys>(other_srckeys)...);
        }
        template<typename... SrcKeys>
        auto bitop(std::string_view operation, std::string_view destkey, std::string_view first_srckey, SrcKeys&&... other_srckeys) && noexcept{
            return std::move(*this).command("BITOP", operation, destkey, first_srckey, std::forward<SrcKeys>(other_srckeys)...);
        }

        template<typename... Args>
        auto bitfield(std::string_view key, Args&&... args) & noexcept{
            return this->command("BITFIELD", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto bitfield(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("BITFIELD", key, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto bitfield_ro(std::string_view key, Args&&... args) & noexcept{
            return this->command("BITFIELD_RO", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto bitfield_ro(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("BITFIELD_RO", key, std::forward<Args>(args)...);
        }

        // ---- Stream ----
        template<typename... Args>
        auto xadd(std::string_view key, Args&&... args) & noexcept{
            return this->command("XADD", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xadd(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("XADD", key, std::forward<Args>(args)...);
        }

        template<typename... Ids>
        auto xack(std::string_view key, std::string_view group, std::string_view first_id, Ids&&... other_ids) & noexcept{
            return this->command("XACK", key, group, first_id, std::forward<Ids>(other_ids)...);
        }
        template<typename... Ids>
        auto xack(std::string_view key, std::string_view group, std::string_view first_id, Ids&&... other_ids) && noexcept{
            return std::move(*this).command("XACK", key, group, first_id, std::forward<Ids>(other_ids)...);
        }

        template<typename... Args>
        auto xautoclaim(std::string_view key, std::string_view group, std::string_view consumer, size_t min_idle_time, std::string_view start, Args&&... args) & noexcept{
            return this->command("XAUTOCLAIM", key, group, consumer, details::to_string(min_idle_time), start, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xautoclaim(std::string_view key, std::string_view group, std::string_view consumer, size_t min_idle_time, std::string_view start, Args&&... args) && noexcept{
            return std::move(*this).command("XAUTOCLAIM", key, group, consumer, details::to_string(min_idle_time), start, std::forward<Args>(args)...);
        }

        template<typename... Ids>
        auto xdel(std::string_view key, std::string_view first_id, Ids&&... other_ids) & noexcept{
            return this->command("XDEL", key, first_id, std::forward<Ids>(other_ids)...);
        }
        template<typename... Ids>
        auto xdel(std::string_view key, std::string_view first_id, Ids&&... other_ids) && noexcept{
            return std::move(*this).command("XDEL", key, first_id, std::forward<Ids>(other_ids)...);
        }

        template<typename... Args>
        auto xgroup_create(std::string_view key, std::string_view group, std::string_view id, Args&&... args) & noexcept{
            return this->command("XGROUP", "CREATE", key, group, id, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xgroup_create(std::string_view key, std::string_view group, std::string_view id, Args&&... args) && noexcept{
            return std::move(*this).command("XGROUP", "CREATE", key, group, id, std::forward<Args>(args)...);
        }

        auto xgroup_createconsumer(std::string_view key, std::string_view group, std::string_view consumer) & noexcept{
            return this->command("XGROUP", "CREATECONSUMER", key, group, consumer);
        }
        auto xgroup_createconsumer(std::string_view key, std::string_view group, std::string_view consumer) && noexcept{
            return std::move(*this).command("XGROUP", "CREATECONSUMER", key, group, consumer);
        }

        auto xgroup_delconsumer(std::string_view key, std::string_view group, std::string_view consumer) & noexcept{
            return this->command("XGROUP", "DELCONSUMER", key, group, consumer);
        }
        auto xgroup_delconsumer(std::string_view key, std::string_view group, std::string_view consumer) && noexcept{
            return std::move(*this).command("XGROUP", "DELCONSUMER", key, group, consumer);
        }

        auto xgroup_destroy(std::string_view key, std::string_view group) & noexcept{
            return this->command("XGROUP", "DESTROY", key, group);
        }
        auto xgroup_destroy(std::string_view key, std::string_view group) && noexcept{
            return std::move(*this).command("XGROUP", "DESTROY", key, group);
        }

        template<typename... Args>
        auto xgroup_setid(std::string_view key, std::string_view group, std::string_view id, Args&&... args) & noexcept{
            return this->command("XGROUP", "SETID", key, group, id, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xgroup_setid(std::string_view key, std::string_view group, std::string_view id, Args&&... args) && noexcept{
            return std::move(*this).command("XGROUP", "SETID", key, group, id, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto xinfo_consumers(std::string_view key, std::string_view group) & noexcept{
            return this->command("XINFO", "CONSUMERS", key, group);
        }
        template<typename... Args>
        auto xinfo_consumers(std::string_view key, std::string_view group) && noexcept{
            return std::move(*this).command("XINFO", "CONSUMERS", key, group);
        }

        auto xinfo_groups(std::string_view key) & noexcept{
            return this->command("XINFO", "GROUPS", key);
        }
        auto xinfo_groups(std::string_view key) && noexcept{
            return std::move(*this).command("XINFO", "GROUPS", key);
        }

        template<typename... Args>
        auto xinfo_stream(std::string_view key, Args&&... args) & noexcept{
            return this->command("XINFO", "STREAM", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xinfo_stream(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("XINFO", "STREAM", key, std::forward<Args>(args)...);
        }

        auto xlen(std::string_view key) & noexcept{
            return this->command("XLEN", key);
        }
        auto xlen(std::string_view key) && noexcept{
            return std::move(*this).command("XLEN", key);
        }

        template<typename... Args>
        auto xpending(std::string_view key, std::string_view group, Args&&... args) & noexcept{
            return this->command("XPENDING", key, group, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xpending(std::string_view key, std::string_view group, Args&&... args) && noexcept{
            return std::move(*this).command("XPENDING", key, group, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto xrange(std::string_view key, std::string_view start, std::string_view end, Args&&... args) & noexcept{
            return this->command("XRANGE", key, start, end, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xrange(std::string_view key, std::string_view start, std::string_view end, Args&&... args) && noexcept{
            return std::move(*this).command("XRANGE", key, start, end, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto xreadgroup(std::string_view group_keyword, std::string_view group, std::string_view consumer, Args&&... args) & noexcept{
            return this->command("XREADGROUP", group_keyword, group, consumer, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xreadgroup(std::string_view group_keyword, std::string_view group, std::string_view consumer, Args&&... args) && noexcept{
            return std::move(*this).command("XREADGROUP", group_keyword, group, consumer, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto xtrim(std::string_view key, Args&&... args) & noexcept{
            return this->command("XTRIM", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xtrim(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("XTRIM", key, std::forward<Args>(args)...);
        }

        // ---- Scripting ----

        template<typename... Args>
        auto eval(std::string_view script, size_t numkeys, Args&&... args) & noexcept{
            return this->command("EVAL", script, details::to_string(numkeys), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto eval(std::string_view script, size_t numkeys, Args&&... args) && noexcept{
            return std::move(*this).command("EVAL", script, details::to_string(numkeys), std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto evalsha(std::string_view sha1, size_t numkeys, Args&&... args) & noexcept{
            return this->command("EVALSHA", sha1, details::to_string(numkeys), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto evalsha(std::string_view sha1, size_t numkeys, Args&&... args) && noexcept{
            return std::move(*this).command("EVALSHA", sha1, details::to_string(numkeys), std::forward<Args>(args)...);
        }

        auto script_load(std::string_view script) & noexcept{
            return this->command("SCRIPT", "LOAD", script);
        }
        auto script_load(std::string_view script) && noexcept{
            return std::move(*this).command("SCRIPT", "LOAD", script);
        }

        // ---- Transaction ----
        template<typename... Keys>
        auto watch(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("WATCH", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto watch(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("WATCH", first_key, std::forward<Keys>(other_keys)...);
        }

        auto unwatch() & noexcept{
            return this->command("UNWATCH");
        }
        auto unwatch() && noexcept{
            return std::move(*this).command("UNWATCH");
        }

        auto exec() & noexcept{
            return this->command("EXEC");
        }
        auto exec() && noexcept{
            return std::move(*this).command("EXEC");
        }

        // ---- Generic ----

        template<typename... Args>
        auto copy(std::string_view source, std::string_view destination, Args&&... args) & noexcept{
            return this->command("COPY", source, destination, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto copy(std::string_view source, std::string_view destination, Args&&... args) && noexcept{
            return std::move(*this).command("COPY", source, destination, std::forward<Args>(args)...);
        }

        template<typename... Keys>
        auto del(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("DEL", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto del(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("DEL", first_key, std::forward<Keys>(other_keys)...);
        }

        auto dump(std::string_view key) & noexcept{
            return this->command("DUMP", key);
        }
        auto dump(std::string_view key) && noexcept{
            return std::move(*this).command("DUMP", key);
        }

        auto exists(std::string_view key) & noexcept{
            return this->command("EXISTS", key);
        }
        auto exists(std::string_view key) && noexcept{
            return std::move(*this).command("EXISTS", key);
        }

        template<typename... Args>
        auto expire(std::string_view key, size_t seconds, Args&&... args) & noexcept{
            return this->command("EXPIRE", key, details::to_string(seconds), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto expire(std::string_view key, size_t seconds, Args&&... args) && noexcept{
            return std::move(*this).command("EXPIRE", key, details::to_string(seconds), std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto expireat(std::string_view key, size_t unix_time_seconds, Args&&... args) & noexcept{
            return this->command("EXPIREAT", key, details::to_string(unix_time_seconds), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto expireat(std::string_view key, size_t unix_time_seconds, Args&&... args) && noexcept{
            return std::move(*this).command("EXPIREAT", key, details::to_string(unix_time_seconds), std::forward<Args>(args)...);
        }

        auto expiretime(std::string_view key) & noexcept{
            return this->command("EXPIRETIME", key);
        }
        auto expiretime(std::string_view key) && noexcept{
            return std::move(*this).command("EXPIRETIME", key);
        }

        auto keys(std::string_view pattern) & noexcept{
            return this->command("KEYS", pattern);
        }
        auto keys(std::string_view pattern) && noexcept{
            return std::move(*this).command("KEYS", pattern);
        }

        auto move(std::string_view key, size_t db) & noexcept{
            return this->command("MOVE", key, details::to_string(db));
        }
        auto move(std::string_view key, size_t db) && noexcept{
            return std::move(*this).command("MOVE", key, details::to_string(db));
        }

        auto persist(std::string_view key) & noexcept{
            return this->command("PERSIST", key);
        }
        auto persist(std::string_view key) && noexcept{
            return std::move(*this).command("PERSIST", key);
        }

        template<typename... Args>
        auto pexpire(std::string_view key, size_t milliseconds, Args&&... args) & noexcept{
            return this->command("PEXPIRE", key, details::to_string(milliseconds), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto pexpire(std::string_view key, size_t milliseconds, Args&&... args) && noexcept{
            return std::move(*this).command("PEXPIRE", key, details::to_string(milliseconds), std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto pexpireat(std::string_view key, size_t unix_time_milliseconds, Args&&... args) & noexcept{
            return this->command("PEXPIREAT", key, details::to_string(unix_time_milliseconds), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto pexpireat(std::string_view key, size_t unix_time_milliseconds, Args&&... args) && noexcept{
            return std::move(*this).command("PEXPIREAT", key, details::to_string(unix_time_milliseconds), std::forward<Args>(args)...);
        }

        auto pexpiretime(std::string_view key) & noexcept{
            return this->command("PEXPIRETIME", key);
        }
        auto pexpiretime(std::string_view key) && noexcept{
            return std::move(*this).command("PEXPIRETIME", key);
        }

        auto pttl(std::string_view key) & noexcept{
            return this->command("PTTL", key);
        }
        auto pttl(std::string_view key) && noexcept{
            return std::move(*this).command("PTTL", key);
        }

        auto randomkey() & noexcept{
            return this->command("RANDOMKEY");
        }
        auto randomkey() && noexcept{
            return std::move(*this).command("RANDOMKEY");
        }

        auto rename(std::string_view key, std::string_view newkey) & noexcept{
            return this->command("RENAME", key, newkey);
        }
        auto rename(std::string_view key, std::string_view newkey) && noexcept{
            return std::move(*this).command("RENAME", key, newkey);
        }

        auto renamenx(std::string_view key, std::string_view newkey) & noexcept{
            return this->command("RENAMENX", key, newkey);
        }
        auto renamenx(std::string_view key, std::string_view newkey) && noexcept{
            return std::move(*this).command("RENAMENX", key, newkey);
        }

        template<typename... Args>
        auto scan(size_t cursor, Args&&... args) & noexcept{
            return this->command("SCAN", details::to_string(cursor), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto scan(size_t cursor, Args&&... args) && noexcept{
            return std::move(*this).command("SCAN", details::to_string(cursor), std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto sort(std::string_view key, Args&&... args) & noexcept{
            return this->command("SORT", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto sort(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("SORT", key, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto sort_ro(std::string_view key, Args&&... args) & noexcept{
            return this->command("SORT_RO", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto sort_ro(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("SORT_RO", key, std::forward<Args>(args)...);
        }

        template<typename... Keys>
        auto touch(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("TOUCH", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto touch(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("TOUCH", first_key, std::forward<Keys>(other_keys)...);
        }

        auto ttl(std::string_view key) & noexcept{
            return this->command("TTL", key);
        }
        auto ttl(std::string_view key) && noexcept{
            return std::move(*this).command("TTL", key);
        }

        auto type(std::string_view key) & noexcept{
            return this->command("TYPE", key);
        }
        auto type(std::string_view key) && noexcept{
            return std::move(*this).command("TYPE", key);
        }

        template<typename... Keys>
        auto unlink(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("UNLINK", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto unlink(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("UNLINK", first_key, std::forward<Keys>(other_keys)...);
        }
    };

    template<class T, size_t num_ops>
    class RedisCommandOperationAwaiter{
        using Tuple = decltype(details::generate_tuple_result_t(std::make_index_sequence<num_ops>()));
        
        T* pipe;
        std::string ops[num_ops];
        Tuple result;
        std::coroutine_handle<> handler;
        std::coroutine_handle<> workers[num_ops];
    #ifndef XNET_DISABLE_THREAD_SAFE
        std::atomic<size_t> done;
    #else
        size_t done;
    #endif

        template<size_t... Is>
        void start_all_tasks(std::index_sequence<Is...>) noexcept{
            (worker<Is>(*this), ...);
        }

        template<size_t worker_id>
        static xnet::detached_task worker(RedisCommandOperationAwaiter& awaiter) noexcept{
            awaiter.workers[worker_id] = co_await details::get_handler{};
            co_await std::suspend_always{};
        #ifndef XNET_DISABLE_THREAD_SAFE
            std::get<worker_id>(awaiter.result) = std::move(details::result());
            size_t seen = awaiter.done.fetch_add(1, std::memory_order_acq_rel);
            bool need_resume = (seen + 1 == num_ops);
            if(need_resume){
                awaiter.handler.resume();
            }
        #else
            std::get<worker_id>(awaiter.result) = std::move(details::result());
            ++awaiter.done;
            if(awaiter.done == num_ops){
                awaiter.handler.resume();
            }
        #endif
        }

        template<size_t... Is>
        void set_all_cancelled(std::string_view error_str, std::index_sequence<Is...>) noexcept{
            (std::get<Is>(result).template set<xredis::RedisValue::simple_error_t>(error_str), ...);
        }

    public:
        template<class... Strings>
        RedisCommandOperationAwaiter(T* pipe, Strings&&... strings)
        noexcept: pipe(pipe), ops(std::forward<Strings>(strings)...), result(), handler(), workers(), done(0)
        {}

        RedisCommandOperationAwaiter(const RedisCommandOperationAwaiter&) = delete;

        bool await_ready() const noexcept { return false; }

        template<class Promise>
        bool await_suspend(std::coroutine_handle<Promise> handle) noexcept {
            this->handler = handle;
            if constexpr(requires{ handle.promise().xcoro_hook(this); }){
                if(!handle.promise().xcoro_hook(this)){
                    set_all_cancelled(xredis::RedisClientError::OP_CANCELLED, std::make_index_sequence<num_ops>());
                    return false;
                }
            }
            this->start_all_tasks(std::make_index_sequence<num_ops>());
            std::string_view payloads[num_ops];
            for(size_t i = 0; i < num_ops; i++){
                payloads[i] = this->ops[i];
            }
            int pos = pipe->ring.push_n(this->workers, payloads, num_ops);
            if(pos >= 0){
                auto& ctx = pipe->event.context();
                bool prep_success = false;

                ctx.lock();
                io_uring* ring = ctx.native();
                io_uring_sqe* sqe = io_uring_get_sqe(ring);
                if(sqe != nullptr){
                    io_uring_prep_write(sqe, pipe->event.fd(), &details::event_signal(), sizeof(size_t), 0);
                    io_uring_sqe_set_data(sqe, nullptr);
                    prep_success = true;
                }
                ctx.unlock();

                if(!prep_success){
                    [[maybe_unused]] ssize_t n = ::write(pipe->event.fd(), &details::event_signal(), sizeof(size_t));
                }
                return true;
            }
            for(size_t i = 0; i < num_ops; i++){
                this->workers[i].destroy();
            }
            set_all_cancelled(
                pos == -1 ? xredis::RedisClientError::RING_OVERFLOW : xredis::RedisClientError::CONN_ERR,
                std::make_index_sequence<num_ops>()
            );
            return false;
        }

        auto&& await_resume() noexcept{
            this->handler = nullptr;
            return std::move(this->result);
        }

        xnet::io_result<bool> cancel() noexcept{
            return {true, 0};
        }
        std::coroutine_handle<>& handle() noexcept{ return this->handler; }
    };

    template<class T, size_t num_ops, bool multi = false>
    class ClusterCommandOperationAwaiter{
        using Tuple = decltype(details::generate_tuple_result_t(std::make_index_sequence<num_ops>()));

        T* cluster;
        std::array<uint16_t, num_ops> slots;
        std::string ops[num_ops];
        Tuple result;
        std::coroutine_handle<> handler;
        std::coroutine_handle<> workers[num_ops];
    #ifndef XNET_DISABLE_THREAD_SAFE
        std::atomic<size_t> done;
    #else
        size_t done;
    #endif
        template<size_t... Is>
        void start_all_tasks(std::index_sequence<Is...>) noexcept{
            (worker<Is>(*this), ...);
        }

        template<size_t worker_id>
        static xnet::detached_task worker(ClusterCommandOperationAwaiter& awaiter) noexcept{
            awaiter.workers[worker_id] = co_await details::get_handler{};
            co_await std::suspend_always{};
        #ifndef XNET_DISABLE_THREAD_SAFE
            std::get<worker_id>(awaiter.result) = std::move(details::result());
            size_t seen = awaiter.done.fetch_add(1, std::memory_order_acq_rel);
            bool need_resume = (seen + 1 == num_ops);
            if(need_resume){
                awaiter.handler.resume();
            }
        #else
            std::get<worker_id>(awaiter.result) = std::move(details::result());
            ++awaiter.done;
            if(awaiter.done == num_ops){
                awaiter.handler.resume();
            }
        #endif
        }

        template<size_t... Is>
        void set_all_cancelled(std::string_view error_str, std::index_sequence<Is...>) noexcept{
            (std::get<Is>(result).template set<xredis::RedisValue::simple_error_t>(error_str), ...);
        }

        template<class Pipe, size_t idx>
        void push_pipe(Pipe** pipes_set) noexcept{
            Pipe* pipe = cluster->get_pipe(this->slots[idx]);
            size_t set_idx = 0;
            bool write = false;
            for(size_t i = 0; i < num_ops; i++){
                if(pipes_set[i] == pipe){
                    break;
                }
                else if(pipes_set[i] == nullptr){
                    pipes_set[i] = pipe;
                    set_idx = i;
                    write = true;
                    break;
                }
            }
            int pos = pipe->ring.push(this->workers[idx], this->ops[idx]);
            if(pos < 0){
                this->workers[idx].destroy();
                std::get<idx>(this->result).template set<xredis::RedisValue::simple_error_t>(
                    pos == -1 ? xredis::RedisClientError::RING_OVERFLOW : xredis::RedisClientError::CONN_ERR
                );
                if(write){
                    pipes_set[set_idx] = nullptr;
                }
                #ifndef XNET_DISABLE_THREAD_SAFE
                    this->done.fetch_add(1, std::memory_order_release);
                #else
                    ++this->done;
                #endif
            }
            return;
        }

        template<class Pipe, size_t... Is>
        void push_all_pipe(Pipe** pipes_set, std::index_sequence<Is...>) noexcept{
            (push_pipe<Pipe, Is>(pipes_set), ...);
        }

    public:
        template<class... Strings>
        ClusterCommandOperationAwaiter(T* cluster, std::array<uint16_t, num_ops>&& slots, Strings&&... ops)
        noexcept: cluster(cluster), slots(std::move(slots)), ops(std::forward<Strings>(ops)...), result(), handler(), workers(), done(0)
        {}
        ClusterCommandOperationAwaiter(const ClusterCommandOperationAwaiter&) = delete;

        bool await_ready() noexcept{
            if constexpr(multi){
                size_t same_slot = slots[0];
                for(size_t i = 1; i < num_ops; i++){
                    if(slots[i] != same_slot){
                        set_all_cancelled(xredis::RedisClientError::CROSS_SLOT, std::make_index_sequence<num_ops>());
                        return true;
                    }
                }
            }
            return false;
        }

        template<class Promise>
        bool await_suspend(std::coroutine_handle<Promise> handle) noexcept{
            this->handler = handle;
            if constexpr(requires { handle.promise().xcoro_hook(this); }){
                if(!handle.promise().xcoro_hook(this)){
                    set_all_cancelled(xredis::RedisClientError::OP_CANCELLED, std::make_index_sequence<num_ops>());
                    return false;
                }
            }
            this->start_all_tasks(std::make_index_sequence<num_ops>());
            if constexpr(multi){
                auto* pipe = cluster->get_pipe(this->slots[0]);
                std::string_view payloads[num_ops];
                for(size_t i = 0; i < num_ops; i++){
                    payloads[i] = this->ops[i];
                }
                int pos = pipe->ring.push_n(this->workers, payloads, num_ops);
                if(pos >= 0){
                    auto& ctx = pipe->event.context();
                    bool prep_success = false;
                    
                    ctx.lock();
                    io_uring* ring = ctx.native();
                    io_uring_sqe* sqe = io_uring_get_sqe(ring);
                    if(sqe != nullptr){
                        io_uring_prep_write(sqe, pipe->event.fd(), &details::event_signal(), sizeof(size_t), 0);
                        io_uring_sqe_set_data(sqe, nullptr);
                        prep_success = true;
                    }
                    ctx.unlock();

                    if(!prep_success){
                        [[maybe_unused]] ssize_t n = ::write(pipe->event.fd(), &details::event_signal(), sizeof(size_t));
                    }
                    return true;
                }
                for(size_t i = 0; i < num_ops; i++){
                    this->workers[i].destroy();
                }
                set_all_cancelled(
                    pos == -1 ? xredis::RedisClientError::RING_OVERFLOW : xredis::RedisClientError::CONN_ERR,
                    std::make_index_sequence<num_ops>()
                );
                return false;
            }

            // not multi
            using pipe_pointer_t = decltype(cluster->get_pipe(0));
            pipe_pointer_t pipes_set[num_ops] = {};
            this->push_all_pipe(pipes_set, std::make_index_sequence<num_ops>());
            for(size_t i = 0; i < num_ops; i++){
                pipe_pointer_t pipe = pipes_set[i];
                if(!pipe){
                    break;
                }
                auto& ctx = pipe->event.context();
                bool prep_success = false;

                ctx.lock();
                io_uring* ring = ctx.native();
                io_uring_sqe* sqe = io_uring_get_sqe(ring);
                if(sqe != nullptr){
                    io_uring_prep_write(sqe, pipe->event.fd(), &details::event_signal(), sizeof(size_t), 0);
                    io_uring_sqe_set_data(sqe, nullptr);
                    prep_success = true;
                }
                ctx.unlock();

                if(!prep_success){
                    [[maybe_unused]] ssize_t n = ::write(pipe->event.fd(), &details::event_signal(), sizeof(size_t));
                }
            }
            #ifndef XNET_DISABLE_THREAD_SAFE
                bool complete_early = this->done.load(std::memory_order_acquire) == num_ops;
                return !complete_early;
            #else
                return this->done != num_ops;
            #endif
        }

        auto&& await_resume() noexcept{
            this->handler = nullptr;
            return std::move(this->result);
        }

        xnet::io_result<bool> cancel() noexcept{
            return {true, 0};
        }
        std::coroutine_handle<>& handle() noexcept { return this->handler; }
    };

    template<class T, size_t num_ops, bool cluster_mode = false, bool multi = false>
    struct [[nodiscard]] RedisCommandOperation{
        // T* pipe;
        struct ClusterPipe{
            T* cluster;
            std::array<uint16_t, num_ops> slots;
        };
        using pipe_t = std::conditional_t<cluster_mode, ClusterPipe, T*>;

        pipe_t pipe;
        std::string ops[num_ops];

        template<class... String>
        RedisCommandOperation(T* pipe, String&&... strings) 
        noexcept: pipe(pipe), ops(std::forward<String>(strings)...)
        {}

        template<class... String>
        RedisCommandOperation(T* cluster, uint16_t slot, String&&... ops)
        noexcept: pipe{cluster, {slot}}, ops(std::forward<String>(ops)...)
        {}
        template<class... String>
        RedisCommandOperation(T* cluster, std::array<uint16_t, num_ops>&& slots, String&&... ops)
        noexcept: pipe{cluster, std::move(slots)}, ops(std::forward<String>(ops)...)
        {}

        RedisCommandOperation(RedisCommandOperation&&) = default;

        template<bool ifcluster, size_t... Is, typename std::enable_if<!ifcluster, bool>::type = true>
        auto co_await_transform(std::index_sequence<Is...>) & noexcept{
            return RedisCommandOperationAwaiter<T, num_ops>(
                pipe, this->ops[Is]...
            );
        }
        template<bool ifcluster, size_t... Is, typename std::enable_if<!ifcluster, bool>::type = true>
        auto co_await_transform(std::index_sequence<Is...>) && noexcept{
            return RedisCommandOperationAwaiter<T, num_ops>(
                pipe, std::move(this->ops[Is])...
            );
        }

        template<bool ifcluster, size_t... Is, typename std::enable_if<ifcluster && sizeof...(Is) == 1, bool>::type = true>
        auto co_await_transform(std::index_sequence<Is...>) && noexcept{
            auto* raw_pipe = this->pipe.cluster->get_pipe(this->pipe.slots[0]);
            return CommandAwaiter<std::remove_pointer_t<decltype(raw_pipe)>>(raw_pipe, std::move(ops[0]));
        }
        template<bool ifcluster, size_t... Is, typename std::enable_if<ifcluster && sizeof...(Is) == 1, bool>::type = true>
        auto co_await_transform(std::index_sequence<Is...>) & noexcept{
            auto* raw_pipe = this->pipe.cluster->get_pipe(this->pipe.slots[0]);
            return CommandAwaiter<std::remove_pointer_t<decltype(raw_pipe)>>(raw_pipe, std::string(ops[0]));
        }
        
        template<bool ifcluster, size_t... Is, typename std::enable_if<ifcluster && (sizeof...(Is) > 1), bool>::type = true>
        auto co_await_transform(std::index_sequence<Is...>) & noexcept{
            return ClusterCommandOperationAwaiter<T, num_ops, multi>(pipe.cluster, {pipe.slots[Is]...}, this->ops[Is]...);
        }
        template<bool ifcluster, size_t... Is, typename std::enable_if<ifcluster && (sizeof...(Is) > 1), bool>::type = true>
        auto co_await_transform(std::index_sequence<Is...>) && noexcept{
            return ClusterCommandOperationAwaiter<T, num_ops, multi>(pipe.cluster, {pipe.slots[Is]...}, std::move(this->ops[Is])...);
        }

        auto operator co_await() & noexcept{
            return this->template co_await_transform<cluster_mode>(std::make_index_sequence<num_ops>());
        }
        auto operator co_await() && noexcept{
            return std::move(*this).template co_await_transform<cluster_mode>(std::make_index_sequence<num_ops>());
        }

        template<size_t... Is>
        auto chain(std::string&& new_op, std::index_sequence<Is...>) & noexcept{
            return RedisCommandOperation<T, num_ops + 1, cluster_mode, multi>(this->pipe, this->ops[Is]..., std::move(new_op));
        }
        template<size_t... Is>
        auto chain(std::string&& new_op, std::index_sequence<Is...>) && noexcept{
            return RedisCommandOperation<T, num_ops + 1, cluster_mode, multi>(this->pipe, std::move(this->ops[Is])..., std::move(new_op));
        }
        template<size_t... Is>
        auto chain(uint16_t new_slot, std::string&& new_op, std::index_sequence<Is...>) & noexcept{
            return RedisCommandOperation<T, num_ops + 1, cluster_mode, multi>(pipe.cluster, {pipe.slots[Is]..., new_slot}, this->ops[Is]..., std::move(new_op));
        }
        template<size_t... Is>
        auto chain(uint16_t new_slot, std::string&& new_op, std::index_sequence<Is...>) && noexcept{
            return RedisCommandOperation<T, num_ops + 1, cluster_mode, multi>(pipe.cluster, {pipe.slots[Is]..., new_slot}, std::move(this->ops[Is])..., std::move(new_op));
        }

        // for single || cluster forward
        template<bool ifcluster, class... Args, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _command(Args&&... args) & noexcept{
            return this->chain(xredis::build_commands(std::string_view(args)...), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, class... Args, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _command(Args&&... args) && noexcept{
            return std::move(*this).chain(xredis::build_commands(std::string_view(args)...), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, class... Args, typename std::enable_if<ifcluster, bool>::type = true>
        auto _command(std::string_view cmd, std::string_view key, Args&&... args) & noexcept{
            uint16_t slot = xredis::get_slot(key.data(), key.size());
            return this->chain(slot, xredis::build_commands(cmd, key, std::string_view(std::forward<Args>(args))...), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, class... Args, typename std::enable_if<ifcluster, bool>::type = true>
        auto _command(std::string_view cmd, std::string_view key, Args&&... args) && noexcept{
            uint16_t slot = xredis::get_slot(key.data(), key.size());
            return std::move(*this).chain(slot, xredis::build_commands(cmd, key, std::string_view(std::forward<Args>(args))...), std::make_index_sequence<num_ops>());
        }

        template<bool ifcluster, std::input_iterator InputIt, class... Args, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _command_range(std::string_view cmd, InputIt begin, size_t count) & noexcept{
            return this->chain(xredis::build_commands_from_range(cmd, begin, count), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, std::input_iterator InputIt, class... Args, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _command_range(std::string_view cmd, InputIt begin, size_t count) && noexcept{
            return std::move(*this).chain(xredis::build_commands_from_range(cmd, begin, count), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, std::input_iterator InputIt, class... Args, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _command_range(std::string_view cmd, std::string_view key, InputIt begin, size_t count) & noexcept{
            return this->chain(xredis::build_commands_from_range(cmd, key, begin, count), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, std::input_iterator InputIt, class... Args, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _command_range(std::string_view cmd, std::string_view key, InputIt begin, size_t count) && noexcept{
            return std::move(*this).chain(xredis::build_commands_from_range(cmd, key, begin, count), std::make_index_sequence<num_ops>());
        }

        template<bool ifcluster, std::input_iterator InputIt, class... Args, typename std::enable_if<ifcluster, bool>::type = true>
        auto _command_range(std::string_view cmd, std::string_view key, InputIt begin, size_t count) & noexcept{
            uint16_t slot = xredis::get_slot(key.data(), key.size());
            return this->chain(slot, xredis::build_commands_from_range(cmd, key, begin, count), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, std::input_iterator InputIt, class... Args, typename std::enable_if<ifcluster, bool>::type = true>
        auto _command_range(std::string_view cmd, std::string_view key, InputIt begin, size_t count) && noexcept{
            uint16_t slot = xredis::get_slot(key.data(), key.size());
            return std::move(*this).chain(slot, xredis::build_commands_from_range(cmd, key, begin, count), std::make_index_sequence<num_ops>());
        }

        template<class... Args>
        auto command(Args&&... args) & noexcept{
            return this->template _command<cluster_mode>(std::forward<Args>(args)...);
        }
        template<class... Args>
        auto command(Args&&... args) && noexcept{
            return std::move(*this).template _command<cluster_mode>(std::forward<Args>(args)...);
        }
        template<std::input_iterator InputIt>
        auto command_range(std::string_view cmd, InputIt begin, size_t count) & noexcept{
            return this->template _command_range<cluster_mode>(cmd, begin, count);
        }
        template<std::input_iterator InputIt>
        auto command_range(std::string_view cmd, InputIt begin, size_t count) && noexcept{
            return std::move(*this).template _command_range<cluster_mode>(cmd, begin, count);
        }
        template<std::input_iterator InputIt>
        auto command_range(std::string_view cmd, std::string_view key, InputIt begin, size_t count) & noexcept{
            return this->template _command_range<cluster_mode>(cmd, key, begin, count);
        }
        template<std::input_iterator InputIt>
        auto command_range(std::string_view cmd, std::string_view key, InputIt begin, size_t count) && noexcept{
            return std::move(*this).template _command_range<cluster_mode>(cmd, key, begin, count);
        }
        
        auto ping() & noexcept{
            return this->command("PING");
        }
        auto ping() && noexcept{
            return std::move(*this).command("PING");
        }

        auto select(size_t index) & noexcept{
            return this->command("SELECT", details::to_string(index));
        }
        auto select(size_t index) && noexcept{
            return std::move(*this).command("SELECT", details::to_string(index));
        }

        auto flushall(bool async = false) & noexcept{
            return this->command("FLUSHALL", async ? "ASYNC" : "SYNC");
        }
        auto flushall(bool async = false) && noexcept{
            return std::move(*this).command("FLUSHALL", async ? "ASYNC" : "SYNC");
        }

        auto flushdb(bool async = false) & noexcept{
            return this->command("FLUSHDB", async ? "ASYNC" : "SYNC");
        }
        auto flushdb(bool async = false) && noexcept{
            return std::move(*this).command("FLUSHDB", async ? "ASYNC" : "SYNC");
        }

        auto dbsize() & noexcept{
            return this->command("DBSIZE");
        }
        auto dbsize() && noexcept{
            return std::move(*this).command("DBSIZE");
        }

        auto save() & noexcept{
            return this->command("SAVE");
        }
        auto save() && noexcept{
            return std::move(*this).command("SAVE");
        }

        template<typename... Args>
        auto shutdown(Args&&... args) & noexcept{
            return this->command("SHUTDOWN", std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto shutdown(Args&&... args) && noexcept{
            return std::move(*this).command("SHUTDOWN", std::forward<Args>(args)...);
        }

        auto publish(std::string_view channel, std::string_view message) & noexcept{
            return this->command("PUBLISH", channel, message);
        }
        auto publish(std::string_view channel, std::string_view message) && noexcept{
            return std::move(*this).command("PUBLISH", channel, message);
        }

        auto spublish(std::string_view shardchannel, std::string_view message) & noexcept{
            return this->command("SPUBLISH", shardchannel, message);
        }
        auto spublish(std::string_view shardchannel, std::string_view message) && noexcept{
            return std::move(*this).command("SPUBLISH", shardchannel, message);
        }

        // ---- String ----

        auto append(std::string_view key, std::string_view value) & noexcept{
            return this->command("APPEND", key, value);
        }
        auto append(std::string_view key, std::string_view value) && noexcept{
            return std::move(*this).command("APPEND", key, value);
        }

        auto decr(std::string_view key) & noexcept{
            return this->command("DECR", key);
        }
        auto decr(std::string_view key) && noexcept{
            return std::move(*this).command("DECR", key);
        }

        auto decrby(std::string_view key, int64_t decrement) & noexcept{
            return this->command("DECRBY", key, details::to_string(decrement));
        }
        auto decrby(std::string_view key, int64_t decrement) && noexcept{
            return std::move(*this).command("DECRBY", key, details::to_string(decrement));
        }

        auto get(std::string_view key) & noexcept{
            return this->command("GET", key);
        }
        auto get(std::string_view key) && noexcept{
            return std::move(*this).command("GET", key);
        }

        auto getdel(std::string_view key) & noexcept{
            return this->command("GETDEL", key);
        }
        auto getdel(std::string_view key) && noexcept{
            return std::move(*this).command("GETDEL", key);
        }

        template<typename... Args>
        auto getex(std::string_view key, Args&&... args) & noexcept{
            return this->command("GETEX", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto getex(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("GETEX", key, std::forward<Args>(args)...);
        }

        auto getrange(std::string_view key, int64_t start, int64_t end) & noexcept{
            return this->command("GETRANGE", key, details::to_string(start), details::to_string(end));
        }
        auto getrange(std::string_view key, int64_t start, int64_t end) && noexcept{
            return std::move(*this).command("GETRANGE", key, details::to_string(start), details::to_string(end));
        }

        auto incr(std::string_view key) & noexcept{
            return this->command("INCR", key);
        }
        auto incr(std::string_view key) && noexcept{
            return std::move(*this).command("INCR", key);
        }

        auto incrby(std::string_view key, int64_t increment) & noexcept{
            return this->command("INCRBY", key, details::to_string(increment));
        }
        auto incrby(std::string_view key, int64_t increment) && noexcept{
            return std::move(*this).command("INCRBY", key, details::to_string(increment));
        }

        auto incrbyfloat(std::string_view key, double increment) & noexcept{
            return this->command("INCRBYFLOAT", key, details::to_string(increment));
        }
        auto incrbyfloat(std::string_view key, double increment) && noexcept{
            return std::move(*this).command("INCRBYFLOAT", key, details::to_string(increment));
        }

        template<typename... Args>
        auto lcs(std::string_view key1, std::string_view key2, Args&&... args) & noexcept{
            return this->command("LCS", key1, key2, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto lcs(std::string_view key1, std::string_view key2, Args&&... args) && noexcept{
            return std::move(*this).command("LCS", key1, key2, std::forward<Args>(args)...);
        }

        template<typename... Keys>
        auto mget(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("MGET", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto mget(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("MGET", first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Args>
        requires (sizeof...(Args) > 0 && sizeof...(Args) % 2 == 0)
        auto mset(Args&&... args) & noexcept{
            return this->command("MSET", std::forward<Args>(args)...);
        }
        template<typename... Args>
        requires (sizeof...(Args) > 0 && sizeof...(Args) % 2 == 0)
        auto mset(Args&&... args) && noexcept{
            return std::move(*this).command("MSET", std::forward<Args>(args)...);
        }

        template<typename... Args>
        requires (sizeof...(Args) > 0 && sizeof...(Args) % 2 == 0)
        auto msetnx(Args&&... args) & noexcept{
            return this->command("MSETNX", std::forward<Args>(args)...);
        }
        template<typename... Args>
        requires (sizeof...(Args) > 0 && sizeof...(Args) % 2 == 0)
        auto msetnx(Args&&... args) && noexcept{
            return std::move(*this).command("MSETNX", std::forward<Args>(args)...);
        }

        auto psetex(std::string_view key, int64_t milliseconds, std::string_view value) & noexcept{
            return this->command("PSETEX", key, details::to_string(milliseconds), value);
        }
        auto psetex(std::string_view key, int64_t milliseconds, std::string_view value) && noexcept{
            return std::move(*this).command("PSETEX", key, details::to_string(milliseconds), value);
        }

        template<typename... Args>
        auto set(std::string_view key, std::string_view value, Args&&... args) & noexcept{
            return this->command("SET", key, value, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto set(std::string_view key, std::string_view value, Args&&... args) && noexcept{
            return std::move(*this).command("SET", key, value, std::forward<Args>(args)...);
        }

        auto setex(std::string_view key, int64_t seconds, std::string_view value) & noexcept{
            return this->command("SETEX", key, details::to_string(seconds), value);
        }
        auto setex(std::string_view key, int64_t seconds, std::string_view value) && noexcept{
            return std::move(*this).command("SETEX", key, details::to_string(seconds), value);
        }

        auto setnx(std::string_view key, std::string_view value) & noexcept{
            return this->command("SETNX", key, value);
        }
        auto setnx(std::string_view key, std::string_view value) && noexcept{
            return std::move(*this).command("SETNX", key, value);
        }

        auto setrange(std::string_view key, int64_t offset, std::string_view value) & noexcept{
            return this->command("SETRANGE", key, details::to_string(offset), value);
        }
        auto setrange(std::string_view key, int64_t offset, std::string_view value) && noexcept{
            return std::move(*this).command("SETRANGE", key, details::to_string(offset), value);
        }

        auto strlen(std::string_view key) & noexcept{
            return this->command("STRLEN", key);
        }
        auto strlen(std::string_view key) && noexcept{
            return std::move(*this).command("STRLEN", key);
        }

        // ---- Hash ----

        template<typename... Fields>
        auto hdel(std::string_view key, std::string_view first_field, Fields&&... other_fields) & noexcept{
            return this->command("HDEL", key, first_field, std::forward<Fields>(other_fields)...);
        }
        template<typename... Fields>
        auto hdel(std::string_view key, std::string_view first_field, Fields&&... other_fields) && noexcept{
            return std::move(*this).command("HDEL", key, first_field, std::forward<Fields>(other_fields)...);
        }

        auto hexists(std::string_view key, std::string_view field) & noexcept{
            return this->command("HEXISTS", key, field);
        }
        auto hexists(std::string_view key, std::string_view field) && noexcept{
            return std::move(*this).command("HEXISTS", key, field);
        }

        template<typename... Args>
        auto hexpire(std::string_view key, int64_t seconds, Args&&... fields) & noexcept{
            return this->command("HEXPIRE", key, details::to_string(seconds), std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto hexpire(std::string_view key, int64_t seconds, Args&&... fields) && noexcept{
            return std::move(*this).command("HEXPIRE", key, details::to_string(seconds), std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hexpireat(std::string_view key, int64_t timestamp, Args&&... fields) & noexcept{
            return this->command("HEXPIREAT", key, details::to_string(timestamp), std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto hexpireat(std::string_view key, int64_t timestamp, Args&&... fields) && noexcept{
            return std::move(*this).command("HEXPIREAT", key, details::to_string(timestamp), std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hexpiretime(std::string_view key, Args&&... fields) & noexcept{
            return this->command("HEXPIRETIME", key, std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto hexpiretime(std::string_view key, Args&&... fields) && noexcept{
            return std::move(*this).command("HEXPIRETIME", key, std::forward<Args>(fields)...);
        }

        auto hget(std::string_view key, std::string_view field) & noexcept{
            return this->command("HGET", key, field);
        }
        auto hget(std::string_view key, std::string_view field) && noexcept{
            return std::move(*this).command("HGET", key, field);
        }

        auto hgetall(std::string_view key) & noexcept{
            return this->command("HGETALL", key);
        }
        auto hgetall(std::string_view key) && noexcept{
            return std::move(*this).command("HGETALL", key);
        }

        auto hgetdel(std::string_view key, std::string_view field) & noexcept{
            return this->command("HGETDEL", key, field);
        }
        auto hgetdel(std::string_view key, std::string_view field) && noexcept{
            return std::move(*this).command("HGETDEL", key, field);
        }

        template<typename... Args>
        auto hgetex(std::string_view key, std::string_view field, Args&&... args) & noexcept{
            return this->command("HGETEX", key, field, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto hgetex(std::string_view key, std::string_view field, Args&&... args) && noexcept{
            return std::move(*this).command("HGETEX", key, field, std::forward<Args>(args)...);
        }

        auto hincrby(std::string_view key, std::string_view field, int64_t increment) & noexcept{
            return this->command("HINCRBY", key, field, details::to_string(increment));
        }
        auto hincrby(std::string_view key, std::string_view field, int64_t increment) && noexcept{
            return std::move(*this).command("HINCRBY", key, field, details::to_string(increment));
        }

        auto hincrbyfloat(std::string_view key, std::string_view field, double increment) & noexcept{
            return this->command("HINCRBYFLOAT", key, field, details::to_string(increment));
        }
        auto hincrbyfloat(std::string_view key, std::string_view field, double increment) && noexcept{
            return std::move(*this).command("HINCRBYFLOAT", key, field, details::to_string(increment));
        }

        auto hkeys(std::string_view key) & noexcept{
            return this->command("HKEYS", key);
        }
        auto hkeys(std::string_view key) && noexcept{
            return std::move(*this).command("HKEYS", key);
        }

        auto hlen(std::string_view key) & noexcept{
            return this->command("HLEN", key);
        }
        auto hlen(std::string_view key) && noexcept{
            return std::move(*this).command("HLEN", key);
        }

        template<typename... Fields>
        auto hmget(std::string_view key, std::string_view first_field, Fields&&... other_fields) & noexcept{
            return this->command("HMGET", key, first_field, std::forward<Fields>(other_fields)...);
        }
        template<typename... Fields>
        auto hmget(std::string_view key, std::string_view first_field, Fields&&... other_fields) && noexcept{
            return std::move(*this).command("HMGET", key, first_field, std::forward<Fields>(other_fields)...);
        }

        template<typename... Args>
        auto hpersist(std::string_view key, Args&&... fields) & noexcept{
            return this->command("HPERSIST", key, std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto hpersist(std::string_view key, Args&&... fields) && noexcept{
            return std::move(*this).command("HPERSIST", key, std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hpexpire(std::string_view key, int64_t milliseconds, Args&&... fields) & noexcept{
            return this->command("HPEXPIRE", key, details::to_string(milliseconds), std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto hpexpire(std::string_view key, int64_t milliseconds, Args&&... fields) && noexcept{
            return std::move(*this).command("HPEXPIRE", key, details::to_string(milliseconds), std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hpexpireat(std::string_view key, int64_t milliseconds_timestamp, Args&&... fields) & noexcept{
            return this->command("HPEXPIREAT", key, details::to_string(milliseconds_timestamp), std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto hpexpireat(std::string_view key, int64_t milliseconds_timestamp, Args&&... fields) && noexcept{
            return std::move(*this).command("HPEXPIREAT", key, details::to_string(milliseconds_timestamp), std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hpexpiretime(std::string_view key, Args&&... fields) & noexcept{
            return this->command("HPEXPIRETIME", key, std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto hpexpiretime(std::string_view key, Args&&... fields) && noexcept{
            return std::move(*this).command("HPEXPIRETIME", key, std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hpttl(std::string_view key, Args&&... fields) & noexcept{
            return this->command("HPTTL", key, std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto hpttl(std::string_view key, Args&&... fields) && noexcept{
            return std::move(*this).command("HPTTL", key, std::forward<Args>(fields)...);
        }

        template<typename... Args>
        auto hrandfield(std::string_view key, Args&&... args) & noexcept{
            return this->command("HRANDFIELD", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto hrandfield(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("HRANDFIELD", key, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto hscan(std::string_view key, int64_t cursor, Args&&... args) & noexcept{
            return this->command("HSCAN", key, details::to_string(cursor), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto hscan(std::string_view key, int64_t cursor, Args&&... args) && noexcept{
            return std::move(*this).command("HSCAN", key, details::to_string(cursor), std::forward<Args>(args)...);
        }

        template<typename... Args>
        requires (sizeof...(Args) >= 2 && sizeof...(Args) % 2 == 0)
        auto hset(std::string_view key, Args&&... args) & noexcept{
            return this->command("HSET", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        requires (sizeof...(Args) >= 2 && sizeof...(Args) % 2 == 0)
        auto hset(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("HSET", key, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto hsetex(std::string_view key, Args&&... args) & noexcept{
            return this->command("HSETEX", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto hsetex(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("HSETEX", key, std::forward<Args>(args)...);
        }

        auto hsetnx(std::string_view key, std::string_view field, std::string_view value) & noexcept{
            return this->command("HSETNX", key, field, value);
        }
        auto hsetnx(std::string_view key, std::string_view field, std::string_view value) && noexcept{
            return std::move(*this).command("HSETNX", key, field, value);
        }

        auto hstrlen(std::string_view key, std::string_view field) & noexcept{
            return this->command("HSTRLEN", key, field);
        }
        auto hstrlen(std::string_view key, std::string_view field) && noexcept{
            return std::move(*this).command("HSTRLEN", key, field);
        }

        template<typename... Args>
        auto httl(std::string_view key, Args&&... fields) & noexcept{
            return this->command("HTTL", key, std::forward<Args>(fields)...);
        }
        template<typename... Args>
        auto httl(std::string_view key, Args&&... fields) && noexcept{
            return std::move(*this).command("HTTL", key, std::forward<Args>(fields)...);
        }

        auto hvals(std::string_view key) & noexcept{
            return this->command("HVALS", key);
        }
        auto hvals(std::string_view key) && noexcept{
            return std::move(*this).command("HVALS", key);
        }

        // ---- List ----
        auto blmove(std::string_view source, std::string_view destination, std::string_view wherefrom, std::string_view whereto, double timeout) & noexcept{
            return this->command("BLMOVE", source, destination, wherefrom, whereto, details::to_string(timeout));
        }
        auto blmove(std::string_view source, std::string_view destination, std::string_view wherefrom, std::string_view whereto, double timeout) && noexcept{
            return std::move(*this).command("BLMOVE", source, destination, wherefrom, whereto, details::to_string(timeout));
        }

        template<bool ifcluster, typename... Args, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _blmpop(double timeout, int64_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            return this->command("BLMPOP", details::to_string(timeout), details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }
        template<bool ifcluster, typename... Args, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _blmpop(double timeout, int64_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            return std::move(*this).command("BLMPOP", details::to_string(timeout), details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }
        template<bool ifcluster, typename... Args, typename std::enable_if<ifcluster, bool>::type = true>
        auto _blmpop(double timeout, int64_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return this->chain(slot, xredis::build_commands(
                std::string_view("BLMPOP"),
                details::to_string(timeout),
                details::to_string(numkeys),
                first_key, std::string_view(std::forward<Args>(args))...
            ), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, typename... Args, typename std::enable_if<ifcluster, bool>::type = true>
        auto _blmpop(double timeout, int64_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return std::move(*this).chain(slot, xredis::build_commands(
                std::string_view("BLMPOP"),
                details::to_string(timeout),
                details::to_string(numkeys),
                first_key, std::string_view(std::forward<Args>(args))...
            ), std::make_index_sequence<num_ops>());
        }
        template<typename... Args>
        auto blmpop(double timeout, int64_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            return this->template _blmpop<cluster_mode>(timeout, numkeys, first_key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto blmpop(double timeout, int64_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            return std::move(*this).template _blmpop<cluster_mode>(timeout, numkeys, first_key, std::forward<Args>(args)...);
        }

        template<typename... Keys>
        auto blpop(std::string_view first_key, Keys&&... other_keys_and_timeout) & noexcept{
            return this->command("BLPOP", first_key, std::forward<Keys>(other_keys_and_timeout)...);
        }
        template<typename... Keys>
        auto blpop(std::string_view first_key, Keys&&... other_keys_and_timeout) && noexcept{
            return std::move(*this).command("BLPOP", first_key, std::forward<Keys>(other_keys_and_timeout)...);
        }

        template<typename... Keys>
        auto brpop(std::string_view first_key, Keys&&... other_keys_and_timeout) & noexcept{
            return this->command("BRPOP", first_key, std::forward<Keys>(other_keys_and_timeout)...);
        }
        template<typename... Keys>
        auto brpop(std::string_view first_key, Keys&&... other_keys_and_timeout) && noexcept{
            return std::move(*this).command("BRPOP", first_key, std::forward<Keys>(other_keys_and_timeout)...);
        }

        auto lindex(std::string_view key, int64_t index) & noexcept{
            return this->command("LINDEX", key, details::to_string(index));
        }
        auto lindex(std::string_view key, int64_t index) && noexcept{
            return std::move(*this).command("LINDEX", key, details::to_string(index));
        }

        auto linsert(std::string_view key, std::string_view where, std::string_view pivot, std::string_view element) & noexcept{
            return this->command("LINSERT", key, where, pivot, element);
        }
        auto linsert(std::string_view key, std::string_view where, std::string_view pivot, std::string_view element) && noexcept{
            return std::move(*this).command("LINSERT", key, where, pivot, element);
        }

        auto llen(std::string_view key) & noexcept{
            return this->command("LLEN", key);
        }
        auto llen(std::string_view key) && noexcept{
            return std::move(*this).command("LLEN", key);
        }

        auto lmove(std::string_view source, std::string_view destination, std::string_view wherefrom, std::string_view whereto) & noexcept{
            return this->command("LMOVE", source, destination, wherefrom, whereto);
        }
        auto lmove(std::string_view source, std::string_view destination, std::string_view wherefrom, std::string_view whereto) && noexcept{
            return std::move(*this).command("LMOVE", source, destination, wherefrom, whereto);
        }

        template<bool ifcluster, typename... Args, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _lmpop(int64_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            return this->command("LMPOP", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }
        template<bool ifcluster, typename... Args, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _lmpop(int64_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            return std::move(*this).command("LMPOP", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }
        template<bool ifcluster, typename... Args, typename std::enable_if<ifcluster, bool>::type = true>
        auto _lmpop(int64_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return this->chain(slot, xredis::build_commands(
                std::string_view("LMPOP"),
                details::to_string(numkeys),
                first_key, std::string_view(std::forward<Args>(args))...
            ), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, typename... Args, typename std::enable_if<ifcluster, bool>::type = true>
        auto _lmpop(int64_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return std::move(*this).chain(slot, xredis::build_commands(
                std::string_view("LMPOP"),
                details::to_string(numkeys),
                first_key, std::string_view(std::forward<Args>(args))...
            ), std::make_index_sequence<num_ops>());
        }
        template<typename... Args>
        auto lmpop(int64_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            return this->template _lmpop<cluster_mode>(numkeys, first_key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto lmpop(int64_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            return std::move(*this).template _lmpop<cluster_mode>(numkeys, first_key, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto lpop(std::string_view key, Args&&... count) & noexcept{
            return this->command("LPOP", key, std::forward<Args>(count)...);
        }
        template<typename... Args>
        auto lpop(std::string_view key, Args&&... count) && noexcept{
            return std::move(*this).command("LPOP", key, std::forward<Args>(count)...);
        }

        template<typename... Args>
        auto lpos(std::string_view key, std::string_view element, Args&&... args) & noexcept{
            return this->command("LPOS", key, element, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto lpos(std::string_view key, std::string_view element, Args&&... args) && noexcept{
            return std::move(*this).command("LPOS", key, element, std::forward<Args>(args)...);
        }

        template<typename... Vals>
        auto lpush(std::string_view key, std::string_view first_val, Vals&&... other_vals) & noexcept{
            return this->command("LPUSH", key, first_val, std::forward<Vals>(other_vals)...);
        }
        template<typename... Vals>
        auto lpush(std::string_view key, std::string_view first_val, Vals&&... other_vals) && noexcept{
            return std::move(*this).command("LPUSH", key, first_val, std::forward<Vals>(other_vals)...);
        }

        template<typename... Vals>
        auto lpushx(std::string_view key, std::string_view first_val, Vals&&... other_vals) & noexcept{
            return this->command("LPUSHX", key, first_val, std::forward<Vals>(other_vals)...);
        }
        template<typename... Vals>
        auto lpushx(std::string_view key, std::string_view first_val, Vals&&... other_vals) && noexcept{
            return std::move(*this).command("LPUSHX", key, first_val, std::forward<Vals>(other_vals)...);
        }

        auto lrange(std::string_view key, int64_t start, int64_t stop) & noexcept{
            return this->command("LRANGE", key, details::to_string(start), details::to_string(stop));
        }
        auto lrange(std::string_view key, int64_t start, int64_t stop) && noexcept{
            return std::move(*this).command("LRANGE", key, details::to_string(start), details::to_string(stop));
        }

        auto lrem(std::string_view key, int64_t count, std::string_view element) & noexcept{
            return this->command("LREM", key, details::to_string(count), element);
        }
        auto lrem(std::string_view key, int64_t count, std::string_view element) && noexcept{
            return std::move(*this).command("LREM", key, details::to_string(count), element);
        }

        auto lset(std::string_view key, int64_t index, std::string_view element) & noexcept{
            return this->command("LSET", key, details::to_string(index), element);
        }
        auto lset(std::string_view key, int64_t index, std::string_view element) && noexcept{
            return std::move(*this).command("LSET", key, details::to_string(index), element);
        }

        auto ltrim(std::string_view key, int64_t start, int64_t stop) & noexcept{
            return this->command("LTRIM", key, details::to_string(start), details::to_string(stop));
        }
        auto ltrim(std::string_view key, int64_t start, int64_t stop) && noexcept{
            return std::move(*this).command("LTRIM", key, details::to_string(start), details::to_string(stop));
        }

        auto rpop(std::string_view key) & noexcept{
            return this->command("RPOP", key);
        }
        auto rpop(std::string_view key) && noexcept{
            return std::move(*this).command("RPOP", key);
        }

        template<typename... Vals>
        auto rpush(std::string_view key, std::string_view first_val, Vals&&... other_vals) & noexcept{
            return this->command("RPUSH", key, first_val, std::forward<Vals>(other_vals)...);
        }
        template<typename... Vals>
        auto rpush(std::string_view key, std::string_view first_val, Vals&&... other_vals) && noexcept{
            return std::move(*this).command("RPUSH", key, first_val, std::forward<Vals>(other_vals)...);
        }

        template<typename... Vals>
        auto rpushx(std::string_view key, std::string_view first_val, Vals&&... other_vals) & noexcept{
            return this->command("RPUSHX", key, first_val, std::forward<Vals>(other_vals)...);
        }
        template<typename... Vals>
        auto rpushx(std::string_view key, std::string_view first_val, Vals&&... other_vals) && noexcept{
            return std::move(*this).command("RPUSHX", key, first_val, std::forward<Vals>(other_vals)...);
        }

        // ---- Set ----

        template<typename... Members>
        auto sadd(std::string_view key, std::string_view first_member, Members&&... other_members) & noexcept{
            return this->command("SADD", key, first_member, std::forward<Members>(other_members)...);
        }
        template<typename... Members>
        auto sadd(std::string_view key, std::string_view first_member, Members&&... other_members) && noexcept{
            return std::move(*this).command("SADD", key, first_member, std::forward<Members>(other_members)...);
        }

        auto scard(std::string_view key) & noexcept{
            return this->command("SCARD", key);
        }
        auto scard(std::string_view key) && noexcept{
            return std::move(*this).command("SCARD", key);
        }

        template<typename... Keys>
        auto sdiff(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("SDIFF", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto sdiff(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("SDIFF", first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Keys>
        auto sdiffstore(std::string_view destination, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("SDIFFSTORE", destination, first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto sdiffstore(std::string_view destination, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("SDIFFSTORE", destination, first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Keys>
        auto sinter(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("SINTER", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto sinter(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("SINTER", first_key, std::forward<Keys>(other_keys)...);
        }

        template<bool ifcluster, typename... Args, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _sintercard(int64_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            return this->command("SINTERCARD", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }
        template<bool ifcluster, typename... Args, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _sintercard(int64_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            return std::move(*this).command("SINTERCARD", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }
        template<bool ifcluster, typename... Args, typename std::enable_if<ifcluster, bool>::type = true>
        auto _sintercard(int64_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return this->chain(slot, xredis::build_commands(
                    std::string_view("SINTERCARD"),
                    details::to_string(numkeys),
                    first_key, std::string_view(std::forward<Args>(args))...
            ), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, typename... Args, typename std::enable_if<ifcluster, bool>::type = true>
        auto _sintercard(int64_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return std::move(*this).chain(slot, xredis::build_commands(
                    std::string_view("SINTERCARD"),
                    details::to_string(numkeys),
                    first_key, std::string_view(std::forward<Args>(args))...
            ), std::make_index_sequence<num_ops>());
        }
        template<typename... Args>
        auto sintercard(int64_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            return this->template _sintercard<cluster_mode>(numkeys, first_key, std::forward<Args>(args)...);            
        }
        template<typename... Args>
        auto sintercard(int64_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            return std::move(*this).template _sintercard<cluster_mode>(numkeys, first_key, std::forward<Args>(args)...);
        }
        
        template<typename... Keys>
        auto sinterstore(std::string_view destination, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("SINTERSTORE", destination, first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto sinterstore(std::string_view destination, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("SINTERSTORE", destination, first_key, std::forward<Keys>(other_keys)...);
        }

        auto sismember(std::string_view key, std::string_view member) & noexcept{
            return this->command("SISMEMBER", key, member);
        }
        auto sismember(std::string_view key, std::string_view member) && noexcept{
            return std::move(*this).command("SISMEMBER", key, member);
        }

        auto smembers(std::string_view key) & noexcept{
            return this->command("SMEMBERS", key);
        }
        auto smembers(std::string_view key) && noexcept{
            return std::move(*this).command("SMEMBERS", key);
        }

        template<typename... Members>
        auto smismember(std::string_view key, std::string_view first_member, Members&&... other_members) & noexcept{
            return this->command("SMISMEMBER", key, first_member, std::forward<Members>(other_members)...);
        }
        template<typename... Members>
        auto smismember(std::string_view key, std::string_view first_member, Members&&... other_members) && noexcept{
            return std::move(*this).command("SMISMEMBER", key, first_member, std::forward<Members>(other_members)...);
        }

        auto smove(std::string_view source, std::string_view destination, std::string_view member) & noexcept{
            return this->command("SMOVE", source, destination, member);
        }
        auto smove(std::string_view source, std::string_view destination, std::string_view member) && noexcept{
            return std::move(*this).command("SMOVE", source, destination, member);
        }

        template<typename... Args>
        auto spop(std::string_view key, Args&&... count) & noexcept{
            return this->command("SPOP", key, std::forward<Args>(count)...);
        }
        template<typename... Args>
        auto spop(std::string_view key, Args&&... count) && noexcept{
            return std::move(*this).command("SPOP", key, std::forward<Args>(count)...);
        }

        template<typename... Args>
        auto srandmember(std::string_view key, Args&&... count) & noexcept{
            return this->command("SRANDMEMBER", key, std::forward<Args>(count)...);
        }
        template<typename... Args>
        auto srandmember(std::string_view key, Args&&... count) && noexcept{
            return std::move(*this).command("SRANDMEMBER", key, std::forward<Args>(count)...);
        }

        template<typename... Members>
        auto srem(std::string_view key, std::string_view first_member, Members&&... other_members) & noexcept{
            return this->command("SREM", key, first_member, std::forward<Members>(other_members)...);
        }
        template<typename... Members>
        auto srem(std::string_view key, std::string_view first_member, Members&&... other_members) && noexcept{
            return std::move(*this).command("SREM", key, first_member, std::forward<Members>(other_members)...);
        }

        template<typename... Args>
        auto sscan(std::string_view key, int64_t cursor, Args&&... args) & noexcept{
            return this->command("SSCAN", key, details::to_string(cursor), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto sscan(std::string_view key, int64_t cursor, Args&&... args) && noexcept{
            return std::move(*this).command("SSCAN", key, details::to_string(cursor), std::forward<Args>(args)...);
        }

        template<typename... Keys>
        auto sunion(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("SUNION", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto sunion(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("SUNION", first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Keys>
        auto sunionstore(std::string_view destination, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("SUNIONSTORE", destination, first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto sunionstore(std::string_view destination, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("SUNIONSTORE", destination, first_key, std::forward<Keys>(other_keys)...);
        }

        // ---- ZSet ----
        template<typename... Args>
        auto zadd(std::string_view key, Args&&... args) & noexcept{
            return this->command("ZADD", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto zadd(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("ZADD", key, std::forward<Args>(args)...);
        }

        auto zcard(std::string_view key) & noexcept{
            return this->command("ZCARD", key);
        }
        auto zcard(std::string_view key) && noexcept{
            return std::move(*this).command("ZCARD", key);
        }

        auto zcount(std::string_view key, double min, double max) & noexcept{
            return this->command("ZCOUNT", key, details::to_string(min), details::to_string(max));
        }
        auto zcount(std::string_view key, double min, double max) && noexcept{
            return std::move(*this).command("ZCOUNT", key, details::to_string(min), details::to_string(max));
        }

        template<bool ifcluster, typename... Keys, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _zdiff(size_t numkeys, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("ZDIFF", details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }
        template<bool ifcluster, typename... Keys, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _zdiff(size_t numkeys, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("ZDIFF", details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }
        template<bool ifcluster, typename... Keys, typename std::enable_if<ifcluster, bool>::type = true>
        auto _zdiff(size_t numkeys, std::string_view first_key, Keys&&... other_keys) & noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return this->chain(slot, xredis::build_commands(
                std::string_view("ZDIFF"),
                details::to_string(numkeys),
                first_key, std::string_view(std::forward<Keys>(other_keys))...
            ), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, typename... Keys, typename std::enable_if<ifcluster, bool>::type = true>
        auto _zdiff(size_t numkeys, std::string_view first_key, Keys&&... other_keys) && noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return std::move(*this).chain(slot, xredis::build_commands(
                std::string_view("ZDIFF"),
                details::to_string(numkeys),
                first_key, std::string_view(std::forward<Keys>(other_keys))...
            ), std::make_index_sequence<num_ops>());
        }
        template<typename... Keys>
        auto zdiff(size_t numkeys, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->template _zdiff<cluster_mode>(numkeys, first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto zdiff(size_t numkeys, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).template _zdiff<cluster_mode>(numkeys, first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Keys>
        auto zdiffstore(std::string_view destination, size_t numkeys, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("ZDIFFSTORE", destination, details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto zdiffstore(std::string_view destination, size_t numkeys, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("ZDIFFSTORE", destination, details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }

        auto zincrby(std::string_view key, double increment, std::string_view member) & noexcept{
            return this->command("ZINCRBY", key, details::to_string(increment), member);
        }
        auto zincrby(std::string_view key, double increment, std::string_view member) && noexcept{
            return std::move(*this).command("ZINCRBY", key, details::to_string(increment), member);
        }

        template<bool ifcluster, typename... Keys, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _zinter(size_t numkeys, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("ZINTER", details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }
        template<bool ifcluster, typename... Keys, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _zinter(size_t numkeys, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("ZINTER", details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }
        template<bool ifcluster, typename... Keys, typename std::enable_if<ifcluster, bool>::type = true>
        auto _zinter(size_t numkeys, std::string_view first_key, Keys&&... other_keys) & noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return this->chain(slot, xredis::build_commands(
                std::string_view("ZINTER"),
                details::to_string(numkeys),
                first_key, std::string_view(std::forward<Keys>(other_keys))...
            ), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, typename... Keys, typename std::enable_if<ifcluster, bool>::type = true>
        auto _zinter(size_t numkeys, std::string_view first_key, Keys&&... other_keys) && noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return std::move(*this).chain(slot, xredis::build_commands(
                std::string_view("ZINTER"),
                details::to_string(numkeys),
                first_key, std::string_view(std::forward<Keys>(other_keys))...
            ), std::make_index_sequence<num_ops>());
        }
        template<typename... Keys>
        auto zinter(size_t numkeys, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->template _zinter<cluster_mode>(numkeys, first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto zinter(size_t numkeys, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).template _zinter<cluster_mode>(numkeys, first_key, std::forward<Keys>(other_keys)...);
        }


        template<bool ifcluster, typename... Args, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _zintercard(size_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            return this->command("ZINTERCARD", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }
        template<bool ifcluster, typename... Args, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _zintercard(size_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            return std::move(*this).command("ZINTERCARD", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }
        template<bool ifcluster, typename... Args, typename std::enable_if<ifcluster, bool>::type = true>
        auto _zintercard(size_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return this->chain(slot, xredis::build_commands(
                std::string_view("ZINTERCARD"),
                details::to_string(numkeys),
                first_key, std::string_view(std::forward<Args>(args))...
            ), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, typename... Args, typename std::enable_if<ifcluster, bool>::type = true>
        auto _zintercard(size_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return std::move(*this).chain(slot, xredis::build_commands(
                std::string_view("ZINTERCARD"),
                details::to_string(numkeys),
                first_key, std::string_view(std::forward<Args>(args))...
            ), std::make_index_sequence<num_ops>());
        }
        template<typename... Args>
        auto zintercard(size_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            return this->template _zintercard<cluster_mode>(numkeys, first_key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto zintercard(size_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            return std::move(*this).template _zintercard<cluster_mode>(numkeys, first_key, std::forward<Args>(args)...);
        }

        template<bool ifcluster, typename... Args, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _zmpop(size_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            return this->command("ZMPOP", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }
        template<bool ifcluster, typename... Args, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _zmpop(size_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            return std::move(*this).command("ZMPOP", details::to_string(numkeys), first_key, std::forward<Args>(args)...);
        }
        template<bool ifcluster, typename... Args, typename std::enable_if<ifcluster, bool>::type = true>
        auto _zmpop(size_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return this->chain(slot, xredis::build_commands(
                std::string_view("ZMPOP"),
                details::to_string(numkeys),
                first_key, std::string_view(std::forward<Args>(args))...
            ), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, typename... Args, typename std::enable_if<ifcluster, bool>::type = true>
        auto _zmpop(size_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return std::move(*this).chain(slot, xredis::build_commands(
                std::string_view("ZMPOP"),
                details::to_string(numkeys),
                first_key, std::string_view(std::forward<Args>(args))...
            ), std::make_index_sequence<num_ops>());
        }
        template<typename... Args>
        auto zmpop(size_t numkeys, std::string_view first_key, Args&&... args) & noexcept{
            return this->template _zintercard<cluster_mode>(numkeys, first_key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto zmpop(size_t numkeys, std::string_view first_key, Args&&... args) && noexcept{
            return std::move(*this).template _zintercard<cluster_mode>(numkeys, first_key, std::forward<Args>(args)...);
        }

        template<typename... Members>
        auto zmscore(std::string_view key, std::string_view first_member, Members&&... other_members) & noexcept{
            return this->command("ZMSCORE", key, first_member, std::forward<Members>(other_members)...);
        }
        template<typename... Members>
        auto zmscore(std::string_view key, std::string_view first_member, Members&&... other_members) && noexcept{
            return std::move(*this).command("ZMSCORE", key, first_member, std::forward<Members>(other_members)...);
        }

        template<typename... Args>
        auto zrandmember(std::string_view key, Args&&... args) & noexcept{
            return this->command("ZRANDMEMBER", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto zrandmember(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("ZRANDMEMBER", key, std::forward<Args>(args)...);
        }

        template<typename... Options>
        auto zrange(std::string_view key, std::string_view start, std::string_view stop, Options&&... options) & noexcept{
            return this->command("ZRANGE", key, start, stop, std::forward<Options>(options)...);
        }
        template<typename... Options>
        auto zrange(std::string_view key, std::string_view start, std::string_view stop, Options&&... options) && noexcept{
            return std::move(*this).command("ZRANGE", key, start, stop, std::forward<Options>(options)...);
        }

        template<typename... Args>
        auto zrank(std::string_view key, std::string_view member, Args&&... args) & noexcept{
            return this->command("ZRANK", key, member, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto zrank(std::string_view key, std::string_view member, Args&&... args) && noexcept{
            return std::move(*this).command("ZRANK", key, member, std::forward<Args>(args)...);
        }

        template<typename... Members>
        auto zrem(std::string_view key, std::string_view first_member, Members&&... other_members) & noexcept{
            return this->command("ZREM", key, first_member, std::forward<Members>(other_members)...);
        }
        template<typename... Members>
        auto zrem(std::string_view key, std::string_view first_member, Members&&... other_members) && noexcept{
            return std::move(*this).command("ZREM", key, first_member, std::forward<Members>(other_members)...);
        }

        template<typename... Args>
        auto zscan(std::string_view key, size_t cursor, Args&&... args) & noexcept{
            return this->command("ZSCAN", key, details::to_string(cursor), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto zscan(std::string_view key, size_t cursor, Args&&... args) && noexcept{
            return std::move(*this).command("ZSCAN", key, details::to_string(cursor), std::forward<Args>(args)...);
        }

        auto zscore(std::string_view key, std::string_view member) & noexcept{
            return this->command("ZSCORE", key, member);
        }
        auto zscore(std::string_view key, std::string_view member) && noexcept{
            return std::move(*this).command("ZSCORE", key, member);
        }

        template<bool ifcluster, typename... Keys, std::enable_if<!ifcluster, bool>::type = true>
        auto _zunion(size_t numkeys, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("ZUNION", details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }
        template<bool ifcluster, typename... Keys, std::enable_if<!ifcluster, bool>::type = true>
        auto _zunion(size_t numkeys, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("ZUNION", details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }
        template<bool ifcluster, typename... Keys, std::enable_if<ifcluster, bool>::type = true>
        auto _zunion(size_t numkeys, std::string_view first_key, Keys&&... other_keys) & noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return this->chain(slot, xredis::build_commands(
                std::string_view("ZUNION"),
                details::to_string(numkeys),
                first_key, std::string_view(std::forward<Keys>(other_keys))...
            ), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, typename... Keys, std::enable_if<ifcluster, bool>::type = true>
        auto _zunion(size_t numkeys, std::string_view first_key, Keys&&... other_keys) && noexcept{
            uint16_t slot = xredis::get_slot(first_key.data(), first_key.size());
            return std::move(*this).chain(slot, xredis::build_commands(
                std::string_view("ZUNION"),
                details::to_string(numkeys),
                first_key, std::string_view(std::forward<Keys>(other_keys))...
            ), std::make_index_sequence<num_ops>());
        }
        template<typename... Keys>
        auto zunion(size_t numkeys, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->template _zunion<cluster_mode>(numkeys, first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto zunion(size_t numkeys, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).template _zunion<cluster_mode>(numkeys, first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... Keys>
        auto zunionstore(std::string_view destination, size_t numkeys, std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("ZUNIONSTORE", destination, details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto zunionstore(std::string_view destination, size_t numkeys, std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("ZUNIONSTORE", destination, details::to_string(numkeys), first_key, std::forward<Keys>(other_keys)...);
        }

        // ---- HyperLogLog ----

        template<typename... Args>
        auto pfadd(std::string_view key, Args&&... elements) & noexcept{
            return this->command("PFADD", key, std::forward<Args>(elements)...);
        }
        template<typename... Args>
        auto pfadd(std::string_view key, Args&&... elements) && noexcept{
            return std::move(*this).command("PFADD", key, std::forward<Args>(elements)...);
        }

        template<typename... Keys>
        auto pfcount(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("PFCOUNT", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto pfcount(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("PFCOUNT", first_key, std::forward<Keys>(other_keys)...);
        }

        template<typename... SrcKeys>
        auto pfmerge(std::string_view destkey, std::string_view first_srckey, SrcKeys&&... other_srckeys) & noexcept{
            return this->command("PFMERGE", destkey, first_srckey, std::forward<SrcKeys>(other_srckeys)...);
        }
        template<typename... SrcKeys>
        auto pfmerge(std::string_view destkey, std::string_view first_srckey, SrcKeys&&... other_srckeys) && noexcept{
            return std::move(*this).command("PFMERGE", destkey, first_srckey, std::forward<SrcKeys>(other_srckeys)...);
        }

        // ---- Bitmap ----
        auto setbit(std::string_view key, size_t offset, std::string_view value) & noexcept{
            return this->command("SETBIT", key, details::to_string(offset), value);
        }
        auto setbit(std::string_view key, size_t offset, std::string_view value) && noexcept{
            return std::move(*this).command("SETBIT", key, details::to_string(offset), value);
        }

        auto getbit(std::string_view key, size_t offset) & noexcept{
            return this->command("GETBIT", key, details::to_string(offset));
        }
        auto getbit(std::string_view key, size_t offset) && noexcept{
            return std::move(*this).command("GETBIT", key, details::to_string(offset));
        }

        template<typename... Args>
        auto bitcount(std::string_view key, int64_t start, int64_t stop, Args&&... args) & noexcept{
            return this->command("BITCOUNT", key, details::to_string(start), details::to_string(stop), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto bitcount(std::string_view key, int64_t start, int64_t stop, Args&&... args) && noexcept{
            return std::move(*this).command("BITCOUNT", key, details::to_string(start), details::to_string(stop), std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto bitpos(std::string_view key, std::string_view bit, Args&&... args) & noexcept{
            return this->command("BITPOS", key, bit, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto bitpos(std::string_view key, std::string_view bit, Args&&... args) && noexcept{
            return std::move(*this).command("BITPOS", key, bit, std::forward<Args>(args)...);
        }

        template<bool ifcluster, typename... SrcKeys, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _bitop(std::string_view operation, std::string_view destkey, std::string_view first_srckey, SrcKeys&&... other_srckeys) & noexcept{
            return this->command("BITOP", operation, destkey, first_srckey, std::forward<SrcKeys>(other_srckeys)...);
        }
        template<bool ifcluster, typename... SrcKeys, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _bitop(std::string_view operation, std::string_view destkey, std::string_view first_srckey, SrcKeys&&... other_srckeys) && noexcept{
            return std::move(*this).command("BITOP", operation, destkey, first_srckey, std::forward<SrcKeys>(other_srckeys)...);
        }
        template<bool ifcluster, typename... SrcKeys, typename std::enable_if<ifcluster, bool>::type = true>
        auto _bitop(std::string_view operation, std::string_view destkey, std::string_view first_srckey, SrcKeys&&... other_srckeys) & noexcept{
            uint16_t slot = xredis::get_slot(destkey.data(), destkey.size());
            return this->chain(slot, xredis::build_commands(
                std::string_view("BITOP"),
                operation, destkey, first_srckey,
                std::string_view(std::forward<SrcKeys>(other_srckeys))...
            ), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, typename... SrcKeys, typename std::enable_if<ifcluster, bool>::type = true>
        auto _bitop(std::string_view operation, std::string_view destkey, std::string_view first_srckey, SrcKeys&&... other_srckeys) && noexcept{
            uint16_t slot = xredis::get_slot(destkey.data(), destkey.size());
            return std::move(*this).chain(slot, xredis::build_commands(
                std::string_view("BITOP"),
                operation, destkey, first_srckey,
                std::string_view(std::forward<SrcKeys>(other_srckeys))...
            ), std::make_index_sequence<num_ops>());
        }
        template<typename... SrcKeys>
        auto bitop(std::string_view operation, std::string_view destkey, std::string_view first_srckey, SrcKeys&&... other_srckeys) & noexcept{
            return this->template _bitop<cluster_mode>(operation, destkey, first_srckey, std::forward<SrcKeys>(other_srckeys)...);
        }
        template<typename... SrcKeys>
        auto bitop(std::string_view operation, std::string_view destkey, std::string_view first_srckey, SrcKeys&&... other_srckeys) && noexcept{
            return std::move(*this).template _bitop<cluster_mode>(operation, destkey, first_srckey, std::forward<SrcKeys>(other_srckeys)...);
        }

        template<typename... Args>
        auto bitfield(std::string_view key, Args&&... args) & noexcept{
            return this->command("BITFIELD", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto bitfield(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("BITFIELD", key, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto bitfield_ro(std::string_view key, Args&&... args) & noexcept{
            return this->command("BITFIELD_RO", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto bitfield_ro(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("BITFIELD_RO", key, std::forward<Args>(args)...);
        }

        // ---- Stream ----
        template<typename... Args>
        auto xadd(std::string_view key, Args&&... args) & noexcept{
            return this->command("XADD", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xadd(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("XADD", key, std::forward<Args>(args)...);
        }

        template<typename... Ids>
        auto xack(std::string_view key, std::string_view group, std::string_view first_id, Ids&&... other_ids) & noexcept{
            return this->command("XACK", key, group, first_id, std::forward<Ids>(other_ids)...);
        }
        template<typename... Ids>
        auto xack(std::string_view key, std::string_view group, std::string_view first_id, Ids&&... other_ids) && noexcept{
            return std::move(*this).command("XACK", key, group, first_id, std::forward<Ids>(other_ids)...);
        }

        template<typename... Args>
        auto xautoclaim(std::string_view key, std::string_view group, std::string_view consumer, size_t min_idle_time, std::string_view start, Args&&... args) & noexcept{
            return this->command("XAUTOCLAIM", key, group, consumer, details::to_string(min_idle_time), start, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xautoclaim(std::string_view key, std::string_view group, std::string_view consumer, size_t min_idle_time, std::string_view start, Args&&... args) && noexcept{
            return std::move(*this).command("XAUTOCLAIM", key, group, consumer, details::to_string(min_idle_time), start, std::forward<Args>(args)...);
        }

        template<typename... Ids>
        auto xdel(std::string_view key, std::string_view first_id, Ids&&... other_ids) & noexcept{
            return this->command("XDEL", key, first_id, std::forward<Ids>(other_ids)...);
        }
        template<typename... Ids>
        auto xdel(std::string_view key, std::string_view first_id, Ids&&... other_ids) && noexcept{
            return std::move(*this).command("XDEL", key, first_id, std::forward<Ids>(other_ids)...);
        }

        template<typename... Args>
        auto xgroup_create(std::string_view key, std::string_view group, std::string_view id, Args&&... args) & noexcept{
            return this->command("XGROUP", "CREATE", key, group, id, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xgroup_create(std::string_view key, std::string_view group, std::string_view id, Args&&... args) && noexcept{
            return std::move(*this).command("XGROUP", "CREATE", key, group, id, std::forward<Args>(args)...);
        }

        auto xgroup_createconsumer(std::string_view key, std::string_view group, std::string_view consumer) & noexcept{
            return this->command("XGROUP", "CREATECONSUMER", key, group, consumer);
        }
        auto xgroup_createconsumer(std::string_view key, std::string_view group, std::string_view consumer) && noexcept{
            return std::move(*this).command("XGROUP", "CREATECONSUMER", key, group, consumer);
        }

        auto xgroup_delconsumer(std::string_view key, std::string_view group, std::string_view consumer) & noexcept{
            return this->command("XGROUP", "DELCONSUMER", key, group, consumer);
        }
        auto xgroup_delconsumer(std::string_view key, std::string_view group, std::string_view consumer) && noexcept{
            return std::move(*this).command("XGROUP", "DELCONSUMER", key, group, consumer);
        }

        auto xgroup_destroy(std::string_view key, std::string_view group) & noexcept{
            return this->command("XGROUP", "DESTROY", key, group);
        }
        auto xgroup_destroy(std::string_view key, std::string_view group) && noexcept{
            return std::move(*this).command("XGROUP", "DESTROY", key, group);
        }

        template<typename... Args>
        auto xgroup_setid(std::string_view key, std::string_view group, std::string_view id, Args&&... args) & noexcept{
            return this->command("XGROUP", "SETID", key, group, id, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xgroup_setid(std::string_view key, std::string_view group, std::string_view id, Args&&... args) && noexcept{
            return std::move(*this).command("XGROUP", "SETID", key, group, id, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto xinfo_consumers(std::string_view key, std::string_view group) & noexcept{
            return this->command("XINFO", "CONSUMERS", key, group);
        }
        template<typename... Args>
        auto xinfo_consumers(std::string_view key, std::string_view group) && noexcept{
            return std::move(*this).command("XINFO", "CONSUMERS", key, group);
        }

        auto xinfo_groups(std::string_view key) & noexcept{
            return this->command("XINFO", "GROUPS", key);
        }
        auto xinfo_groups(std::string_view key) && noexcept{
            return std::move(*this).command("XINFO", "GROUPS", key);
        }

        template<typename... Args>
        auto xinfo_stream(std::string_view key, Args&&... args) & noexcept{
            return this->command("XINFO", "STREAM", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xinfo_stream(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("XINFO", "STREAM", key, std::forward<Args>(args)...);
        }

        auto xlen(std::string_view key) & noexcept{
            return this->command("XLEN", key);
        }
        auto xlen(std::string_view key) && noexcept{
            return std::move(*this).command("XLEN", key);
        }

        template<typename... Args>
        auto xpending(std::string_view key, std::string_view group, Args&&... args) & noexcept{
            return this->command("XPENDING", key, group, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xpending(std::string_view key, std::string_view group, Args&&... args) && noexcept{
            return std::move(*this).command("XPENDING", key, group, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto xrange(std::string_view key, std::string_view start, std::string_view end, Args&&... args) & noexcept{
            return this->command("XRANGE", key, start, end, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xrange(std::string_view key, std::string_view start, std::string_view end, Args&&... args) && noexcept{
            return std::move(*this).command("XRANGE", key, start, end, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto xreadgroup(std::string_view group_keyword, std::string_view group, std::string_view consumer, Args&&... args) & noexcept{
            return this->command("XREADGROUP", group_keyword, group, consumer, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xreadgroup(std::string_view group_keyword, std::string_view group, std::string_view consumer, Args&&... args) && noexcept{
            return std::move(*this).command("XREADGROUP", group_keyword, group, consumer, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto xtrim(std::string_view key, Args&&... args) & noexcept{
            return this->command("XTRIM", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto xtrim(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("XTRIM", key, std::forward<Args>(args)...);
        }

        // ---- Scripting ----

        template<typename... Args>
        auto eval(std::string_view script, size_t numkeys, Args&&... args) & noexcept{
            return this->command("EVAL", script, details::to_string(numkeys), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto eval(std::string_view script, size_t numkeys, Args&&... args) && noexcept{
            return std::move(*this).command("EVAL", script, details::to_string(numkeys), std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto evalsha(std::string_view sha1, size_t numkeys, Args&&... args) & noexcept{
            return this->command("EVALSHA", sha1, details::to_string(numkeys), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto evalsha(std::string_view sha1, size_t numkeys, Args&&... args) && noexcept{
            return std::move(*this).command("EVALSHA", sha1, details::to_string(numkeys), std::forward<Args>(args)...);
        }

        auto script_load(std::string_view script) & noexcept{
            return this->command("SCRIPT", "LOAD", script);
        }
        auto script_load(std::string_view script) && noexcept{
            return std::move(*this).command("SCRIPT", "LOAD", script);
        }

        // ---- Transaction ----
        template<typename... Keys>
        auto watch(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("WATCH", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto watch(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("WATCH", first_key, std::forward<Keys>(other_keys)...);
        }

        template<bool ifcluster, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _unwatch(std::string_view tag = "") & noexcept{
            return this->command("UNWATCH");
        }
        template<bool ifcluster, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _unwatch(std::string_view tag = "") && noexcept{
            return std::move(*this).command("UNWATCH");
        }
        template<bool ifcluster, typename std::enable_if<ifcluster, bool>::type = true>
        auto _unwatch(std::string_view tag) & noexcept{
            uint16_t slot = xredis::get_slot(tag.data(), tag.size());
            return this->chain(slot, xredis::build_commands(
                std::string_view("UNWATCH")
            ), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, typename std::enable_if<ifcluster, bool>::type = true>
        auto _unwatch(std::string_view tag) && noexcept{
            uint16_t slot = xredis::get_slot(tag.data(), tag.size());
            return std::move(*this).chain(slot, xredis::build_commands(
                std::string_view("UNWATCH")
            ), std::make_index_sequence<num_ops>());
        }
        template<class... Args>
        auto unwatch(Args&&... args) & noexcept{
            return this->template _unwatch<cluster_mode>(std::forward<Args>(args)...);
        }
        template<class... Args>
        auto unwatch(Args&&... args) && noexcept{
            return std::move(*this).template _unwatch<cluster_mode>(std::forward<Args>(args)...);
        }

        template<bool ifcluster, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _exec() & noexcept{
            return this->command("EXEC");
        }
        template<bool ifcluster, typename std::enable_if<!ifcluster, bool>::type = true>
        auto _exec() && noexcept{
            return std::move(*this).command("EXEC");
        }
        template<bool ifcluster, typename std::enable_if<ifcluster, bool>::type = true>
        auto _exec() & noexcept{
            return this->chain(this->pipe.slots[0], xredis::build_commands(
                std::string_view("EXEC")
            ), std::make_index_sequence<num_ops>());
        }
        template<bool ifcluster, typename std::enable_if<ifcluster, bool>::type = true>
        auto _exec() && noexcept{
            return std::move(*this).chain(this->pipe.slots[0], xredis::build_commands(
                std::string_view("EXEC")
            ), std::make_index_sequence<num_ops>());
        }
        auto exec() & noexcept{
            static_assert(!cluster_mode || multi, "exec() should be used with the redis.multi() first");
            return this->template _exec<cluster_mode>();
        }
        auto exec() && noexcept{
            static_assert(!cluster_mode || multi, "exec() should be used with the redis.multi() first");
            return std::move(*this).template _exec<cluster_mode>();
        }

        // ---- Generic ----

        template<typename... Args>
        auto copy(std::string_view source, std::string_view destination, Args&&... args) & noexcept{
            return this->command("COPY", source, destination, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto copy(std::string_view source, std::string_view destination, Args&&... args) && noexcept{
            return std::move(*this).command("COPY", source, destination, std::forward<Args>(args)...);
        }

        template<typename... Keys>
        auto del(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("DEL", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto del(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("DEL", first_key, std::forward<Keys>(other_keys)...);
        }

        auto dump(std::string_view key) & noexcept{
            return this->command("DUMP", key);
        }
        auto dump(std::string_view key) && noexcept{
            return std::move(*this).command("DUMP", key);
        }

        auto exists(std::string_view key) & noexcept{
            return this->command("EXISTS", key);
        }
        auto exists(std::string_view key) && noexcept{
            return std::move(*this).command("EXISTS", key);
        }

        template<typename... Args>
        auto expire(std::string_view key, size_t seconds, Args&&... args) & noexcept{
            return this->command("EXPIRE", key, details::to_string(seconds), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto expire(std::string_view key, size_t seconds, Args&&... args) && noexcept{
            return std::move(*this).command("EXPIRE", key, details::to_string(seconds), std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto expireat(std::string_view key, size_t unix_time_seconds, Args&&... args) & noexcept{
            return this->command("EXPIREAT", key, details::to_string(unix_time_seconds), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto expireat(std::string_view key, size_t unix_time_seconds, Args&&... args) && noexcept{
            return std::move(*this).command("EXPIREAT", key, details::to_string(unix_time_seconds), std::forward<Args>(args)...);
        }

        auto expiretime(std::string_view key) & noexcept{
            return this->command("EXPIRETIME", key);
        }
        auto expiretime(std::string_view key) && noexcept{
            return std::move(*this).command("EXPIRETIME", key);
        }

        auto keys(std::string_view pattern) & noexcept{
            return this->command("KEYS", pattern);
        }
        auto keys(std::string_view pattern) && noexcept{
            return std::move(*this).command("KEYS", pattern);
        }

        auto move(std::string_view key, size_t db) & noexcept{
            return this->command("MOVE", key, details::to_string(db));
        }
        auto move(std::string_view key, size_t db) && noexcept{
            return std::move(*this).command("MOVE", key, details::to_string(db));
        }

        auto persist(std::string_view key) & noexcept{
            return this->command("PERSIST", key);
        }
        auto persist(std::string_view key) && noexcept{
            return std::move(*this).command("PERSIST", key);
        }

        template<typename... Args>
        auto pexpire(std::string_view key, size_t milliseconds, Args&&... args) & noexcept{
            return this->command("PEXPIRE", key, details::to_string(milliseconds), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto pexpire(std::string_view key, size_t milliseconds, Args&&... args) && noexcept{
            return std::move(*this).command("PEXPIRE", key, details::to_string(milliseconds), std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto pexpireat(std::string_view key, size_t unix_time_milliseconds, Args&&... args) & noexcept{
            return this->command("PEXPIREAT", key, details::to_string(unix_time_milliseconds), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto pexpireat(std::string_view key, size_t unix_time_milliseconds, Args&&... args) && noexcept{
            return std::move(*this).command("PEXPIREAT", key, details::to_string(unix_time_milliseconds), std::forward<Args>(args)...);
        }

        auto pexpiretime(std::string_view key) & noexcept{
            return this->command("PEXPIRETIME", key);
        }
        auto pexpiretime(std::string_view key) && noexcept{
            return std::move(*this).command("PEXPIRETIME", key);
        }

        auto pttl(std::string_view key) & noexcept{
            return this->command("PTTL", key);
        }
        auto pttl(std::string_view key) && noexcept{
            return std::move(*this).command("PTTL", key);
        }

        auto randomkey() & noexcept{
            return this->command("RANDOMKEY");
        }
        auto randomkey() && noexcept{
            return std::move(*this).command("RANDOMKEY");
        }

        auto rename(std::string_view key, std::string_view newkey) & noexcept{
            return this->command("RENAME", key, newkey);
        }
        auto rename(std::string_view key, std::string_view newkey) && noexcept{
            return std::move(*this).command("RENAME", key, newkey);
        }

        auto renamenx(std::string_view key, std::string_view newkey) & noexcept{
            return this->command("RENAMENX", key, newkey);
        }
        auto renamenx(std::string_view key, std::string_view newkey) && noexcept{
            return std::move(*this).command("RENAMENX", key, newkey);
        }

        template<typename... Args>
        auto scan(size_t cursor, Args&&... args) & noexcept{
            return this->command("SCAN", details::to_string(cursor), std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto scan(size_t cursor, Args&&... args) && noexcept{
            return std::move(*this).command("SCAN", details::to_string(cursor), std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto sort(std::string_view key, Args&&... args) & noexcept{
            return this->command("SORT", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto sort(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("SORT", key, std::forward<Args>(args)...);
        }

        template<typename... Args>
        auto sort_ro(std::string_view key, Args&&... args) & noexcept{
            return this->command("SORT_RO", key, std::forward<Args>(args)...);
        }
        template<typename... Args>
        auto sort_ro(std::string_view key, Args&&... args) && noexcept{
            return std::move(*this).command("SORT_RO", key, std::forward<Args>(args)...);
        }

        template<typename... Keys>
        auto touch(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("TOUCH", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto touch(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("TOUCH", first_key, std::forward<Keys>(other_keys)...);
        }

        auto ttl(std::string_view key) & noexcept{
            return this->command("TTL", key);
        }
        auto ttl(std::string_view key) && noexcept{
            return std::move(*this).command("TTL", key);
        }

        auto type(std::string_view key) & noexcept{
            return this->command("TYPE", key);
        }
        auto type(std::string_view key) && noexcept{
            return std::move(*this).command("TYPE", key);
        }

        template<typename... Keys>
        auto unlink(std::string_view first_key, Keys&&... other_keys) & noexcept{
            return this->command("UNLINK", first_key, std::forward<Keys>(other_keys)...);
        }
        template<typename... Keys>
        auto unlink(std::string_view first_key, Keys&&... other_keys) && noexcept{
            return std::move(*this).command("UNLINK", first_key, std::forward<Keys>(other_keys)...);
        }
    };
}

#endif