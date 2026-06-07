#pragma once

// ==========================================
// System Dependencies
// ==========================================
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <string>
#include <string_view>
#include <sys/eventfd.h>
#include <type_traits>
#include <utility>
#include <vector>

#ifdef XREDIS_ENABLE_TLS
#include<openssl/ssl.h>
#include<openssl/err.h>
#include<linux/tls.h>
#endif

// ==========================================
// External Dependencies
// ==========================================
#include "xnet.hpp"

// ==========================================
// MACRO
// ==========================================
#ifndef XREDIS_PARSER_SOCKBUFFER_SIZE
#define XREDIS_PARSER_SOCKBUFFER_SIZE 2048
#endif

#ifndef XREDIS_PARSER_MAXBULK_SIZE
#define XREDIS_PARSER_MAXBULK_SIZE 1024 * 1024 * 512
#endif

#ifndef XREDIS_PARSER_LINEBUFFER_SIZE
#define XREDIS_PARSER_LINEBUFFER_SIZE 511
#endif

#ifndef XREDIS_PIPELINE_BATCH_SIZE
#define XREDIS_PIPELINE_BATCH_SIZE 64
#endif

// ==================================================
// From header: RedisValue.h
// ==================================================
namespace xredis{
    struct RedisClientError{
        static inline std::string_view RING_OVERFLOW = "RING OVERFLOW";
        static inline std::string_view CONN_ERR = "CONN ERR";
        static inline std::string_view OP_CANCELLED = "OP CANCELLED";
        static inline std::string_view CROSS_SLOT = "CROSS SLOT";
    };


    class RedisValue{
    public:
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

        RedisValue() noexcept: null(nullptr), value_type(TYPE::NULLPTR){}
        RedisValue(const RedisValue&) = delete;
        RedisValue& operator=(const RedisValue&) = delete;
        RedisValue(RedisValue&&) noexcept;
        RedisValue& operator=(RedisValue&&) noexcept;
        ~RedisValue() noexcept { this->reset(); }

        // --- type system ---
        template<size_t n>
        struct string_type: public std::string{
            using std::string::string;
        };
        template<size_t n>
        struct vector_type: public std::vector<RedisValue>{
            using std::vector<RedisValue>::vector;
        };

        // --- basic ---
        using null_t = std::nullptr_t;
        using integer_t = int64_t;
        using bool_t    = bool;
        using double_t  = double;

        // --- string ---
        using simple_string_t    = string_type<0>;
        using simple_error_t     = string_type<1>;
        using bulk_string_t      = string_type<2>;
        using big_number_t       = string_type<3>;
        using bulk_error_t       = string_type<4>;
        using verbatim_string_t  = string_type<5>;

        // --- container ---
        using array_t = vector_type<0>;
        using push_t  = vector_type<1>;
        using set_t = vector_type<2>;
        using map_t = std::vector<std::pair<RedisValue, RedisValue>>;
        using attribute_t = std::pair<map_t, RedisValue>;

        template<class T>
        T& get() noexcept;
        template<class T>
        T& as() noexcept { return this->get<T>(); }
        std::string_view as_string() const noexcept;
        std::string_view as_error() const noexcept;
        // for debug (json string)
        std::string dump() const noexcept;

        int type() const noexcept { return this->value_type; }
        bool is_nullptr() const noexcept { return this->value_type == RedisValue::TYPE::NULLPTR; }
        bool is_array() const noexcept { return this->value_type == RedisValue::TYPE::ARRAY; }
        bool is_push() const noexcept { return this->value_type == RedisValue::TYPE::PUSH; }
        bool is_simple_string() const noexcept { return this->value_type == RedisValue::TYPE::SIMPLE_STRING; }
        bool is_simple_error() const noexcept { return this->value_type == RedisValue::TYPE::SIMPLE_ERROR; }
        bool is_bulk_string() const noexcept { return this->value_type == RedisValue::TYPE::BULK_STRING; }
        bool is_big_number() const noexcept { return this->value_type == RedisValue::TYPE::BIG_NUMBER; }
        bool is_bulk_error() const noexcept { return this->value_type == RedisValue::TYPE::BULK_ERROR; }
        bool is_verbatim_string() const noexcept { return this->value_type == RedisValue::TYPE::VERBATIM_STRING; }
        bool is_map() const noexcept { return this->value_type == RedisValue::TYPE::MAP; }
        bool is_set() const noexcept { return this->value_type == RedisValue::TYPE::SET; }
        bool is_integer() const noexcept { return this->value_type == RedisValue::TYPE::INTEGER; }
        bool is_bool() const noexcept { return this->value_type == RedisValue::TYPE::BOOL; }
        bool is_double() const noexcept { return this->value_type == RedisValue::TYPE::DOUBLE; }
        bool is_attribute() const noexcept { return this->value_type == RedisValue::TYPE::ATTRIBUTE; }
        bool is_string_type() const noexcept { return is_simple_string() || is_bulk_string() || is_verbatim_string(); }
        bool is_error() const noexcept { return is_simple_error() || is_bulk_error(); }
        bool error() const noexcept { return is_simple_error() || is_bulk_error(); }

        template<class T, class V>
        void set(V&& value) noexcept{
            this->reset();
            this->_set<T>(std::forward<V>(value));
        }

    private:
        static void dump_recursion(const RedisValue& value, std::string& str) noexcept;
        void reset() noexcept;

        template<class T, class V>
        void _set(V&& value) noexcept{
            constexpr bool match_types = std::is_same_v<T, null_t> || std::is_same_v<T, integer_t>
                                    ||  std::is_same_v<T, bool_t> || std::is_same_v<T, double_t>
                                    ||  std::is_same_v<T, simple_string_t> || std::is_same_v<T, simple_error_t>
                                    ||  std::is_same_v<T, bulk_string_t> ||std::is_same_v<T, big_number_t>
                                    ||  std::is_same_v<T, bulk_error_t> || std::is_same_v<T, verbatim_string_t>
                                    ||  std::is_same_v<T, map_t> || std::is_same_v<T, set_t>
                                    ||  std::is_same_v<T, array_t> || std::is_same_v<T, push_t>
                                    ||  std::is_same_v<T, attribute_t>;
            static_assert(match_types, "RedisValue::set<T>(value): T is not a support type");

            if constexpr(std::is_same_v<T, null_t>){
                this->null = value;
                this->value_type = RedisValue::TYPE::NULLPTR;
                return;
            }
            if constexpr(std::is_same_v<T, integer_t>){
                this->integer = value;
                this->value_type = RedisValue::TYPE::INTEGER;
                return;
            }
            if constexpr(std::is_same_v<T, bool_t>){
                this->boolean = value;
                this->value_type = RedisValue::TYPE::BOOL;
                return;
            }
            if constexpr(std::is_same_v<T, double_t>){
                this->doublev = value;
                this->value_type = RedisValue::TYPE::DOUBLE;
                return;
            }

            if constexpr(std::is_same_v<T, simple_string_t>){
                this->simple_string = new simple_string_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::SIMPLE_STRING;
                return;
            }
            if constexpr(std::is_same_v<T, simple_error_t>){
                this->simple_error = new simple_error_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::SIMPLE_ERROR;
                return;
            }
            if constexpr(std::is_same_v<T, bulk_string_t>){
                this->bulk_string = new bulk_string_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::BULK_STRING;
                return;
            }
            if constexpr(std::is_same_v<T, big_number_t>){
                this->big_number = new big_number_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::BIG_NUMBER;
                return;
            }
            if constexpr(std::is_same_v<T, bulk_error_t>){
                this->bulk_error = new bulk_error_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::BULK_ERROR;
                return;
            }
            if constexpr(std::is_same_v<T, verbatim_string_t>){
                this->verbatim_string = new verbatim_string_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::VERBATIM_STRING;
                return;
            }

            if constexpr(std::is_same_v<T, map_t>){
                this->mapv = new map_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::MAP;
                return;
            }
            if constexpr(std::is_same_v<T, set_t>){
                this->setv = new set_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::SET;
                return;
            }
            if constexpr(std::is_same_v<T, array_t>){
                this->array = new array_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::ARRAY;
                return;
            }
            if constexpr(std::is_same_v<T, push_t>){
                this->push = new push_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::PUSH;
                return;
            }
            if constexpr(std::is_same_v<T, attribute_t>){
                this->attribute = new attribute_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::ATTRIBUTE;
                return;
            }
        }

        union
        {
            null_t null;
            simple_string_t* simple_string;
            simple_error_t* simple_error;
            integer_t integer;
            bulk_string_t* bulk_string;
            array_t* array;
            bool_t boolean;
            double_t doublev;
            big_number_t* big_number;
            bulk_error_t* bulk_error;
            verbatim_string_t* verbatim_string;
            map_t* mapv;
            set_t* setv;
            push_t* push;
            attribute_t* attribute;
        };

        int value_type;
    };
}

// ==================================================
// From header: mpsc_ring.hpp
// ==================================================
namespace xredis{
    namespace details{
        // special MPSCRing for redis only as the consumer will only consume the ring after writing
        template<bool shared, size_t xcapacity>
        class MPSCRing{
            struct shared_ring{
                alignas(64) std::atomic_size_t head;
                std::atomic_size_t head_committed;
                alignas(64) std::atomic_size_t tail_allocated;
                std::atomic_size_t tail;
                alignas(64) std::coroutine_handle<> handles[xcapacity];
                alignas(64) std::string_view payloads[xcapacity];
            };

            struct noshared_ring{
                size_t head;
                size_t head_committed;
                size_t tail;
                std::coroutine_handle<> handles[xcapacity];
                std::string_view payloads[xcapacity];
            };

            using atomic_type = std::conditional_t<shared, std::atomic<size_t>, size_t>;
            using ring_type = std::conditional_t<shared, shared_ring, noshared_ring>;
            static constexpr size_t mask = xcapacity - 1;
            static constexpr size_t STOP_BIT = (1ULL << 63);
            
            ring_type ring;

            static consteval bool is_power_of_two(size_t x) noexcept {
                return x && ((x & (x - 1)) == 0);
            }

            template<bool s, typename std::enable_if<s, bool>::type = true>
            int _push_n(std::coroutine_handle<>* handles, std::string_view* payloads, size_t count) noexcept{
                while(true){
                    size_t t_alloc = this->ring.tail_allocated.load(std::memory_order_relaxed);
                    size_t head = this->ring.head.load(std::memory_order_acquire);

                    if(t_alloc & STOP_BIT){
                        return -2;
                    }
                    if(t_alloc - head >= xcapacity){
                        return -1;
                    }
                    if(!this->ring.tail_allocated.compare_exchange_weak(t_alloc, t_alloc + count, std::memory_order_relaxed)){
                        continue;
                    }
                    size_t pos = t_alloc & mask;
                    for(size_t i = 0; i < count; i++){
                        size_t idx = (pos + i) & mask;
                        this->ring.handles[idx] = handles[i];
                        this->ring.payloads[idx] = payloads[i];
                    }
                    while (this->ring.tail.load(std::memory_order_acquire) != t_alloc) {
                        XNET_CPU_RELAX();
                    }
                    this->ring.tail.store(t_alloc + count, std::memory_order_release);
                    return static_cast<int>(pos);
                }
            }
            template<bool s, typename std::enable_if<!s, bool>::type = true>
            int _push_n(std::coroutine_handle<>* handles, std::string_view* payloads, size_t count) noexcept{
                if(this->ring.tail & STOP_BIT){
                    return -2;
                }
                if(this->ring.tail - this->ring.head >= xcapacity){
                    return -1;
                }
                size_t pos = this->ring.tail & mask;
                this->ring.tail += count;
                for(size_t i = 0; i < count; i++){
                    size_t idx = (pos + i) & mask;
                    this->ring.handles[idx] = handles[i];
                    this->ring.payloads[idx] = payloads[i];
                }
                return static_cast<int>(pos);
            }

            template<bool s, typename std::enable_if<s, bool>::type = true>
            int _push(std::coroutine_handle<> h, std::string_view sv) noexcept{
                while(true){
                    size_t t_alloc = this->ring.tail_allocated.load(std::memory_order_relaxed);
                    size_t head = this->ring.head.load(std::memory_order_acquire);

                    if(t_alloc & STOP_BIT){
                        return -2;
                    }
                    if(t_alloc - head >= xcapacity){
                        return -1;
                    }
                    if(!this->ring.tail_allocated.compare_exchange_weak(t_alloc, t_alloc + 1, std::memory_order_relaxed)){
                        continue;
                    }
                    size_t pos = t_alloc & mask;
                    this->ring.handles[pos] = h;
                    this->ring.payloads[pos] = sv;
                    while (this->ring.tail.load(std::memory_order_acquire) != t_alloc) {
                        XNET_CPU_RELAX();
                    }
                    this->ring.tail.store(t_alloc + 1, std::memory_order_release);
                    return static_cast<int>(pos);
                }
            }
            template<bool s, typename std::enable_if<s, bool>::type = true>
            void _pop(std::coroutine_handle<>& h, std::string_view& sv) noexcept{
                size_t h_val = this->ring.head.load(std::memory_order_relaxed);
                size_t pos = h_val & mask;
                h = this->ring.handles[pos];
                sv = this->ring.payloads[pos];
                this->ring.head.store(h_val + 1, std::memory_order_release);
            }

            template<bool s, typename std::enable_if<!s, bool>::type = true>
            int _push(std::coroutine_handle<> h, std::string_view sv) noexcept{
                if(this->ring.tail & STOP_BIT){
                    return -2;
                }
                if(this->ring.tail - this->ring.head >= xcapacity){
                    return -1;
                }
                size_t pos = this->ring.tail & mask;
                ++this->ring.tail;
                this->ring.handles[pos] = h;
                this->ring.payloads[pos] = sv;
                return static_cast<int>(pos);
            }
            template<bool s, typename std::enable_if<!s, bool>::type = true>
            void _pop(std::coroutine_handle<>& h, std::string_view& sv) noexcept{
                size_t pos = this->ring.head & mask;            
                ++this->ring.head;
                h = this->ring.handles[pos];
                sv = this->ring.payloads[pos];
            }

            template<class T, bool s, typename std::enable_if<s, bool>::type = true>
            size_t _prep_writev(T* iov, size_t& num_evs) noexcept{
                size_t start = this->ring.head_committed.load(std::memory_order_acquire);
                size_t end = this->ring.tail.load(std::memory_order_acquire);
                num_evs = (end - start) < num_evs ? (end - start) : num_evs;
                this->ring.head_committed.fetch_add(num_evs, std::memory_order_release);
                size_t total_bytes = 0;
                for(size_t i = 0; i < num_evs; i++){
                    std::string_view& payload = this->ring.payloads[(start + i) & mask];
                    iov[i].iov_base = (void*)(payload.data());
                    iov[i].iov_len = payload.size();
                    total_bytes += payload.size();
                }
                return total_bytes;
            }

            template<class T, bool s, typename std::enable_if<!s, bool>::type = true>
            size_t _prep_writev(T* iov, size_t& num_evs) noexcept{
                size_t start = this->ring.head_committed;
                size_t end = this->ring.tail;
                num_evs = (end - start) < num_evs ? (end - start) : num_evs;
                this->ring.head_committed += num_evs;
                size_t total_bytes = 0;
                for(size_t i = 0; i < num_evs; i++){
                    std::string_view& payload = this->ring.payloads[(start + i) & mask];
                    iov[i].iov_base = (void*)(payload.data());
                    iov[i].iov_len = payload.size();
                    total_bytes += payload.size();
                }
                return total_bytes;
            }

            template<bool s, typename std::enable_if<s, bool>::type = true>
            void _drain(xredis::RedisValue& result_slot) noexcept{
                size_t old = this->ring.tail_allocated.fetch_or(STOP_BIT, std::memory_order_acq_rel);
                while(this->ring.tail.load() != old){
                    XNET_CPU_RELAX();
                }
                size_t head = this->ring.head.load(std::memory_order_relaxed);
                for(size_t i = head; i != old; i++){
                    xredis::RedisValue error;
                    error.set<xredis::RedisValue::simple_error_t>(std::string_view("CONN ERR"));
                    result_slot = std::move(error);
                    this->ring.handles[i & mask].resume();
                }
            }
            template<bool s, typename std::enable_if<!s, bool>::type = true>
            void _drain(xredis::RedisValue& result_slot) noexcept{
                size_t tail = this->ring.tail;
                this->ring.tail |= STOP_BIT;
                for(size_t i = this->ring.head; i != tail; i++){
                    xredis::RedisValue error;
                    error.set<xredis::RedisValue::simple_error_t>(std::string_view("CONN ERR"));
                    result_slot = std::move(error);
                    this->ring.handles[i & mask].resume();
                }
            }

            template<bool s, typename std::enable_if<s, bool>::type = true>
            void _reset() noexcept{
                this->ring.head.store(0, std::memory_order_relaxed);
                this->ring.head_committed.store(0, std::memory_order_relaxed);
                this->ring.tail.store(0, std::memory_order_relaxed);
                // last
                this->ring.tail_allocated.store(0, std::memory_order_release);
            }
            template<bool s, typename std::enable_if<!s, bool>::type = true>
            void _reset() noexcept{
                this->ring.head = 0;
                this->ring.head_committed = 0;
                this->ring.tail = 0;
            }

        public:
            static constexpr size_t capacity = xcapacity; 

            MPSCRing() noexcept: ring(){
                static_assert(MPSCRing::is_power_of_two(capacity), "MPSCRing::MPSCRing() the capacity must be the power of two");
            }

            ~MPSCRing() = default;

            int push(std::coroutine_handle<> h, std::string_view sv) noexcept{
                return this->_push<shared>(h, sv);
            }
            void pop(std::coroutine_handle<>& h, std::string_view& sv) noexcept{
                return this->_pop<shared>(h, sv);
            }

            int push_n(std::coroutine_handle<>* handles, std::string_view* payloads, size_t count) noexcept{
                return this->_push_n<shared>(handles, payloads, count);
            }

            void drain(xredis::RedisValue& result_slot) noexcept{
                return this->_drain<shared>(result_slot);
            }

            void reset() noexcept{
                return this->_reset<shared>();
            }

            template<class T>
            size_t prep_writev(T* iov, size_t& size) noexcept{
                return this->_prep_writev<T, shared>(iov, size);
            }
        };
    }
}

// ==================================================
// From header: RedisParser.h
// ==================================================
namespace xredis{
    class RedisParser{
    public:
        static constexpr size_t sockbuffer_size = XREDIS_PARSER_SOCKBUFFER_SIZE;
        static constexpr size_t max_bulk_size = XREDIS_PARSER_MAXBULK_SIZE;

        RedisParser(xnet::v4TCPClient& stream) 
        noexcept: linebuffer(), pos(0), sockbuffer(new char[sockbuffer_size]), stream(stream), timed(10)
        { this->linebuffer.reserve(XREDIS_PARSER_LINEBUFFER_SIZE); }

        RedisParser(RedisParser&&) = default;

        ~RedisParser(){
            delete[] sockbuffer;
        }
        using result_t = std::pair<std::string, int>;


        xnet::task<xnet::io_result<xredis::RedisValue>> parse(int timeout = -1) noexcept;

        xnet::io_result<xredis::RedisValue> try_parse() noexcept;

        xnet::io_result<size_t> try_parse_n(RedisValue* values, int* errs, size_t n) noexcept;

        void consume() noexcept;
        void reset() noexcept;

    private:
        xnet::task<xnet::io_result<std::string_view>> read_line(char& ch) noexcept;

        xnet::task<xnet::io_result<std::string_view>> read_bulk(size_t bytes) noexcept;

        xnet::task<int> parse_recursion(xredis::RedisValue& result) noexcept;
        
        xnet::io_result<std::string_view> try_read_line(char& ch) noexcept;

        xnet::io_result<std::string_view> try_read_bulk(size_t bytes) noexcept;

        int try_parse_recursion(xredis::RedisValue& result) noexcept;

        std::string linebuffer;
        size_t pos;
        char* sockbuffer;
        xnet::v4TCPClient& stream;
        int timed;
    };
}

// ==================================================
// From header: RedisConnection.h
// ==================================================
namespace xredis{
    #ifdef XREDIS_ENABLE_TLS
        struct TLSOption{
            const char* cacert = nullptr;
            const char* cacertdir = nullptr;
            
            const char* cert = nullptr;
            const char* key = nullptr;
            const char* sni = nullptr;
        };
    #endif

    struct ConnectionOption{
        const char* ip = "127.0.0.1";
        const char* username = nullptr;
        const char* password = nullptr;
        const char* clientname = nullptr;
        int db = 0;
        int resp = 3;
        uint16_t port = 6379;
    #ifdef XREDIS_ENABLE_TLS
        TLSOption tls;
    #endif
    };

    struct ClusterConnectionOption{
        using IP = const char*;
        IP* seeds_ip = nullptr;
        const uint16_t* ports = nullptr;
        size_t num_seeds = 0;
        const char* username = nullptr;
        const char* password = nullptr;
        const char* clientname = nullptr;
        int resp = 3;
    #ifdef XREDIS_ENABLE_TLS
        TLSOption tls;
    #endif 
    };

    struct XLongLastOption{
        std::string ip;
        std::string username;
        std::string password;
        std::string clientname;
        int db;
        int resp;
        uint16_t port;
        #ifdef XREDIS_ENABLE_TLS
            std::string cacert;
            std::string cacertdir;
            
            std::string cert;
            std::string key;
            std::string sni;
        #endif

        XLongLastOption() = default;
        XLongLastOption(const XLongLastOption&) = default;
        XLongLastOption(XLongLastOption&&) = default;
        XLongLastOption(const ConnectionOption& option) noexcept{
            this->set_option(option);
        }

        void set_option(const ConnectionOption& option) noexcept{
            this->ip = option.ip ? option.ip : "";
            this->username = option.username ? option.username : "";
            this->password = option.password ? option.password : "";
            this->clientname = option.clientname ? option.clientname : "";
            this->db = option.db;
            this->resp = option.resp;
            this->port = option.port;
        #ifdef XREDIS_ENABLE_TLS
            this->cacert = option.tls.cacert ? option.tls.cacert : "";
            this->cacertdir = option.tls.cacertdir ? option.tls.cacertdir : "";
            this->cert = option.tls.cert ? option.tls.cert : "";
            this->key = option.tls.key ? option.tls.key : "";
            this->sni = option.tls.sni ? option.tls.sni : "";
        #endif
        }

        operator xredis::ConnectionOption() const noexcept{
            struct xredis::ConnectionOption option;
            option.ip = ip.empty() ? nullptr : ip.c_str();
            option.username = username.empty() ? nullptr : username.c_str();
            option.password = password.empty() ? nullptr : password.c_str();
            option.clientname = clientname.empty() ? nullptr : clientname.c_str();
            option.db = this->db;
            option.resp = this->resp;
            option.port = this->port;
        #ifdef XREDIS_ENABLE_TLS
            option.tls.cacert = cacert.empty() ? nullptr : cacert.c_str();
            option.tls.cacertdir = cacertdir.empty() ? nullptr : cacertdir.c_str();
            option.tls.cert = cert.empty() ? nullptr : cert.c_str();
            option.tls.key = key.empty() ? nullptr : key.c_str();
            option.tls.sni = sni.empty() ? nullptr : sni.c_str();
        #endif
            return option;
        }
    };

    struct ClusterLongLastOption{
        std::string username;
        std::string password;
        std::string clientname;
        int resp;
        #ifdef XREDIS_ENABLE_TLS
            std::string cacert;
            std::string cacertdir;
            
            std::string cert;
            std::string key;
            std::string sni;
        #endif

        ClusterLongLastOption() = default;
        ClusterLongLastOption(const ClusterLongLastOption&) = default;
        ClusterLongLastOption(ClusterLongLastOption&&) = default;
        ClusterLongLastOption(const ClusterConnectionOption& option) noexcept{
            this->set_option(option);
        }

        void set_option(const ClusterConnectionOption& option) noexcept{
            this->username = option.username ? option.username : "";
            this->password = option.password ? option.password : "";
            this->clientname = option.clientname ? option.clientname : "";
            this->resp = option.resp;
        #ifdef XREDIS_ENABLE_TLS
            this->cacert = option.tls.cacert ? option.tls.cacert : "";
            this->cacertdir = option.tls.cacertdir ? option.tls.cacertdir : "";
            this->cert = option.tls.cert ? option.tls.cert : "";
            this->key = option.tls.key ? option.tls.key : "";
            this->sni = option.tls.sni ? option.tls.sni : "";
        #endif
        }

        operator xredis::ClusterConnectionOption() const noexcept{
            struct xredis::ClusterConnectionOption option;
            option.username = username.empty() ? nullptr : username.c_str();
            option.password = password.empty() ? nullptr : password.c_str();
            option.clientname = clientname.empty() ? nullptr : clientname.c_str();
            option.resp = this->resp;
        #ifdef XREDIS_ENABLE_TLS
            option.tls.cacert = cacert.empty() ? nullptr : cacert.c_str();
            option.tls.cacertdir = cacertdir.empty() ? nullptr : cacertdir.c_str();
            option.tls.cert = cert.empty() ? nullptr : cert.c_str();
            option.tls.key = key.empty() ? nullptr : key.c_str();
            option.tls.sni = sni.empty() ? nullptr : sni.c_str();
        #endif
            return option;
        }
    };

    template<bool shared, size_t xcapacity>
    struct Pipe{
        using Event = xnet::AsyncStream<-1, -1, false>;
        using Ring = xredis::details::MPSCRing<shared, xcapacity>;

        Ring ring;
        xnet::v4TCPClient client;
        Event event;
        xredis::RedisParser parser;
        xredis::XLongLastOption option;
    #ifndef XNET_DISABLE_THREAD_SAFE
        std::atomic<bool> alive;
        std::atomic<int> ref;

        template<bool s>
        void set_alive(bool v) noexcept{
            this->alive.store(v, std::memory_order_release);
        }
        template<bool s>
        bool get_alive() noexcept{
            return this->alive.load(std::memory_order_acquire);
        }

        template<bool s>
        int add_ref(int x, std::memory_order order = std::memory_order_acq_rel) noexcept{
            return this->ref.fetch_add(x, order) + x;
        }
        template<bool s>
        int dec_ref(int x, std::memory_order order = std::memory_order_acq_rel) noexcept{
            return this->ref.fetch_sub(x, order) - x;
        }
    #else
        std::conditional_t<shared, std::atomic<bool>, bool> alive;
        std::conditional_t<shared, std::atomic<int>, int> ref;

        template<bool s, typename std::enable_if<s, bool>::type = true>
        void set_alive(bool v) noexcept{
            this->alive.store(v, std::memory_order_release);
        }
        template<bool s, typename std::enable_if<!s, bool>::type = true>
        void set_alive(bool v) noexcept{
            this->alive = v;
        }
        template<bool s,  typename std::enable_if<s, bool>::type = true>
        int add_ref(int x, std::memory_order order = std::memory_order_acq_rel) noexcept{
            return this->ref.fetch_add(x, order) + x;
        }
        template<bool s, typename std::enable_if<s, bool>::type = true>
        int dec_ref(int x, std::memory_order order = std::memory_order_acq_rel) noexcept{
            return this->ref.fetch_sub(x, order) - x;
        }

        template<bool s, typename std::enable_if<s, bool>::type = true>
        bool get_alive() noexcept{
            return this->alive.load(std::memory_order_acquire);
        }
        template<bool s, typename std::enable_if<!s, bool>::type = true>
        bool get_alive() noexcept{
            return this->alive;
        }

        template<bool s, typename std::enable_if<!s, bool>::type = true>
        int add_ref(int x, std::memory_order order = std::memory_order_acq_rel) noexcept{
            return this->ref += x;
        }
        template<bool s, typename std::enable_if<!s, bool>::type = true>
        int dec_ref(int x, std::memory_order order = std::memory_order_acq_rel) noexcept{
            return this->ref -= x;
        }
    #endif

        Pipe(xnet::io_context& ctx)
        noexcept: ring(), client(ctx), event(ctx, ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)), 
        parser(client), option(), alive(false)
        {}

        ~Pipe() = default;
    };

    inline uint32_t iptouint32(const char* ptr, size_t len) noexcept;

    inline size_t uint32toip(char* buffer, uint32_t ip) noexcept;

    inline uint16_t get_slot(const char* ptr, size_t len) noexcept;

    inline uint16_t crc16(const char* ptr, size_t len) noexcept;
    
    inline xnet::task<xnet::io_result<xredis::RedisValue>> connect_spot(
        xnet::TCPClient& client,
        xredis::RedisParser& parser,
        const ConnectionOption& option, 
        int timeout=10
    ) noexcept;

    inline xnet::task<xnet::io_result<xredis::RedisValue>> connect_cluster(
        xnet::io_context& ctx,
        const ClusterConnectionOption& option,
        int timeout=10
    ) noexcept;
}

// ==================================================
// From header: RedisCommander.hpp
// ==================================================
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

// ==================================================
// From header: RedisClient.hpp
// ==================================================
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

// ==================================================
// From header: RedisCluster.hpp
// ==================================================
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

// ==================================================
// From header: RedisSubscriber.hpp
// ==================================================
namespace xredis{
    class RedisSubscriber{
        xnet::v4TCPClient client;
        xredis::RedisParser parser;
    public:
        RedisSubscriber(xnet::io_context& ctx) noexcept: client(ctx), parser(client)
        {}
        RedisSubscriber(RedisSubscriber&& other) = default;
        RedisSubscriber& operator=(RedisSubscriber&& other) = delete;

        ~RedisSubscriber() = default;

        auto connect(const xredis::ConnectionOption& option, int timeout=10) noexcept{
            return xredis::connect_spot(this->client, this->parser, option, timeout);
        }

        template<class... Args>
        xnet::ptask<xnet::io_result<bool>> subscribe(std::string_view channel, Args&&... channels) noexcept{
            std::string data = xredis::build_commands(
                std::string_view("SUBSCRIBE"),
                channel, std::string_view(std::forward<Args>(channels))...
            );
            size_t sent = 0;
            while(sent < data.size()){
                auto res = co_await client.send(data.data() + sent, data.size() - sent, 0);
                if(res.err){
                    co_return {false, res.err};
                }
                sent += *res;
            }
            co_return {true, 0};
        }
        template<std::input_iterator InputIt>
        xnet::ptask<xnet::io_result<bool>> subscribe(InputIt begin, size_t count) noexcept{
            std::string data = xredis::build_commands_from_range(
                std::string_view("SUBSCRIBE"),
                begin, count
            );
            size_t sent = 0;
            while(sent < data.size()){
                auto res = co_await client.send(data.data() + sent, data.size() - sent, 0);
                if(res.err){
                    co_return {false, res.err};
                }
                sent += *res;
            }
            co_return {true, 0};
        }

        template<class... Args>
        xnet::ptask<xnet::io_result<bool>> unsubscribe(std::string_view channel, Args&&... channels) noexcept{
            std::string data = xredis::build_commands(
                std::string_view("UNSUBSCRIBE"),
                channel, std::string_view(std::forward<Args>(channels))...
            );
            size_t sent = 0;
            while(sent < data.size()){
                auto res = co_await client.send(data.data() + sent, data.size() - sent, 0);
                if(res.err){
                    co_return {false, res.err};
                }
                sent += *res;
            }
            co_return {true, 0};
        }
        template<std::input_iterator InputIt>
        xnet::ptask<xnet::io_result<bool>> unsubscribe(InputIt begin, size_t count) noexcept{
            std::string data = xredis::build_commands_from_range(
                std::string_view("UNSUBSCRIBE"),
                begin, count
            );
            size_t sent = 0;
            while(sent < data.size()){
                auto res = co_await client.send(data.data() + sent, data.size() - sent, 0);
                if(res.err){
                    co_return {false, res.err};
                }
                sent += *res;
            }
            co_return {true, 0};
        }

        template<class... Args>
        xnet::ptask<xnet::io_result<bool>> psubscribe(std::string_view pattern, Args&&... patterns) noexcept{
            std::string data = xredis::build_commands(
                std::string_view("PSUBSCRIBE"),
                pattern, std::string_view(std::forward<Args>(patterns))...
            );
            size_t sent = 0;
            while(sent < data.size()){
                auto res = co_await client.send(data.data() + sent, data.size() - sent, 0);
                if(res.err){
                    co_return {false, res.err};
                }
                sent += *res;
            }
            co_return {true, 0};
        }
        template<std::input_iterator InputIt>
        xnet::ptask<xnet::io_result<bool>> psubscribe(InputIt begin, size_t count) noexcept{
            std::string data = xredis::build_commands_from_range(
                std::string_view("PSUBSCRIBE"),
                begin, count
            );
            size_t sent = 0;
            while(sent < data.size()){
                auto res = co_await client.send(data.data() + sent, data.size() - sent, 0);
                if(res.err){
                    co_return {false, res.err};
                }
                sent += *res;
            }
            co_return {true, 0};
        }

        template<class... Args>
        xnet::ptask<xnet::io_result<bool>> punsubscribe(std::string_view pattern, Args&&... patterns) noexcept{
            std::string data = xredis::build_commands(
                std::string_view("PUNSUBSCRIBE"),
                pattern, std::string_view(std::forward<Args>(patterns))...
            );
            size_t sent = 0;
            while(sent < data.size()){
                auto res = co_await client.send(data.data() + sent, data.size() - sent, 0);
                if(res.err){
                    co_return {false, res.err};
                }
                sent += *res;
            }
            co_return {true, 0};
        }
        template<std::input_iterator InputIt>
        xnet::ptask<xnet::io_result<bool>> punsubscribe(InputIt begin, size_t count) noexcept{
            std::string data = xredis::build_commands_from_range(
                std::string_view("PUNSUBSCRIBE"),
                begin, count
            );
            size_t sent = 0;
            while(sent < data.size()){
                auto res = co_await client.send(data.data() + sent, data.size() - sent, 0);
                if(res.err){
                    co_return {false, res.err};
                }
                sent += *res;
            }
            co_return {true, 0};
        }

        auto read_one_packet(int timed = -1) noexcept{
            return this->parser.parse(timed);
        }

        void close() noexcept{
            this->parser.reset();
            this->client.close();
        }
    };
}

// ==========================================
// Implementations
// ==========================================

// ==================================================
// From source implementation: RedisValue.cpp
// ==================================================

inline void xredis::RedisValue::reset() noexcept{
    switch(this->value_type)
    {
    case xredis::RedisValue::TYPE::ARRAY         : { delete array; break; }
    case xredis::RedisValue::TYPE::PUSH          : { delete push; break; }
    case xredis::RedisValue::TYPE::MAP           : { delete mapv; break; }
    case xredis::RedisValue::TYPE::SET           : { delete setv; break; }

    case xredis::RedisValue::TYPE::SIMPLE_STRING : { delete simple_string; break; }
    case xredis::RedisValue::TYPE::SIMPLE_ERROR  : { delete simple_error; break; }
    case xredis::RedisValue::TYPE::BULK_STRING   : { delete bulk_string; break; }
    case xredis::RedisValue::TYPE::BIG_NUMBER    : { delete big_number; break; }
    case xredis::RedisValue::TYPE::BULK_ERROR    : { delete bulk_error; break; }
    case xredis::RedisValue::TYPE::VERBATIM_STRING : { delete verbatim_string; break; }

    case xredis::RedisValue::TYPE::ATTRIBUTE     : { delete attribute; break;}

    default:{ break; }
    }
}

inline std::string_view xredis::RedisValue::as_string() const noexcept{
    switch (this->value_type) {
    case xredis::RedisValue::TYPE::SIMPLE_STRING:   return *simple_string;
    case xredis::RedisValue::TYPE::BULK_STRING:     return *bulk_string;
    case xredis::RedisValue::TYPE::VERBATIM_STRING: return *verbatim_string;
    default: return {};
    }
}

inline std::string_view xredis::RedisValue::as_error() const noexcept{
    switch (this->value_type){
    case xredis::RedisValue::SIMPLE_ERROR: return *simple_error;
    case xredis::RedisValue::BULK_ERROR: return *bulk_error;
    default: return {};
    }
}

inline static void escape_string(std::string& out, const std::string_view in) noexcept {
    out.reserve(out.size() + in.size() + 8);

    for(unsigned char ch : in){
        switch(ch){
        case '\"': out.append("\\\"", 2); break;
        case '\\': out.append("\\\\", 2); break;
        case '\b': out.append("\\b", 2);  break;
        case '\f': out.append("\\f", 2);  break;
        case '\n': out.append("\\n", 2);  break;
        case '\r': out.append("\\r", 2);  break;
        case '\t': out.append("\\t", 2);  break;
        default:{
            if(ch < 0x20){
                static constexpr char hex[] = "0123456789abcdef";
                out.append("\\u00", 4);
                out.push_back(hex[ch >> 4]);
                out.push_back(hex[ch & 0xF]);
            }
            else{
                out.push_back(ch);
            }
        }
        }
    }
}

inline void xredis::RedisValue::dump_recursion(const xredis::RedisValue& value, std::string& str) noexcept{
    switch(value.value_type){
    case xredis::RedisValue::TYPE::NULLPTR:{
        str.append("null", 4);
        break;
    }
    case xredis::RedisValue::TYPE::INTEGER: {
        str += std::to_string(value.integer);
        break;
    }
    case xredis::RedisValue::TYPE::DOUBLE: {
        str += std::to_string(value.doublev);
        break;
    }
    case xredis::RedisValue::TYPE::BOOL: {
        std::string_view boolean = value.boolean ? "true" : "false";
        str += boolean;
        break;
    }
    case xredis::RedisValue::TYPE::SIMPLE_STRING:{
        str.push_back('\"');
        escape_string(str, *value.simple_string);
        str.push_back('\"');
        break;
    }
    case xredis::RedisValue::TYPE::SIMPLE_ERROR:{
        str.push_back('\"');
        escape_string(str, *value.simple_error);
        str.push_back('\"');
        break;
    }
    case xredis::RedisValue::TYPE::BULK_STRING:{
        str.push_back('\"');
        escape_string(str, *value.bulk_string);
        str.push_back('\"');
        break;
    }
    case xredis::RedisValue::TYPE::BIG_NUMBER:{
        str.push_back('\"');
        escape_string(str, *value.big_number);
        str.push_back('\"');
        break;
    }
    case xredis::RedisValue::TYPE::BULK_ERROR:{
        str.push_back('\"');
        escape_string(str, *value.bulk_error);
        str.push_back('\"');
        break;
    }
    case xredis::RedisValue::TYPE::VERBATIM_STRING:{
        str.push_back('\"');
        escape_string(str, *value.verbatim_string);
        str.push_back('\"');
        break;
    }
    case xredis::RedisValue::TYPE::ARRAY:{
        if(value.array->empty()){
            str.append("[]", 2);
        }
        else{
            str.push_back('[');
            auto it = value.array->cbegin();
            for(; it != value.array->cend() - 1; ++it){
                dump_recursion(*it, str);
                str.push_back(',');
            }
            dump_recursion(*it, str);
            str.push_back(']');
        }
        break;
    }
    case xredis::RedisValue::TYPE::PUSH:{
        constexpr std::string_view push_str = "{\"type\":\"push\",\"data\":[";
        str += push_str;
        if(value.push->empty()){
            str.append("]}", 2);
        }
        else{
            auto it = value.push->cbegin();
            for(; it != value.push->cend() - 1; ++it){
                dump_recursion(*it, str);
                str.push_back(',');
            }
            dump_recursion(*it, str);
            str.append("]}", 2);
        }
        break;
    }
    case xredis::RedisValue::TYPE::SET:{
        constexpr std::string_view set_str = "{\"type\":\"set\",\"data\":[";
        str += set_str;
        if(value.setv->empty()){
            str.append("]}", 2);
        }
        else{
            auto it = value.setv->cbegin();
            for(; it != value.setv->cend() - 1; ++it){
                dump_recursion(*it, str);
                str.push_back(',');
            }
            dump_recursion(*it, str);
            str.append("]}", 2);
        }
        break;
    }
    case xredis::RedisValue::TYPE::MAP:{
        constexpr std::string_view map_str = "{\"type\":\"map\",\"data\":[";
        str += map_str;
        if(value.mapv->empty()){
            str.append("]}", 2);
        }
        else{
            auto it = value.mapv->cbegin();
            for(; it != value.mapv->cend() - 1; ++it){
                str.push_back('[');
                dump_recursion(it->first, str); // key
                str.push_back(',');
                dump_recursion(it->second, str); // value
                str.append("],", 2);
            }
            str.push_back('[');
            dump_recursion(it->first, str); // key
            str.push_back(',');
            dump_recursion(it->second, str); // value
            str.append("]]}", 3);
        }
        break;
    }
    case xredis::RedisValue::TYPE::ATTRIBUTE:{
        constexpr std::string_view attribute_str = "{\"type\":\"attribute\",\"attributes\":[";
        str += attribute_str;
        const xredis::RedisValue::map_t& attbs = value.attribute->first;
        const xredis::RedisValue& actual_data = value.attribute->second;
        if(attbs.empty()){
            str.append("],\"data\":", 9);
        }
        else{
            auto it = attbs.cbegin();
            for(; it != attbs.cend() - 1; ++it){
                str.push_back('[');
                dump_recursion(it->first, str); // key
                str.push_back(',');
                dump_recursion(it->second, str); // value
                str.append("],", 2);
            }
            str.push_back('[');
            dump_recursion(it->first, str); // key
            str.push_back(',');
            dump_recursion(it->second, str); // value
            str.append("]],\"data\":", 10);
        }
        dump_recursion(actual_data, str);
        str.push_back('}');
        break;
    }
    }
}

inline std::string xredis::RedisValue::dump() const noexcept{
    std::string str;
    dump_recursion(*this, str);
    return str;
}


inline xredis::RedisValue::RedisValue(xredis::RedisValue&& other) noexcept{
    std::memcpy(static_cast<void*>(this), static_cast<const void*>(&other), sizeof(xredis::RedisValue));
    other.value_type = TYPE::NULLPTR;
}
inline xredis::RedisValue& xredis::RedisValue::operator=(xredis::RedisValue&& other) noexcept{
    this->reset();
    std::memcpy(static_cast<void*>(this), static_cast<const void*>(&other), sizeof(xredis::RedisValue));
    other.value_type = xredis::RedisValue::TYPE::NULLPTR;
    return *this;
}

// get && set
template<>
inline xredis::RedisValue::null_t& xredis::RedisValue::get<xredis::RedisValue::null_t>() noexcept {
    return null;
}
template<>
inline xredis::RedisValue::simple_string_t& xredis::RedisValue::get<xredis::RedisValue::simple_string_t>() noexcept {
    return *simple_string;
}
template<>
inline xredis::RedisValue::simple_error_t& xredis::RedisValue::get<xredis::RedisValue::simple_error_t>() noexcept {
    return *simple_error;
}
template<>
inline xredis::RedisValue::integer_t& xredis::RedisValue::get<xredis::RedisValue::integer_t>() noexcept {
    return integer;
}
template<>
inline xredis::RedisValue::bulk_string_t& xredis::RedisValue::get<xredis::RedisValue::bulk_string_t>() noexcept {
    return *bulk_string;
}
template<>
inline xredis::RedisValue::array_t& xredis::RedisValue::get<xredis::RedisValue::array_t>() noexcept {
    return *array;
}
template<>
inline xredis::RedisValue::bool_t& xredis::RedisValue::get<xredis::RedisValue::bool_t>() noexcept {
    return boolean;
}
template<>
inline xredis::RedisValue::double_t& xredis::RedisValue::get<xredis::RedisValue::double_t>() noexcept {
    return doublev;
}
template<>
inline xredis::RedisValue::big_number_t& xredis::RedisValue::get<xredis::RedisValue::big_number_t>() noexcept {
    return *big_number;
}
template<>
inline xredis::RedisValue::bulk_error_t& xredis::RedisValue::get<xredis::RedisValue::bulk_error_t>() noexcept {
    return *bulk_error;
}
template<>
inline xredis::RedisValue::verbatim_string_t& xredis::RedisValue::get<xredis::RedisValue::verbatim_string_t>() noexcept {
    return *verbatim_string;
}
template<>
inline xredis::RedisValue::map_t& xredis::RedisValue::get<xredis::RedisValue::map_t>() noexcept {
    return *mapv;
}
template<>
inline xredis::RedisValue::set_t& xredis::RedisValue::get<xredis::RedisValue::set_t>() noexcept {
    return *setv;
}
template<>
inline xredis::RedisValue::push_t& xredis::RedisValue::get<xredis::RedisValue::push_t>() noexcept {
    return *push;
}
template<>
inline xredis::RedisValue::attribute_t& xredis::RedisValue::get<xredis::RedisValue::attribute_t>() noexcept{
    return *attribute;
}

// ==================================================
// From source implementation: RedisParser.cpp
// ==================================================

inline xnet::task<xnet::io_result<std::string_view>> xredis::RedisParser::read_line(char& ch) noexcept{
    size_t fd = this->pos;
    while(true){
        fd = this->linebuffer.find('\n', fd);
        if(fd != std::string::npos){
            if(fd == this->pos || this->linebuffer[fd - 1] != '\r'){
                co_return {"", -1};
            }
            ch = this->linebuffer[this->pos];
            std::string_view data(this->linebuffer.data() + this->pos + 1, fd - (this->pos + 2));
            this->pos = fd + 1;
            co_return {data, 0};
        }
        fd = this->linebuffer.size();
        xnet::io_result<size_t> result;
        if(this->timed <= 0){
            result = co_await this->stream.recv(this->sockbuffer, sockbuffer_size, 0);
        }
        else{
            result = co_await this->stream.recv(this->sockbuffer, sockbuffer_size, 0).timeout(this->timed);
        }
        if(result.err || *result == 0){
            co_return {"", result.err ? result.err : ECONNRESET};
        }
        if(this->linebuffer.size() + *result >= max_bulk_size){
            co_return {"", EOVERFLOW};
        }
        this->linebuffer.append(this->sockbuffer, *result);
    }
}

inline xnet::task<xnet::io_result<std::string_view>> xredis::RedisParser::read_bulk(size_t bytes) noexcept{
    if(bytes >= max_bulk_size){
        co_return {"", EOVERFLOW};
    }
    this->linebuffer.reserve(bytes + 2 + this->pos);
    while(true){
        if(this->linebuffer.size() - this->pos >= bytes + 2){
            const char* base = this->linebuffer.data() + this->pos;
            if(base[bytes] != '\r' || base[bytes + 1] != '\n'){
                co_return {"", -2};
            }
            std::string_view data(base, bytes);
            this->pos += bytes + 2;
            co_return {data, 0};
        }
        xnet::io_result<size_t> result;
        if(this->timed <= 0){
            result = co_await this->stream.recv(this->sockbuffer, sockbuffer_size, 0);
        }
        else{
            result = co_await this->stream.recv(this->sockbuffer, sockbuffer_size, 0).timeout(this->timed);
        }
        if(result.err || *result == 0){
            co_return {"", result.err ? result.err : ECONNRESET};
        }
        this->linebuffer.append(this->sockbuffer, *result);
    }
}

inline void xredis::RedisParser::consume() noexcept{
    this->linebuffer.erase(0, this->pos);
    this->pos = 0;
}

inline void xredis::RedisParser::reset() noexcept{
    this->linebuffer.clear();
    this->pos = 0;
}

inline xnet::task<xnet::io_result<xredis::RedisValue>> xredis::RedisParser::parse(int timeout) noexcept{
    xredis::RedisValue result;
    this->timed = timeout;
    int err = co_await parse_recursion(result);
    if(err == 0){
        this->consume();
    }
    co_return {std::move(result), err};
}

inline static int stoll_view(const std::string_view& sv, int64_t& result) noexcept{
    if(sv.empty()){
        return EINVAL;
    }

    int64_t integer;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), integer);
    if(ec == std::errc() && ptr == sv.data() + sv.size()){
        result = integer;
        return 0;
    }
    return ec == std::errc::result_out_of_range ? ERANGE : EINVAL;
}

inline static int stod_view(const std::string_view& sv, double& result) noexcept{
    if(sv.empty()){
        return EINVAL;
    }
    double doublev;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), doublev);
    if(ec == std::errc() && ptr == sv.data() + sv.size()){
        result = doublev;
        return 0;
    }
    return ec == std::errc::result_out_of_range ? ERANGE : EINVAL;
}

inline xnet::task<int> xredis::RedisParser::parse_recursion(xredis::RedisValue& result) noexcept{
    char prefix = '\0';
    auto line = co_await this->read_line(prefix);
    if(line.err){
        co_return line.err;
    }

    switch(prefix){
    // --------------------------
    // --- RESP 2---
    case '+':{
        // simple string
        result.set<xredis::RedisValue::simple_string_t>(line.move());
        co_return 0;
    }
    case '-':{
        // simple error
        result.set<xredis::RedisValue::simple_error_t>(line.move());
        co_return 0;
    }
    case ':':{
        // integer
        int64_t integer;
        int err = stoll_view(*line, integer);
        if(!err){
            result.set<xredis::RedisValue::integer_t>(integer);
        }
        co_return err;
    }
    case '$':{
        // bulk string
        int64_t length;
        int err = stoll_view(*line, length);
        if(err || length < 0){
            co_return err ? err : (length == -1 ? 0 : EINVAL);
        }
        auto bulk = co_await this->read_bulk(static_cast<size_t>(length));
        if(bulk.err){
            co_return bulk.err;
        }
        result.set<xredis::RedisValue::bulk_string_t>(bulk.move());
        co_return 0;
    }
    case '*':{
        // Array
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            co_return err ? err : EINVAL;
        }
        xredis::RedisValue::array_t vec;
        vec.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            vec.emplace_back();
            err = co_await parse_recursion(vec[vec.size() - 1]);
            if(err){
                co_return err;
            }
        }
        result.set<xredis::RedisValue::array_t>(std::move(vec));
        co_return 0;
    }
    // --------------------------
    // --- RESP 3---
    case '_':{
        // Null
        result.set<xredis::RedisValue::null_t>(nullptr);
        co_return 0;
    }
    case '#':{
        // Boolean
        char tof;
        if(line->size() == 1){
            tof = (*line)[0];
            if(tof == 't' || tof == 'f'){
                result.set<xredis::RedisValue::bool_t>(tof == 't');
                co_return 0;
            }
        }
        co_return EINVAL;
    }
    case ',':{
        // Double
        double doublev;
        int err = stod_view(*line, doublev);
        if(!err){
            result.set<xredis::RedisValue::double_t>(doublev);
        }
        co_return err;
    }
    case '(':{
        // Big number
        result.set<xredis::RedisValue::big_number_t>(line.move());
        co_return 0;
    }
    case '!':{
        // Bulk error
        int64_t length;
        int err = stoll_view(*line, length);
        if(err || length < 0){
            co_return err ? err : EINVAL;
        }
        auto bulk = co_await this->read_bulk(static_cast<size_t>(length));
        if(bulk.err){
            co_return bulk.err;
        }
        result.set<xredis::RedisValue::bulk_error_t>(bulk.move());
        co_return 0;
    }
    case '=':{
        // Verbatim string
        int64_t length;
        int err = stoll_view(*line, length);
        if(err || length < 0){
            co_return err ? err : EINVAL;
        }
        auto bulk = co_await this->read_bulk(static_cast<size_t>(length));
        if(bulk.err){
            co_return bulk.err;
        }
        result.set<xredis::RedisValue::verbatim_string_t>(bulk.move());
        co_return 0;
    }
    case '%':{
        // Map
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            co_return err ? err : EINVAL;
        }
        xredis::RedisValue::map_t map;
        map.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            xredis::RedisValue key;
            xredis::RedisValue value;
            err = co_await parse_recursion(key);
            if(err){ co_return err; }
            err = co_await parse_recursion(value);
            if(err){ co_return err; }
            map.emplace_back(std::move(key), std::move(value));
        }
        result.set<xredis::RedisValue::map_t>(std::move(map));
        co_return 0;
    }
    case '|':{
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            co_return err ? err : EINVAL;
        }
        xredis::RedisValue::map_t metadata;
        metadata.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            xredis::RedisValue key;
            xredis::RedisValue value;
            err = co_await parse_recursion(key);
            if(err){ co_return err; }
            err = co_await parse_recursion(value);
            if(err){ co_return err; }
            metadata.emplace_back(std::move(key), std::move(value));
        }

        xredis::RedisValue value;
        err = co_await parse_recursion(value);
        if(err){ co_return err; }

        result.set<xredis::RedisValue::attribute_t>(
            xredis::RedisValue::attribute_t(std::move(metadata), std::move(value))
        );
        co_return 0;
    }
    case '~':{
        // Set
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            co_return err ? err : EINVAL;
        }
        xredis::RedisValue::set_t set;
        set.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            set.emplace_back();
            err = co_await parse_recursion(set[set.size() - 1]);
            if(err){
                co_return err;
            }
        }
        result.set<xredis::RedisValue::set_t>(std::move(set));
        co_return 0;
    }
    case '>':{
        // Push
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            co_return err ? err : EINVAL;
        }
        xredis::RedisValue::push_t push;
        push.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            push.emplace_back();
            err = co_await parse_recursion(push[push.size() - 1]);
            if(err){
                co_return err;
            }
        }
        result.set<xredis::RedisValue::push_t>(std::move(push));
        co_return 0;
    }
    default:{
        break;
    }
    }
    co_return -3;
}

inline xnet::io_result<std::string_view> xredis::RedisParser::try_read_line(char& ch) noexcept{
    size_t fd = this->pos;
    while(true){
        fd = this->linebuffer.find('\n', fd);
        if(fd != std::string::npos){
            if(fd == this->pos || this->linebuffer[fd - 1] != '\r'){
                return {"", -1};
            }
            ch = this->linebuffer[this->pos];
            std::string_view data(this->linebuffer.data() + this->pos + 1, fd - (this->pos + 2));
            this->pos = fd + 1;
            return {data, 0};
        }
        fd = this->linebuffer.size();
        ssize_t ret = ::recv(this->stream.fd(), this->sockbuffer, sockbuffer_size, 0);
        int err = (ret < 0 ? errno : (ret == 0 ? ECONNRESET : 0));
        if(err){
            return {"", err};
        }
        if(this->linebuffer.size() + ret >= max_bulk_size){
            return {"", EOVERFLOW};
        }
        this->linebuffer.append(this->sockbuffer, static_cast<size_t>(ret));
    }
}

inline xnet::io_result<std::string_view> xredis::RedisParser::try_read_bulk(size_t bytes) noexcept{
    if(bytes >= max_bulk_size){
        return {"", EOVERFLOW};
    }
    this->linebuffer.reserve(bytes + 2 + this->pos);
    while(true){
        if(this->linebuffer.size() - this->pos >= bytes + 2){
            const char* base = this->linebuffer.data() + this->pos;
            if(base[bytes] != '\r' || base[bytes + 1] != '\n'){
                return {"", -2};
            }
            std::string_view data(base, bytes);
            this->pos += bytes + 2;
            return {data, 0};
        }
        ssize_t ret = ::recv(this->stream.fd(), this->sockbuffer, sockbuffer_size, 0);
        int err = (ret < 0 ? errno : (ret == 0 ? ECONNRESET : 0));
        if(err){
            return {"", err};
        }
        this->linebuffer.append(this->sockbuffer, static_cast<size_t>(ret));
    }
}

inline xnet::io_result<size_t> xredis::RedisParser::try_parse_n(xredis::RedisValue* values, int* errs, size_t n) noexcept{
    for(size_t i = 0; i < n; i++){
        int pivot = this->pos;
        errs[i] = try_parse_recursion(values[i]);
        if(errs[i] == EAGAIN){
            this->linebuffer.erase(0, pivot);
            this->pos = 0;
            return {i, 0};
        }
        else if(errs[i]){
            this->pos = 0;
            return {i, errs[i]};
        }
    }
    this->linebuffer.erase(0, this->pos);
    this->pos = 0;
    return {n, 0};
}

inline xnet::io_result<xredis::RedisValue> xredis::RedisParser::try_parse() noexcept{
    xredis::RedisValue result;
    int err = try_parse_recursion(result);
    if(err == 0){
        this->consume();
    }
    this->pos = 0;
    return {std::move(result), err};
}

inline int xredis::RedisParser::try_parse_recursion(xredis::RedisValue& result) noexcept{
    char prefix = '\0';
    auto line = this->try_read_line(prefix);
    if(line.err){
        return line.err;
    }

    switch(prefix){
    // --------------------------
    // --- RESP 2---
    case '+':{
        // simple string
        result.set<xredis::RedisValue::simple_string_t>(line.move());
        return 0;
    }
    case '-':{
        // simple error
        result.set<xredis::RedisValue::simple_error_t>(line.move());
        return 0;
    }
    case ':':{
        // integer
        int64_t integer;
        int err = stoll_view(*line, integer);
        if(!err){
            result.set<xredis::RedisValue::integer_t>(integer);
        }
        return err;
    }
    case '$':{
        // bulk string
        int64_t length;
        int err = stoll_view(*line, length);
        if(err || length < 0){
            return err ? err : (length == -1 ? 0 : EINVAL);
        }
        auto bulk = this->try_read_bulk(static_cast<size_t>(length));
        if(bulk.err){
            return bulk.err;
        }
        result.set<xredis::RedisValue::bulk_string_t>(bulk.move());
        return 0;
    }
    case '*':{
        // Array
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            return err ? err : EINVAL;
        }
        xredis::RedisValue::array_t vec;
        vec.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            vec.emplace_back();
            err = this->try_parse_recursion(vec[vec.size() - 1]);
            if(err){
                return err;
            }
        }
        result.set<xredis::RedisValue::array_t>(std::move(vec));
        return 0;
    }
    // --------------------------
    // --- RESP 3---
    case '_':{
        // Null
        result.set<xredis::RedisValue::null_t>(nullptr);
        return 0;
    }
    case '#':{
        // Boolean
        char tof;
        if(line->size() == 1){
            tof = (*line)[0];
            if(tof == 't' || tof == 'f'){
                result.set<xredis::RedisValue::bool_t>(tof == 't');
                return 0;
            }
        }
        return EINVAL;
    }
    case ',':{
        // Double
        double doublev;
        int err = stod_view(*line, doublev);
        if(!err){
            result.set<xredis::RedisValue::double_t>(doublev);
        }
        return err;
    }
    case '(':{
        // Big number
        result.set<xredis::RedisValue::big_number_t>(line.move());
        return 0;
    }
    case '!':{
        // Bulk error
        int64_t length;
        int err = stoll_view(*line, length);
        if(err || length < 0){
            return err ? err : EINVAL;
        }
        auto bulk = this->try_read_bulk(static_cast<size_t>(length));
        if(bulk.err){
            return bulk.err;
        }
        result.set<xredis::RedisValue::bulk_error_t>(bulk.move());
        return 0;
    }
    case '=':{
        // Verbatim string
        int64_t length;
        int err = stoll_view(*line, length);
        if(err || length < 0){
            return err ? err : EINVAL;
        }
        auto bulk = this->try_read_bulk(static_cast<size_t>(length));
        if(bulk.err){
            return bulk.err;
        }
        result.set<xredis::RedisValue::verbatim_string_t>(bulk.move());
        return 0;
    }
    case '%':{
        // Map
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            return err ? err : EINVAL;
        }
        xredis::RedisValue::map_t map;
        map.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            xredis::RedisValue key;
            xredis::RedisValue value;
            err = this->try_parse_recursion(key);
            if(err){ return err; }
            err = this->try_parse_recursion(value);
            if(err){ return err; }
            map.emplace_back(std::move(key), std::move(value));
        }
        result.set<xredis::RedisValue::map_t>(std::move(map));
        return 0;
    }
    case '|':{
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            return err ? err : EINVAL;
        }
        xredis::RedisValue::map_t metadata;
        metadata.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            xredis::RedisValue key;
            xredis::RedisValue value;
            err = this->try_parse_recursion(key);
            if(err){ return err; }
            err = this->try_parse_recursion(value);
            if(err){ return err; }
            metadata.emplace_back(std::move(key), std::move(value));
        }

        xredis::RedisValue value;
        err = this->try_parse_recursion(value);
        if(err){ return err; }

        result.set<xredis::RedisValue::attribute_t>(
            xredis::RedisValue::attribute_t(std::move(metadata), std::move(value))
        );
        return 0;
    }
    case '~':{
        // Set
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            return err ? err : EINVAL;
        }
        xredis::RedisValue::set_t set;
        set.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            set.emplace_back();
            err = this->try_parse_recursion(set[set.size() - 1]);
            if(err){
                return err;
            }
        }
        result.set<xredis::RedisValue::set_t>(std::move(set));
        return 0;
    }
    case '>':{
        // Push
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            return err ? err : EINVAL;
        }
        xredis::RedisValue::push_t push;
        push.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            push.emplace_back();
            err = this->try_parse_recursion(push[push.size() - 1]);
            if(err){
                return err;
            }
        }
        result.set<xredis::RedisValue::push_t>(std::move(push));
        return 0;
    }
    default:{
        break;
    }
    }
    return -3;
}
// ==================================================
// From source implementation: RedisConnection.cpp
// ==================================================

#ifdef XREDIS_ENABLE_TLS
    inline static SSL_CTX* create_ssl_ctx(const xredis::TLSOption& opt) noexcept{
        SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
        if(!ctx) return nullptr;

        SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
        SSL_CTX_set_max_proto_version(ctx, TLS1_2_VERSION);
        if(opt.cacert || opt.cacertdir){
            if(!SSL_CTX_load_verify_locations(ctx, opt.cacert, opt.cacertdir)){
                SSL_CTX_free(ctx);
                return nullptr;
            }
        }
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
        if(opt.cert && opt.key){
            if (!SSL_CTX_use_certificate_file(ctx, opt.cert, SSL_FILETYPE_PEM) ||
                !SSL_CTX_use_PrivateKey_file(ctx, opt.key, SSL_FILETYPE_PEM) ||
                !SSL_CTX_check_private_key(ctx)) {
                SSL_CTX_free(ctx);
                return nullptr;
            }
        }
        return ctx;
    }

    inline static xnet::task<int> ssl_handshake(
        xnet::TCPClient& client, 
        const xredis::TLSOption& opt, 
        int timed
    ) noexcept{
        SSL_CTX* ctx = create_ssl_ctx(opt);
        int err = EPROTO;
        if(ctx){
            SSL* ssl = SSL_new(ctx);
            if(ssl){
                if(!SSL_set_fd(ssl, client.fd())){
                    SSL_free(ssl);
                    SSL_CTX_free(ctx);
                    co_return EIO;
                }
                SSL_set_connect_state(ssl);
                if(opt.sni && !SSL_set_tlsext_host_name(ssl, opt.sni)){
                    SSL_free(ssl);
                    SSL_CTX_free(ctx);
                    co_return EINVAL;
                }

                SSL_set_options(ssl, SSL_OP_ENABLE_KTLS);
                while(true){
                    int ret = SSL_do_handshake(ssl);
                    if(ret == 1){
                        err = (BIO_get_ktls_send(SSL_get_wbio(ssl)) 
                                && BIO_get_ktls_recv(SSL_get_rbio(ssl))) ? 0 : EPROTONOSUPPORT;
                        break;
                    }
                    {
                        err = SSL_get_error(ssl, ret);
                        if(err == SSL_ERROR_WANT_READ){
                            auto result = co_await client.readable().timeout(timed);
                            if(result.err == 0){
                                continue;
                            }
                            err = result.err;
                        }
                        else if(err == SSL_ERROR_WANT_WRITE){
                            auto result = co_await client.writable().timeout(timed);
                            if(result.err == 0){
                                continue;
                            }
                            err = result.err;
                        }
                        break;
                    }
                }
                SSL_free(ssl);
            }
            SSL_CTX_free(ctx);
        }
        co_return err;
    }
#endif

inline xnet::task<xnet::io_result<xredis::RedisValue>> xredis::connect_spot(
    xnet::TCPClient& client,
    xredis::RedisParser& parser,
    const xredis::ConnectionOption& option, 
    int timeout
) noexcept{
    std::string data;
    timeout = timeout > 0 ? timeout : 10;

    {
        // HELLO COMMAND
        std::string_view hello = "HELLO";
        std::string_view resp = option.resp == 3 ? "3" : "2";
        std::string_view auth = "AUTH";
        std::string_view setname = "SETNAME";
        if(option.password && option.clientname){
            if(option.username){
                data = xredis::build_commands(
                    hello, resp, 
                    auth, std::string_view(option.username), std::string_view(option.password),
                    setname, std::string_view(option.clientname)    
                );
            }
            else{
                data = xredis::build_commands(
                    hello, resp,
                    auth, std::string_view("default"), std::string_view(option.password),
                    setname, std::string_view(option.clientname)
                );
            }
        }
        else if(option.password && !option.clientname){
            data = xredis::build_commands(
                hello, resp,
                auth, std::string_view(option.password)
            );
        }
        else if(!option.password && option.clientname){
            data = xredis::build_commands(
                hello, resp,
                setname, std::string_view(option.clientname)
            );
        }
        else{
            data = xredis::build_commands(hello, resp);
        }
        data += xredis::build_commands(std::string_view("SELECT"), std::to_string(option.db >= 0 ? option.db : 0));
    }

    {
        // TCP connect
        auto addr = xnet::v4addr(option.ip, option.port);
        auto success = co_await client.connect(&addr, sizeof(addr)).timeout(timeout);
        if(!success){
            client.close();
            co_return {xredis::RedisValue(), success.err};
        }
    }

#ifdef XREDIS_ENABLE_TLS
    {
        // TLS 1.2 support
        if(option.tls.cacert){
            int err = co_await ssl_handshake(client, option.tls, timeout);
            if(err){
                client.close();
                co_return {xredis::RedisValue(), err};
            }
        }
    }
#endif

    {
        int err = 0;
        const char* ptr = data.c_str();
        size_t bytes = data.size();
        while(true){
            auto w = co_await client.send(ptr, bytes, 0).timeout(timeout);
            if(w.err || *w == 0){
                err = w.err ? w.err : ECONNRESET;
                break;
            }
            if(bytes == *w) break;
            bytes -= *w;
            ptr += *w;
        }
        if(err){
            client.close();
            co_return {xredis::RedisValue(), err};
        }
    }

    // HELLO
    auto response = co_await parser.parse(timeout);
    if(response.err || !(response->is_map() || response->is_array())){
        client.close();
        co_return {xredis::RedisValue(), EINVAL};
    }

    {
        // SELECT
        auto response = co_await parser.parse(timeout);
        if(response.err || !response->is_simple_string() || (response->as_string() != "OK")){
            client.close();
            co_return {xredis::RedisValue(), EINVAL};
        }
    }

    co_return {response.move(), 0};
}

inline xnet::task<xnet::io_result<xredis::RedisValue>> xredis::connect_cluster(
    xnet::io_context& ctx,
    const xredis::ClusterConnectionOption& option,
    int timeout
) noexcept{
    std::string data;
    timeout = timeout > 0 ? timeout : 10;
    {
        // HELLO COMMAND
        std::string_view hello = "HELLO";
        std::string_view resp = option.resp == 3 ? "3" : "2";
        std::string_view auth = "AUTH";
        std::string_view setname = "SETNAME";
        if(option.password && option.clientname){
            if(option.username){
                data = xredis::build_commands(
                    hello, resp, 
                    auth, std::string_view(option.username), std::string_view(option.password),
                    setname, std::string_view(option.clientname)    
                );
            }
            else{
                data = xredis::build_commands(
                    hello, resp,
                    auth, std::string_view("default"), std::string_view(option.password),
                    setname, std::string_view(option.clientname)
                );
            }
        }
        else if(option.password && !option.clientname){
            data = xredis::build_commands(
                hello, resp,
                auth, std::string_view(option.password)
            );
        }
        else if(!option.password && option.clientname){
            data = xredis::build_commands(
                hello, resp,
                setname, std::string_view(option.clientname)
            );
        }
        else{
            data = xredis::build_commands(hello, resp);
        }
        data += xredis::build_commands(std::string_view("CLUSTER"), std::string_view("SLOTS"));
    }

    xnet::v4TCPClient client(ctx);
    xredis::RedisValue ret;
    int err = EINVAL;
    for(size_t i = 0; i < option.num_seeds; i++)
    {
        {
            const char* ip = option.seeds_ip[i];
            uint16_t port = option.ports[i];
            auto addr = xnet::v4addr(ip, port);
            auto res = co_await client.connect(&addr, sizeof(addr)).timeout(timeout);
            if(res.err){
                client.close();
                err = res.err;
                continue;
            }
        }
    #ifdef XREDIS_ENABLE_TLS
        {
            // TLS 1.2 support
            if(option.tls.cacert){
                err = co_await ssl_handshake(client, option.tls, timeout);
                if(err){
                    client.close();
                    break;
                }
            }
        }
    #endif
        {
            int send_err = 0;
            const char* ptr = data.c_str();
            size_t bytes = data.size();
            while(true){
                auto w = co_await client.send(ptr, bytes, 0).timeout(timeout);
                if(w.err || *w == 0){
                    send_err = w.err ? w.err : ECONNRESET;
                    break;
                }
                if(bytes == *w) break;
                bytes -= *w;
                ptr += *w;
            }
            if(send_err){
                client.close();
                err = send_err;
                continue;
            }
        }
        {
            xredis::RedisParser parser(client);
            {
                // HELLO
                auto response = co_await parser.parse(timeout);
                if(response.err || !(response->is_map() || response->is_array())){
                    client.close();
                    err = EINVAL;
                    continue;
                }
            }

            {
                // CLUSTER SLOTS
                auto response = co_await parser.parse(timeout);
                if(response.err || !response->is_array()){
                    client.close();
                    err = EINVAL;
                    continue;
                }
                err = 0;
                ret = response.move();
                break;
            }
        }
    }
    co_return {std::move(ret), err};
}

inline uint32_t xredis::iptouint32(const char* ptr, size_t len) noexcept{
    uint32_t result = 0;
    const char* end = ptr + len;

    for(int i = 3; i >= 0; --i){
        uint8_t octet = 0;
        auto [p, err] = std::from_chars(ptr, end, octet);
        if(err != std::errc{}){
            return 0;
        }
        result |= (static_cast<uint32_t>(octet) << (i * 8));
        if(i > 0 && *p != '.'){
            return 0;
        }
        ptr = p + 1;
    }
    return ptr == (end + 1) ? result : 0;
}

inline size_t xredis::uint32toip(char* buffer, uint32_t ip) noexcept{
    const uint8_t octets[4] = {
        static_cast<uint8_t>((ip >> 24) & 0xFF),
        static_cast<uint8_t>((ip >> 16) & 0xFF),
        static_cast<uint8_t>((ip >> 8) & 0xFF),
        static_cast<uint8_t>(ip & 0xFF)
    };

    char* ptr = buffer;
    for(int i = 0; i < 4; i++){
        auto [p, err] = std::to_chars(ptr, buffer + 16, octets[i]);
        ptr = p;
        if(i < 3){
            *ptr++ = '.';
        }
    }
    *ptr = '\0';
    return ptr - buffer - 1;
}

inline uint16_t xredis::get_slot(const char* ptr, size_t len) noexcept{
    std::string_view sv(ptr, len);
    size_t left = sv.find_first_of('{');
    if(left != sv.npos){
        size_t right = sv.find_first_of('}', left + 1);
        if(right != sv.npos){
            if(right - left - 1 > 0){
                return crc16(ptr + left + 1, right - left - 1) % 16384;
            }
        }
    }
    return crc16(ptr, len) % 16384;
}
namespace xredis{
    static inline constexpr uint16_t XMODEMCRC16LOOKUP[] = {
        0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
        0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
        0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
        0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
        0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
        0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
        0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
        0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
        0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
        0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
        0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
        0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
        0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
        0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
        0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
        0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
        0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
        0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
        0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
        0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
        0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
        0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
        0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
        0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
        0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
        0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
        0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
        0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
        0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
        0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
        0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
        0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
    };
}

inline uint16_t xredis::crc16(const char* data, size_t len) noexcept{
    uint16_t crc = 0x0000;
    for (size_t i = 0; i < len; ++i) {
        crc = (crc << 8) ^ XMODEMCRC16LOOKUP[((crc >> 8) ^ (uint8_t)data[i]) & 0xff];
    }
    return crc;
}