#ifndef redis_connection_h
#define redis_connection_h
#include"xnet.hpp"
#include"RedisValue.h"
#include"RedisParser.h"
#include"mpsc_ring.hpp"
#include<cstdint>
#include<atomic>
#include<string>
#include<sys/eventfd.h>

#ifndef XREDIS_PIPELINE_BATCH_SIZE
#define XREDIS_PIPELINE_BATCH_SIZE 64
#endif

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

    uint32_t iptouint32(const char* ptr, size_t len) noexcept;

    size_t uint32toip(char* buffer, uint32_t ip) noexcept;

    uint16_t get_slot(const char* ptr, size_t len) noexcept;

    uint16_t crc16(const char* ptr, size_t len) noexcept;
    
    xnet::task<xnet::io_result<xredis::RedisValue>> connect_spot(
        xnet::TCPClient& client,
        xredis::RedisParser& parser,
        const ConnectionOption& option, 
        int timeout=10
    ) noexcept;

    xnet::task<xnet::io_result<xredis::RedisValue>> connect_cluster(
        xnet::io_context& ctx,
        const ClusterConnectionOption& option,
        int timeout=10
    ) noexcept;
}

#endif