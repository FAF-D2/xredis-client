#include"../include/RedisConnection.h"
#include"../include/RedisCommander.hpp"
#include<string_view>

#ifdef XREDIS_ENABLE_TLS
#include<openssl/ssl.h>
#include<openssl/err.h>
#include<linux/tls.h>
#endif

using xredis::ConnectionOption;
using xredis::RedisValue;

#ifdef XREDIS_ENABLE_TLS
    static SSL_CTX* create_ssl_ctx(const xredis::TLSOption& opt) noexcept{
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

    static xnet::task<int> ssl_handshake(
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

xnet::task<xnet::io_result<xredis::RedisValue>> xredis::connect_spot(
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
            co_return {RedisValue(), success.err};
        }
    }

#ifdef XREDIS_ENABLE_TLS
    {
        // TLS 1.2 support
        if(option.tls.cacert){
            int err = co_await ssl_handshake(client, option.tls, timeout);
            if(err){
                client.close();
                co_return {RedisValue(), err};
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
            co_return {RedisValue(), err};
        }
    }

    // HELLO
    auto response = co_await parser.parse(timeout);
    if(response.err || !(response->is_map() || response->is_array())){
        client.close();
        co_return {RedisValue(), EINVAL};
    }

    {
        // SELECT
        auto response = co_await parser.parse(timeout);
        if(response.err || !response->is_simple_string() || (response->as_string() != "OK")){
            client.close();
            co_return {RedisValue(), EINVAL};
        }
    }

    co_return {response.move(), 0};
}

xnet::task<xnet::io_result<xredis::RedisValue>> xredis::connect_cluster(
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
    RedisValue ret;
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

uint32_t xredis::iptouint32(const char* ptr, size_t len) noexcept{
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

size_t xredis::uint32toip(char* buffer, uint32_t ip) noexcept{
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

uint16_t xredis::get_slot(const char* ptr, size_t len) noexcept{
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

static constexpr uint16_t XMODEMCRC16LOOKUP[] = {
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

uint16_t xredis::crc16(const char* data, size_t len) {
    uint16_t crc = 0x0000;
    for (size_t i = 0; i < len; ++i) {
        crc = (crc << 8) ^ XMODEMCRC16LOOKUP[((crc >> 8) ^ (uint8_t)data[i]) & 0xff];
    }
    return crc;
}