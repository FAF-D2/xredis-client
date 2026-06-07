#ifndef XREDIS_TEST_HEADERONLY
#include"../xredis-client.hpp"
#else
#include"../header-only/xredis-client.hpp"
#endif
#include<cstdio>
#include<cstdlib>
#include<vector>
using xredis::RedisValue;

int err = 0;

xnet::detached_task coro_main(xredis::RedisSubscriber& subscriber) noexcept{
    xredis::ConnectionOption option;
    option.clientname = "D2";
    option.db = 0;
    option.resp = 3;
#ifdef XREDIS_ENABLE_TLS
    option.tls.cacert = "../tls/ca.crt";
    option.tls.cert   = "../tls/client.crt";
    option.tls.key    = "../tls/client.key";
    option.tls.sni    = "localhost";
#endif

    auto result = co_await subscriber.connect(option);
    if(result.err){
        err = result.err;
        printf("error: %d\n", result.err);
        co_return;
    }
    else{
        printf("---Redis Response---\n");
        printf("%s\n", (*result).dump().c_str());
        printf("connect success\n");
    }
    
    {
        auto res = co_await subscriber.subscribe("test_channel1", "test_channel2", "test_channel3", "test_channel4");
        const std::vector<std::string_view> channels = {"test_channel2", "test_channel3"};
        res = co_await subscriber.unsubscribe(channels.begin(), channels.size());
    }

    for(int i = 0; i < 6; i++){
        auto packet = co_await subscriber.read_one_packet();
        if(packet.err || !(packet->is_array() || packet->is_push())){
            err = packet.err ? packet.err : -1;
            printf("packet error: %d\n", packet.err);
            break; 
        }
        printf("read packet: %s\n", packet->dump().c_str());
        auto& arr = packet->as<RedisValue::array_t>();
        // parse packet
    }

    subscriber.close();
}

int main(){
    xnet::io_context ctx;
    xredis::RedisSubscriber redis(ctx);
    coro_main(redis);

    ctx.run_until_complete();
    return err;
}