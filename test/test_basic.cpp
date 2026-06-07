#ifndef XREDIS_TEST_HEADERONLY
#include"../xredis-client.hpp"
#else
#include"../header-only/xredis-client.hpp"
#endif
#include<cstdio>
#include<iostream>
#include<chrono>
using xredis::RedisValue;

constexpr int num_tasks = 10000;
constexpr int ring_buffer_size = 4096;
constexpr int yield_iterations = 8;
using Redis = xredis::RedisClient<true, ring_buffer_size>;

int cur = 0;
using time_point_t = decltype(std::chrono::high_resolution_clock::now());
time_point_t global_start;
time_point_t global_end;

xnet::io_context ctx;
xnet::AsyncTimer timer(ctx);

xnet::detached_task redistask(Redis& redis) noexcept{
    auto res = co_await redis.get("bulkstring");
    if(res.is_error()){
        printf("Error: %s\n", res.as_error().data());
    }
    ++cur;
    if(cur >= num_tasks){
        global_end = std::chrono::high_resolution_clock::now();
        auto all = std::chrono::duration_cast<std::chrono::nanoseconds>(global_end - global_start).count();
        printf("num tasks: %d\n", num_tasks);
        printf("Avg Duration: %ld ns\n", all / num_tasks);
        printf("Total Duration: %lu ns\n", all);
        redis.close();
    }
}

xnet::detached_task benchmark(Redis& redis) noexcept{
    xredis::ConnectionOption option;
    option.clientname = "D2";
    option.db = 0;
#ifdef XREDIS_ENABLE_TLS
    option.tls.cacert = "../tls/ca.crt";
    option.tls.cert   = "../tls/client.crt";
    option.tls.key    = "../tls/client.key";
    option.tls.sni    = "localhost";
#endif

    auto result = co_await redis.connect(option);
    if(result.err){
        printf("error: %d\n", result.err);
        co_return;
    }
    else{
        printf("connect success\n");
    }

    // std::string bigkey;
    // bigkey.reserve(2048);
    // for(int i = 0; i < 1024; i++){
    //     bigkey.append("\t");
    // }
    // co_await redis.set("bulkstring", bigkey);

    std::string smallkey;
    smallkey.reserve(1024);
    for(int i = 0; i < 256; i++){
        smallkey.append("\t");
    }
    co_await redis.set("bulkstring", smallkey);

    global_start = std::chrono::high_resolution_clock::now();
    for(int i = 0; i < num_tasks; i++){
        redistask(redis);
        if((i + 1) % yield_iterations == 0){
            co_await redis.context().yield(); // yield for redis parse()
        }
    }
}

int main(){
    Redis redis(ctx);
    benchmark(redis);

    ctx.run_until_complete();
    return 0;
}
