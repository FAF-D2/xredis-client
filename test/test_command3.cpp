#ifndef XREDIS_TEST_HEADERONLY
#include"../xredis-client.hpp"
#else
#include"../header-only/xredis-client.hpp"
#endif
#include<cstdio>
#include<cstdlib>
using xredis::RedisValue;

int err = 0;

using Redis = xredis::RedisClient<>;
static const xredis::ConnectionOption& get_option() noexcept{
    static xredis::ConnectionOption option;
    option.clientname = "D2";
    option.db = 0;
#ifdef XREDIS_ENABLE_TLS
    option.tls.cacert = "../tls/ca.crt";
    option.tls.cert   = "../tls/client.crt";
    option.tls.key    = "../tls/client.key";
    option.tls.sni    = "localhost";
#endif
    return option;
}

// using Redis = xredis::RedisCluster<>;
// static const xredis::ClusterConnectionOption& get_option() noexcept{
//     static xredis::ClusterConnectionOption option;
//     option.clientname = "D2";
//     static const char* ips[] = {"127.0.0.1", "127.0.0.1", "127.0.0.1"};
//     static const uint16_t ports[] = {7000, 7001, 7002};
//     option.seeds_ip = ips;
//     option.ports = ports;
//     option.num_seeds = 3;
// #ifdef XREDIS_ENABLE_TLS
//     option.tls.cacert = "../tls/ca.crt";
//     option.tls.cert   = "../tls/client.crt";
//     option.tls.key    = "../tls/client.key";
//     option.tls.sni    = "localhost";
// #endif
//     return option;
// }

xnet::task<> command1(Redis& redis) noexcept{
    int ret_error = 1;

    auto check_integer_reply = [](RedisValue& res, int64_t expected){
        return !res.is_integer() || res.as<RedisValue::integer_t>() != expected;
    };

    co_await redis.del("k_exists_test");

    {
        auto res = co_await redis.exists("k_exists_test");
        if(check_integer_reply(res, 0)){
            err = ret_error;
        }
    }
    {
        co_await redis.set("k_exists_test", "1");
        auto [res1, res2] = co_await redis.exists("k_exists_test")
                                           .exists("non_exist_key");
        if(check_integer_reply(res1, 1) || check_integer_reply(res2, 0)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command2(Redis& redis) noexcept{
    int ret_error = 2;

    auto check_integer_reply = [](RedisValue& res, int64_t expected){
        return !res.is_integer() || res.as<RedisValue::integer_t>() != expected;
    };

    co_await redis.del("k_ttl_test");

    {
        auto res = co_await redis.ttl("k_ttl_test");
        if(check_integer_reply(res, -2)){
            err = ret_error;
        }
    }
    {
        co_await redis.set("k_ttl_test", "1");
        auto res = co_await redis.ttl("k_ttl_test");
        if(check_integer_reply(res, -1)){
            err = ret_error;
        }
    }
    {
        co_await redis.expire("k_ttl_test", 10);
        auto res = co_await redis.ttl("k_ttl_test");
        if(!res.is_integer() || res.as<RedisValue::integer_t>() <= 0){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command3(Redis& redis) noexcept{
    int ret_error = 3;

    auto check_status_reply = [](RedisValue& res, std::string_view expected){
        return !res.is_string_type() || res.as_string() != expected;
    };

    {
        auto res = co_await redis.set("test_counter", "100");
        if(check_status_reply(res, "OK")){
            err = ret_error;
        }
    }
    {
        auto [res1, res2, res3] = co_await redis.incrby("test_counter", 420)
                                           .set("test_counter", "not counter")
                                           .get("test_counter");
        if(!(res1.is_integer() && res1.as<xredis::RedisValue::integer_t>() == 520) || check_status_reply(res3, "not counter")){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command4(Redis& redis) noexcept{
    int ret_error = 4;

    auto check_status_reply = [](RedisValue& res, size_t expected){
        return res.is_integer() && res.as<xredis::RedisValue::integer_t>() != expected;
    };

    {
        auto res = co_await redis.publish("any_test_channel", "hello xredis-client");
        if(check_status_reply(res, 0)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command5(Redis& redis) noexcept{
    int ret_error = 5;

    auto check_integer_reply = [](RedisValue& res, int64_t expected){
        return !res.is_integer() || res.as<RedisValue::integer_t>() != expected;
    };

    co_await redis.del("s_user_tags");

    {
        auto res = co_await redis.sadd("s_user_tags", "tech");
        if(check_integer_reply(res, 1)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.sadd("s_user_tags", "music")
                                           .sadd("s_user_tags", "tech", "sports", "game");
        if(check_integer_reply(res1, 1) || check_integer_reply(res2, 2)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command6(Redis& redis) noexcept{
    int ret_error = 6;

    auto check_integer_reply = [](RedisValue& res, int64_t expected){
        return !res.is_integer() || res.as<RedisValue::integer_t>() != expected;
    };

    co_await redis.del("s_user_tags");
    co_await redis.sadd("s_user_tags", "tech", "music", "sports");

    {
        auto res = co_await redis.srem("s_user_tags", "music");
        if(check_integer_reply(res, 1)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.srem("s_user_tags", "tech")
                                           .srem("s_user_tags", "music", "sports", "non_exist");
        if(check_integer_reply(res1, 1) || check_integer_reply(res2, 1)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command7(Redis& redis) noexcept{
    int ret_error = 7;

    co_await redis.del("s_user_tags");
    co_await redis.sadd("s_user_tags", "tech", "music");

    auto check_res_error = [](RedisValue& res, int64_t expected){
        return !res.is_set() || res.as<RedisValue::set_t>().size() != expected;
    };

    {
        auto res = co_await redis.smembers("s_user_tags");
        if(check_res_error(res, 2)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.smembers("s_user_tags")
                                           .smembers("non_exist_set");
        if(check_res_error(res1, 2) || check_res_error(res2, 0)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command8(Redis& redis) noexcept{
    int ret_error = 8;

    auto check_integer_reply = [](RedisValue& res, int64_t expected){
        return !res.is_integer() || res.as<RedisValue::integer_t>() != expected;
    };

    co_await redis.del("s_user_tags");

    {
        auto res = co_await redis.scard("s_user_tags");
        if(check_integer_reply(res, 0)){
            err = ret_error;
        }
    }
    {
        co_await redis.sadd("s_user_tags", "tech", "music");
        auto [res1, res2] = co_await redis.scard("s_user_tags")
                                           .scard("non_exist_set");
        if(check_integer_reply(res1, 2) || check_integer_reply(res2, 0)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command9(Redis& redis) noexcept{
    int ret_error = 9;

    auto check_integer_reply = [](RedisValue& res, int64_t expected){
        return !res.is_integer() || res.as<RedisValue::integer_t>() != expected;
    };

    co_await redis.del("s_user_tags");
    co_await redis.sadd("s_user_tags", "tech", "music");

    {
        auto res = co_await redis.sismember("s_user_tags", "tech");
        if(check_integer_reply(res, 1)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.sismember("s_user_tags", "sports")
                                           .sismember("non_exist_set", "tech");
        if(check_integer_reply(res1, 0) || check_integer_reply(res2, 0)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command10(Redis& redis) noexcept {
    int ret_error = 10;

    auto check_res_error = [](RedisValue& res, size_t expected){
        return !res.is_set() || res.as<RedisValue::set_t>().size() != expected;
    };

    co_await redis.del("{s_set}1", "{s_set}2");
    co_await redis.sadd("{s_set}1", "A", "B");
    co_await redis.sadd("{s_set}2", "B", "C");

    {
        auto res = co_await redis.sunion("{s_set}1", "{s_set}2");
        if(check_res_error(res, 3)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.sunion("{s_set}1", "non_exist_{s_set}")
                                           .sunion("{non_exist}1", "{non_exist}2");
        if(check_res_error(res1, 2) || check_res_error(res2, 0)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::detached_task coro_main(Redis& redis) noexcept{
    auto result = co_await redis.connect(get_option());
    if(result.err){
        err = result.err;
        printf("error: %d\n", result.err);
        co_return;
    }
    else{
        printf("connect success\n");
    }

    co_await command1(redis);
    co_await command2(redis);
    co_await command3(redis);
    co_await command4(redis);
    co_await command5(redis);
    co_await command6(redis);
    co_await command7(redis);
    co_await command8(redis);
    co_await command9(redis);
    co_await command10(redis);

    redis.close();
}

int main(){
    xnet::io_context ctx;
    Redis redis(ctx);
    coro_main(redis);

    ctx.run_until_complete();
    return err;
}