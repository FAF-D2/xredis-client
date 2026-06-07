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
    int ret_error = 1; // first function
    auto check_error = [](auto&& res){
        return !res.is_string_type() || res.as_string() != "OK";
    };
    {
        auto res = co_await redis.set("test_string1", "test_string1");
        if(check_error(res)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.set("test_string1", "test_string1")
                                          .set("test_string2", "test_string2");
        if(check_error(res1) || check_error(res2)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2, res3] = co_await redis.set("test_string1", "test_string1")
                                                .set("test_string2", "test_string2")
                                                .set("test_string3", "test_string3");

        if(check_error(res1) || check_error(res2) || check_error(res3)){
            err = ret_error;
        }
    }                       
    co_return;
}

xnet::task<> command2(Redis& redis) noexcept{
    int ret_error = 2; // first function
    auto check_error = [](auto&& res, std::string_view str){
        return !res.is_string_type() || res.as_string() != str;
    };
    co_await redis.mset("{test_string}1", "test_string1", "{test_string}2", "test_string2", "{test_string}3", "test_string3");
    {
        auto res = co_await redis.get("{test_string}1");
        if(check_error(res, "test_string1")){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.get("{test_string}1")
                                          .get("{test_string}2");
        if(check_error(res1, "test_string1") || check_error(res2, "test_string2")){
            err = ret_error;
        }
    }
    {
        auto [res1, res2, res3] = co_await redis.get("{test_string}1")
                                                .get("{test_string}2")
                                                .get("{test_string}3");

        if(check_error(res1, "test_string1") || check_error(res2, "test_string2") || check_error(res3, "test_string3")){
            err = ret_error;
        }
    }
    co_return;                   
}

xnet::task<> command3(Redis& redis) noexcept{
    int ret_error = 3;
    
    auto check_error = [](auto&& res, std::string_view expected){
        return !res.is_string_type() || res.as_string() != expected;
    };

    {
        auto res = co_await redis.set("test_string1", "test_string1");
        if(check_error(res, "OK")){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.append("test_string1", "aaaa").get("test_string1");
        if(check_error(res2, "test_string1aaaa")){
            err = ret_error;
        }
    }
    {
        auto [res1, res2, res3] = co_await redis.append("test_string1", "bbbbb").append("test_string1", "c").get("test_string1");
        if(check_error(res3, "test_string1aaaabbbbbc")){
            err = ret_error;
        }
    }
    co_return;
}

xnet::task<> command4(Redis& redis) noexcept{
    int ret_error = 4;
    
    auto check_del_count = [](RedisValue& res, int64_t expected){
        return !res.is_integer() || res.get<RedisValue::integer_t>() != expected;
    };

    co_await redis.set("del_key1", "val1").set("del_key2", "val2").set("del_{key}3", "val3");

    {
        auto res = co_await redis.del("del_key1");
        if(check_del_count(res, 1)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.del("del_key2")
                                          .del("del_{key}3", "non_exist_{key}");
        
        if(check_del_count(res1, 1) || check_del_count(res2, 1)){
            err = ret_error;
        }
    }
    {
        auto res = co_await redis.del("never_exist_key_123");
        if(check_del_count(res, 0)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command5(Redis& redis) noexcept{
    int ret_error = 5;
    
    auto check_error = [](RedisValue& res, int64_t expected){
        return !res.is_integer() || res.get<RedisValue::integer_t>() != expected;
    };

    co_await redis.del("incr_key");

    {
        auto res = co_await redis.incr("incr_key");
        if(check_error(res, 1)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.incr("incr_key")
                                          .incr("incr_key");
        if(check_error(res1, 2) || check_error(res2, 3)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2, res3] = co_await redis.incr("incr_key")
                                                .incr("incr_key")
                                                .incr("incr_key");
        if(check_error(res1, 4) || check_error(res2, 5) || check_error(res3, 6)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command6(Redis& redis) noexcept{
    int ret_error = 6;
    
    auto check_error = [](RedisValue& res, int64_t expected){
        return !res.is_integer() || res.get<RedisValue::integer_t>() != expected;
    };

    co_await redis.set("decr_key", "10");

    {
        auto res = co_await redis.decr("decr_key");
        if(check_error(res, 9)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.decr("decr_key")
                                          .decr("decr_key");
        if(check_error(res1, 8) || check_error(res2, 7)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2, res3] = co_await redis.decr("decr_key")
                                                .decr("decr_key")
                                                .decr("decr_key");
        if(check_error(res1, 6) || check_error(res2, 5) || check_error(res3, 4)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command7(Redis& redis) noexcept {
    int ret_error = 7;
    
    auto check_error = [](auto&& res){
        return !res.is_string_type() || res.as_string() != "OK";
    };

    {
        auto res = co_await redis.mset("{mset}_k1", "{mset}_v1");
        if(check_error(res)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.mset("{mset}_k1", "{mset}_v1_new", "{mset}_k2", "{mset}_v2")
                                          .mset("{mset}_k3", "{mset}_v3", "{mset}_k4", "{mset}_v4");
        if(check_error(res1) || check_error(res2)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2, res3] = co_await redis.mset("{mset}_k5", "{mset}_v5")
                                                .mset("{mset}_k6", "{mset}_v6")
                                                .mset("{mset}_k7", "{mset}_v7", "{mset}_k8", "{mset}_v8");
        if(check_error(res1) || check_error(res2) || check_error(res3)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command8(Redis& redis) noexcept{
    int ret_error = 8;
    
    auto check_array_error = [](RedisValue& res, const std::vector<std::string>& expected){
        if(!res.is_array()) return true;
        auto& arr = res.as<RedisValue::array_t>();
        if(arr.size() != expected.size()) return true;
        for (size_t i = 0; i < arr.size(); ++i){
            if(!arr[i].is_string_type() || arr[i].as_string() != expected[i]){
                return true;
            }
        }
        return false;
    };

    co_await redis.mset("{mg}_k1", "{mg}_v1", "{mg}_k2", "{mg}_v2", "{mg}_k3", "{mg}_v3");

    {
        auto res = co_await redis.mget("{mg}_k1", "{mg}_k2");
        if(check_array_error(res, {"{mg}_v1", "{mg}_v2"})){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.mget("{mg}_k1", "{mg}_k2", "{mg}_k3")
                                          .mget("{mg}_k2");
        if(check_array_error(res1, {"{mg}_v1", "{mg}_v2", "{mg}_v3"}) || check_array_error(res2, {"{mg}_v2"})){
            err = ret_error;
        }
    }
    {
        auto [res1, res2, res3] = co_await redis.mget("{mg}_k1")
                                                .mget("{mg}_k2")
                                                .mget("{mg}_k3");
        if(check_array_error(res1, {"{mg}_v1"}) || check_array_error(res2, {"{mg}_v2"}) || check_array_error(res3, {"{mg}_v3"})){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command9(Redis& redis) noexcept{
    int ret_error = 9;

    auto check_transaction_results = [](RedisValue& exec_res){
        if(!exec_res.is_array()) return true;
        auto& arr = exec_res.as<RedisValue::array_t>();
        if(arr.size() != 2) return true;
        if(!arr[0].is_string_type() || arr[0].as_string() != "OK") return true;
        if(!arr[1].is_integer() || arr[1].as<RedisValue::integer_t>() != 1) return true;
        return false;
    };

    co_await redis.del("tx_key:{1001}", "tx_counter:{1001}");

    {
        auto [m_res, s_res, i_res, exec_res] = co_await redis.multi("{1001}")
                                                            .set("tx_key:{1001}", "value")
                                                            .incr("tx_counter:{1001}")
                                                            .exec();

        if(!m_res.is_string_type() || m_res.as_string() != "OK" ||
            !s_res.is_string_type() || s_res.as_string() != "QUEUED" ||
            !i_res.is_string_type() || i_res.as_string() != "QUEUED"){
            err = ret_error;
        }

        if(check_transaction_results(exec_res)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command10(Redis& redis) noexcept{
    int ret_error = 10;

    auto check_integer_reply = [](RedisValue& res, int64_t expected){
        return !res.is_integer() || res.as<RedisValue::integer_t>() != expected;
    };

    co_await redis.set("exp_key", "temporary");

    {
        auto res = co_await redis.expire("exp_key", 10);
        if(check_integer_reply(res, 1)){
            err = ret_error;
        }
    }
    {
        co_await redis.set("exp_k1", "v1").set("exp_k2", "v2");
        
        auto [res1, res2] = co_await redis.expire("exp_k1", 5)
                                          .expire("exp_k2", 5);
                                          
        if(check_integer_reply(res1, 1) || check_integer_reply(res2, 1)){
            err = ret_error;
        }
    }
    {
        auto res = co_await redis.expire("non_exist_exp_key", 20);
        if(check_integer_reply(res, 0)){
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