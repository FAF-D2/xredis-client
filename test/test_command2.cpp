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

    co_await redis.del("h_user1");

    {
        auto res = co_await redis.hset("h_user1", "name", "Tom");
        if(check_integer_reply(res, 1)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.hset("h_user1", "age", "18")
                                          .hset("h_user1", "city", "Beijing", "gender", "male");
        if(check_integer_reply(res1, 1) || check_integer_reply(res2, 2)){
            err = ret_error;
        }
    }
    {
        auto res = co_await redis.hset("h_user1", "name", "Jack");
        if(check_integer_reply(res, 0)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command2(Redis& redis) noexcept{
    int ret_error = 2;

    auto check_string_reply = [](RedisValue& res, std::string_view expected){
        return !res.is_string_type() || res.as_string() != expected;
    };

    co_await redis.del("h_user1");
    co_await redis.hset("h_user1", "name", "Tom", "age", "18");

    {
        auto res = co_await redis.hget("h_user1", "name");
        if(check_string_reply(res, "Tom")){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.hget("h_user1", "age")
                                          .hget("h_user1", "name");
        if(check_string_reply(res1, "18") || check_string_reply(res2, "Tom")){
            err = ret_error;
        }
    }
    {
        auto res = co_await redis.hget("h_user1", "non_exist_field");
        if(!res.is_nullptr()){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command3(Redis& redis) noexcept {
    int ret_error = 3;

    auto check_integer_reply = [](RedisValue& res, int64_t expected){
        return !res.is_integer() || res.as<RedisValue::integer_t>() != expected;
    };

    co_await redis.del("l_list1");

    {
        auto res = co_await redis.lpush("l_list1", "node1");
        if(check_integer_reply(res, 1)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.lpush("l_list1", "node2")
                                          .lpush("l_list1", "node3", "node4");
        if(check_integer_reply(res1, 2) || check_integer_reply(res2, 4)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command4(Redis& redis) noexcept {
    int ret_error = 4;

    auto check_string_reply = [](RedisValue& res, std::string_view expected){
        return !res.is_string_type() || res.as_string() != expected;
    };

    co_await redis.del("l_list1");
    co_await redis.lpush("l_list1", "a", "b", "c");

    {
        auto res = co_await redis.rpop("l_list1");
        if (check_string_reply(res, "a")){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.rpop("l_list1")
                                          .rpop("l_list1");
        if(check_string_reply(res1, "b") || check_string_reply(res2, "c")){
            err = ret_error;
        }
    }
    {
        auto res = co_await redis.rpop("l_list1");
        if(!res.is_nullptr()){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command5(Redis& redis) noexcept{
    int ret_error = 5;

    auto check_array_reply = [](RedisValue& res, size_t expected_size){
        if (!res.is_array()) return true;
        auto& arr = res.as<RedisValue::array_t>();
        return arr.size() != expected_size;
    };

    co_await redis.del("h_user1");
    co_await redis.hset("h_user1", "name", "Tom", "age", "18", "city", "Beijing");

    {
        auto res = co_await redis.hmget("h_user1", "name", "city");
        if(check_array_reply(res, 2)){
            err = ret_error;
        } 
        else{
            auto& arr = res.as<RedisValue::array_t>();
            if(!arr[0].is_string_type() || arr[0].as_string() != "Tom" ||
                !arr[1].is_string_type() || arr[1].as_string() != "Beijing"){
                err = ret_error;
            }
        }
    }
    {
        auto [res1, res2] = co_await redis.hmget("h_user1", "age")
                                          .hmget("h_user1", "name", "non_exist");
        if(check_array_reply(res1, 1) || check_array_reply(res2, 2)){
            err = ret_error;
        } 
        else{
            auto& arr1 = res1.as<RedisValue::array_t>();
            auto& arr2 = res2.as<RedisValue::array_t>();
            if(!arr1[0].is_string_type() || arr1[0].as_string() != "18" ||
                !arr2[0].is_string_type() || arr2[0].as_string() != "Tom" ||
                !arr2[1].is_nullptr()){
                err = ret_error;
            }
        }
    }

    co_return;
}

xnet::task<> command6(Redis& redis) noexcept{
    int ret_error = 6;

    auto check_integer_reply = [](RedisValue& res, int64_t expected){
        return !res.is_integer() || res.as<RedisValue::integer_t>() != expected;
    };

    co_await redis.del("h_user1");
    co_await redis.hset("h_user1", "f1", "v1", "f2", "v2", "f3", "v3");

    {
        auto res = co_await redis.hdel("h_user1", "f1");
        if(check_integer_reply(res, 1)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.hdel("h_user1", "f2", "f3")
                                          .hdel("h_user1", "non_exist");
        if(check_integer_reply(res1, 2) || check_integer_reply(res2, 0)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command7(Redis& redis) noexcept{
    int ret_error = 7;

    auto check_integer_reply = [](RedisValue& res, int64_t expected){
        return !res.is_integer() || res.as<RedisValue::integer_t>() != expected;
    };

    co_await redis.del("l_list1");

    {
        auto res = co_await redis.rpush("l_list1", "node1");
        if (check_integer_reply(res, 1)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.rpush("l_list1", "node2")
                                          .rpush("l_list1", "node3", "node4");
        if (check_integer_reply(res1, 2) || check_integer_reply(res2, 4)){
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command8(Redis& redis) noexcept{
    int ret_error = 8;

    auto check_string_reply = [](RedisValue& res, std::string_view expected){
        return !res.is_string_type() || res.as_string() != expected;
    };

    co_await redis.del("l_list1");
    co_await redis.rpush("l_list1", "a", "b", "c");

    {
        auto res = co_await redis.lpop("l_list1");
        if(check_string_reply(res, "a")){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.lpop("l_list1")
                                          .lpop("l_list1");
        if(check_string_reply(res1, "b") || check_string_reply(res2, "c")){
            err = ret_error;
        }
    }
    {
        auto res = co_await redis.lpop("l_list1");
        if (!res.is_nullptr()) {
            err = ret_error;
        }
    }

    co_return;
}

xnet::task<> command9(Redis& redis) noexcept{
    int ret_error = 9;

    co_await redis.del("l_list1");
    co_await redis.rpush("l_list1", "item1", "item2", "item3");

    {
        auto res = co_await redis.lrange("l_list1", 0, -1);
        if(!res.is_array()){
            err = ret_error;
        }
        else{
            auto& arr = res.as<RedisValue::array_t>();
            if(arr.size() != 3 || arr[0].as_string() != "item1" || arr[2].as_string() != "item3"){
                err = ret_error;
            }
        }
    }
    {
        auto [res1, res2] = co_await redis.lrange("l_list1", 0, 1)
                                          .lrange("l_list1", -1, -1);
        if(!res1.is_array() || !res2.is_array()){
            err = ret_error;
        }
        else{
            auto& arr1 = res1.as<RedisValue::array_t>();
            auto& arr2 = res2.as<RedisValue::array_t>();
            if(arr1.size() != 2 || arr1[1].as_string() != "item2" ||
                arr2.size() != 1 || arr2[0].as_string() != "item3"){
                err = ret_error;
            }
        }
    }

    co_return;
}

xnet::task<> command10(Redis& redis) noexcept {
    int ret_error = 10;

    auto check_integer_reply = [](RedisValue& res, int64_t expected){
        return !res.is_integer() || res.as<RedisValue::integer_t>() != expected;
    };

    co_await redis.del("h_user1");
    co_await redis.hset("h_user1", "status", "active");

    {
        auto res = co_await redis.hexists("h_user1", "status");
        if(check_integer_reply(res, 1)){
            err = ret_error;
        }
    }
    {
        auto [res1, res2] = co_await redis.hexists("h_user1", "token")
                                          .hexists("non_exist_key", "any");
        if(check_integer_reply(res1, 0) || check_integer_reply(res2, 0)){
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