#include <boost/redis/src.hpp>

#include <boost/redis/connection.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/consign.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cstdio>
#include <chrono>
#include <vector>

namespace net = boost::asio;
using boost::redis::request;
using boost::redis::response;
using boost::redis::config;
using boost::redis::connection;

constexpr int num_tasks = 10000;
int cur = 0;

using time_point_t = decltype(std::chrono::high_resolution_clock::now());
time_point_t global_start;
time_point_t global_end;

net::awaitable<void> redistask(std::shared_ptr<connection> conn) 
{
    try {
        request req;
        req.push("GET", "bulkstring");
        response<std::string> resp;

        co_await conn->async_exec(req, resp, net::use_awaitable);

        // int completed = cur.fetch_add(1, std::memory_order_acq_rel) + 1;
        int completed = ++cur;
        if (completed >= num_tasks) {
            global_end = std::chrono::high_resolution_clock::now();
            auto all = std::chrono::duration_cast<std::chrono::nanoseconds>(global_end - global_start).count();
            
            printf("num tasks: %d\n", num_tasks);
            printf("Avg Duration: %ld ns\n", all / num_tasks);
            printf("Total Duration: %lu ns\n", all);
            
            conn->cancel();
        }
    } catch (const std::exception& e) {
        fprintf(stderr, "Error: %s\n", e.what());
    }
    co_return;
}

net::awaitable<void> benchmark(net::io_context& ctx, std::shared_ptr<connection> conn) 
{
    config cfg;
    cfg.addr.host = "127.0.0.1";
    cfg.addr.port = "6379";

    conn->async_run(cfg, {}, net::consign(net::detached, conn));

    request init_req;
    std::string smallkey;
    smallkey.reserve(1024);
    for(int i = 0; i < 256; i++) {
        smallkey.append("\t");
    }
    init_req.push("SET", "bulkstring", smallkey);
    
    response<std::string> init_resp;
    co_await conn->async_exec(init_req, init_resp, net::use_awaitable);
    
    printf("connect success and data initialized\n");

    global_start = std::chrono::high_resolution_clock::now();
    
    auto ex = co_await net::this_coro::executor;

    for (int i = 0; i < num_tasks; ++i) {
        net::co_spawn(ex, redistask(conn), net::detached);
    }
}

int main() 
{
    net::io_context ctx;
    auto conn = std::make_shared<connection>(ctx.get_executor());

    net::co_spawn(ctx, benchmark(ctx, conn), net::detached);

    ctx.run();
    return 0;
}