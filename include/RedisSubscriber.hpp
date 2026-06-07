#ifndef redis_subscriber_hpp
#define redis_subscriber_hpp
#include"xnet.hpp"
#include"RedisValue.h"
#include"RedisParser.h"
#include"RedisConnection.h"
#include<string>
#include<string_view>
#include<iterator>

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

#endif
