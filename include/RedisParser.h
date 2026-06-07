#ifndef redis_parser_h
#define redis_parser_h
#include"xnet.hpp"
#include"RedisValue.h"
#include<string>
#include<string_view>
#include<cstdint>
#include<charconv>

#ifndef XREDIS_PARSER_SOCKBUFFER_SIZE
#define XREDIS_PARSER_SOCKBUFFER_SIZE 2048
#endif

#ifndef XREDIS_PARSER_MAXBULK_SIZE
#define XREDIS_PARSER_MAXBULK_SIZE 1024 * 1024 * 512
#endif

#ifndef XREDIS_PARSER_LINEBUFFER_SIZE
#define XREDIS_PARSER_LINEBUFFER_SIZE 511
#endif

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


#undef XREDIS_PARSER_SOCKBUFFER_SIZE
#endif