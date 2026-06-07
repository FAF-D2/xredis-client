#include"../include/RedisParser.h"
using xredis::RedisParser;
using xredis::RedisValue;

xnet::task<xnet::io_result<std::string_view>> RedisParser::read_line(char& ch) noexcept{
    size_t fd = this->pos;
    while(true){
        fd = this->linebuffer.find('\n', fd);
        if(fd != std::string::npos){
            if(fd == this->pos || this->linebuffer[fd - 1] != '\r'){
                co_return {"", -1};
            }
            ch = this->linebuffer[this->pos];
            std::string_view data(this->linebuffer.data() + this->pos + 1, fd - (this->pos + 2));
            this->pos = fd + 1;
            co_return {data, 0};
        }
        fd = this->linebuffer.size();
        xnet::io_result<size_t> result;
        if(this->timed <= 0){
            result = co_await this->stream.recv(this->sockbuffer, sockbuffer_size, 0);
        }
        else{
            result = co_await this->stream.recv(this->sockbuffer, sockbuffer_size, 0).timeout(this->timed);
        }
        if(result.err || *result == 0){
            co_return {"", result.err ? result.err : ECONNRESET};
        }
        if(this->linebuffer.size() + *result >= max_bulk_size){
            co_return {"", EOVERFLOW};
        }
        this->linebuffer.append(this->sockbuffer, *result);
    }
}

xnet::task<xnet::io_result<std::string_view>> RedisParser::read_bulk(size_t bytes) noexcept{
    if(bytes >= max_bulk_size){
        co_return {"", EOVERFLOW};
    }
    this->linebuffer.reserve(bytes + 2 + this->pos);
    while(true){
        if(this->linebuffer.size() - this->pos >= bytes + 2){
            const char* base = this->linebuffer.data() + this->pos;
            if(base[bytes] != '\r' || base[bytes + 1] != '\n'){
                co_return {"", -2};
            }
            std::string_view data(base, bytes);
            this->pos += bytes + 2;
            co_return {data, 0};
        }
        xnet::io_result<size_t> result;
        if(this->timed <= 0){
            result = co_await this->stream.recv(this->sockbuffer, sockbuffer_size, 0);
        }
        else{
            result = co_await this->stream.recv(this->sockbuffer, sockbuffer_size, 0).timeout(this->timed);
        }
        if(result.err || *result == 0){
            co_return {"", result.err ? result.err : ECONNRESET};
        }
        this->linebuffer.append(this->sockbuffer, *result);
    }
}

void RedisParser::consume() noexcept{
    this->linebuffer.erase(0, this->pos);
    this->pos = 0;
}

void RedisParser::reset() noexcept{
    this->linebuffer.clear();
    this->pos = 0;
}

xnet::task<xnet::io_result<RedisValue>> RedisParser::parse(int timeout) noexcept{
    RedisValue result;
    this->timed = timeout;
    int err = co_await parse_recursion(result);
    if(err == 0){
        this->consume();
    }
    co_return {std::move(result), err};
}

static int stoll_view(const std::string_view& sv, int64_t& result) noexcept{
    if(sv.empty()){
        return EINVAL;
    }

    int64_t integer;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), integer);
    if(ec == std::errc() && ptr == sv.data() + sv.size()){
        result = integer;
        return 0;
    }
    return ec == std::errc::result_out_of_range ? ERANGE : EINVAL;
}

static int stod_view(const std::string_view& sv, double& result) noexcept{
    if(sv.empty()){
        return EINVAL;
    }
    double doublev;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), doublev);
    if(ec == std::errc() && ptr == sv.data() + sv.size()){
        result = doublev;
        return 0;
    }
    return ec == std::errc::result_out_of_range ? ERANGE : EINVAL;
}

xnet::task<int> RedisParser::parse_recursion(RedisValue& result) noexcept{
    char prefix = '\0';
    auto line = co_await this->read_line(prefix);
    if(line.err){
        co_return line.err;
    }

    switch(prefix){
    // --------------------------
    // --- RESP 2---
    case '+':{
        // simple string
        result.set<RedisValue::simple_string_t>(line.move());
        co_return 0;
    }
    case '-':{
        // simple error
        result.set<RedisValue::simple_error_t>(line.move());
        co_return 0;
    }
    case ':':{
        // integer
        int64_t integer;
        int err = stoll_view(*line, integer);
        if(!err){
            result.set<RedisValue::integer_t>(integer);
        }
        co_return err;
    }
    case '$':{
        // bulk string
        int64_t length;
        int err = stoll_view(*line, length);
        if(err || length < 0){
            co_return err ? err : (length == -1 ? 0 : EINVAL);
        }
        auto bulk = co_await this->read_bulk(static_cast<size_t>(length));
        if(bulk.err){
            co_return bulk.err;
        }
        result.set<RedisValue::bulk_string_t>(bulk.move());
        co_return 0;
    }
    case '*':{
        // Array
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            co_return err ? err : EINVAL;
        }
        RedisValue::array_t vec;
        vec.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            vec.emplace_back();
            err = co_await parse_recursion(vec[vec.size() - 1]);
            if(err){
                co_return err;
            }
        }
        result.set<RedisValue::array_t>(std::move(vec));
        co_return 0;
    }
    // --------------------------
    // --- RESP 3---
    case '_':{
        // Null
        result.set<RedisValue::null_t>(nullptr);
        co_return 0;
    }
    case '#':{
        // Boolean
        char tof;
        if(line->size() == 1){
            tof = (*line)[0];
            if(tof == 't' || tof == 'f'){
                result.set<RedisValue::bool_t>(tof == 't');
                co_return 0;
            }
        }
        co_return EINVAL;
    }
    case ',':{
        // Double
        double doublev;
        int err = stod_view(*line, doublev);
        if(!err){
            result.set<RedisValue::double_t>(doublev);
        }
        co_return err;
    }
    case '(':{
        // Big number
        result.set<RedisValue::big_number_t>(line.move());
        co_return 0;
    }
    case '!':{
        // Bulk error
        int64_t length;
        int err = stoll_view(*line, length);
        if(err || length < 0){
            co_return err ? err : EINVAL;
        }
        auto bulk = co_await this->read_bulk(static_cast<size_t>(length));
        if(bulk.err){
            co_return bulk.err;
        }
        result.set<RedisValue::bulk_error_t>(bulk.move());
        co_return 0;
    }
    case '=':{
        // Verbatim string
        int64_t length;
        int err = stoll_view(*line, length);
        if(err || length < 0){
            co_return err ? err : EINVAL;
        }
        auto bulk = co_await this->read_bulk(static_cast<size_t>(length));
        if(bulk.err){
            co_return bulk.err;
        }
        result.set<RedisValue::verbatim_string_t>(bulk.move());
        co_return 0;
    }
    case '%':{
        // Map
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            co_return err ? err : EINVAL;
        }
        RedisValue::map_t map;
        map.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            RedisValue key;
            RedisValue value;
            err = co_await parse_recursion(key);
            if(err){ co_return err; }
            err = co_await parse_recursion(value);
            if(err){ co_return err; }
            map.emplace_back(std::move(key), std::move(value));
        }
        result.set<RedisValue::map_t>(std::move(map));
        co_return 0;
    }
    case '|':{
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            co_return err ? err : EINVAL;
        }
        RedisValue::map_t metadata;
        metadata.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            RedisValue key;
            RedisValue value;
            err = co_await parse_recursion(key);
            if(err){ co_return err; }
            err = co_await parse_recursion(value);
            if(err){ co_return err; }
            metadata.emplace_back(std::move(key), std::move(value));
        }

        RedisValue value;
        err = co_await parse_recursion(value);
        if(err){ co_return err; }

        result.set<RedisValue::attribute_t>(
            RedisValue::attribute_t(std::move(metadata), std::move(value))
        );
        co_return 0;
    }
    case '~':{
        // Set
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            co_return err ? err : EINVAL;
        }
        RedisValue::set_t set;
        set.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            set.emplace_back();
            err = co_await parse_recursion(set[set.size() - 1]);
            if(err){
                co_return err;
            }
        }
        result.set<RedisValue::set_t>(std::move(set));
        co_return 0;
    }
    case '>':{
        // Push
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            co_return err ? err : EINVAL;
        }
        RedisValue::push_t push;
        push.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            push.emplace_back();
            err = co_await parse_recursion(push[push.size() - 1]);
            if(err){
                co_return err;
            }
        }
        result.set<RedisValue::push_t>(std::move(push));
        co_return 0;
    }
    default:{
        break;
    }
    }
    co_return -3;
}

xnet::io_result<std::string_view> RedisParser::try_read_line(char& ch) noexcept{
    size_t fd = this->pos;
    while(true){
        fd = this->linebuffer.find('\n', fd);
        if(fd != std::string::npos){
            if(fd == this->pos || this->linebuffer[fd - 1] != '\r'){
                return {"", -1};
            }
            ch = this->linebuffer[this->pos];
            std::string_view data(this->linebuffer.data() + this->pos + 1, fd - (this->pos + 2));
            this->pos = fd + 1;
            return {data, 0};
        }
        fd = this->linebuffer.size();
        ssize_t ret = ::recv(this->stream.fd(), this->sockbuffer, sockbuffer_size, 0);
        int err = (ret < 0 ? errno : (ret == 0 ? ECONNRESET : 0));
        if(err){
            return {"", err};
        }
        if(this->linebuffer.size() + ret >= max_bulk_size){
            return {"", EOVERFLOW};
        }
        this->linebuffer.append(this->sockbuffer, static_cast<size_t>(ret));
    }
}

xnet::io_result<std::string_view> RedisParser::try_read_bulk(size_t bytes) noexcept{
    if(bytes >= max_bulk_size){
        return {"", EOVERFLOW};
    }
    this->linebuffer.reserve(bytes + 2 + this->pos);
    while(true){
        if(this->linebuffer.size() - this->pos >= bytes + 2){
            const char* base = this->linebuffer.data() + this->pos;
            if(base[bytes] != '\r' || base[bytes + 1] != '\n'){
                return {"", -2};
            }
            std::string_view data(base, bytes);
            this->pos += bytes + 2;
            return {data, 0};
        }
        ssize_t ret = ::recv(this->stream.fd(), this->sockbuffer, sockbuffer_size, 0);
        int err = (ret < 0 ? errno : (ret == 0 ? ECONNRESET : 0));
        if(err){
            return {"", err};
        }
        this->linebuffer.append(this->sockbuffer, static_cast<size_t>(ret));
    }
}

xnet::io_result<size_t> RedisParser::try_parse_n(RedisValue* values, int* errs, size_t n) noexcept{
    for(size_t i = 0; i < n; i++){
        int pivot = this->pos;
        errs[i] = try_parse_recursion(values[i]);
        if(errs[i] == EAGAIN){
            this->linebuffer.erase(0, pivot);
            this->pos = 0;
            return {i, 0};
        }
        else if(errs[i]){
            this->pos = 0;
            return {i, errs[i]};
        }
    }
    this->linebuffer.erase(0, this->pos);
    this->pos = 0;
    return {n, 0};
}

xnet::io_result<RedisValue> RedisParser::try_parse() noexcept{
    RedisValue result;
    int err = try_parse_recursion(result);
    if(err == 0){
        this->consume();
    }
    this->pos = 0;
    return {std::move(result), err};
}

int RedisParser::try_parse_recursion(RedisValue& result) noexcept{
    char prefix = '\0';
    auto line = this->try_read_line(prefix);
    if(line.err){
        return line.err;
    }

    switch(prefix){
    // --------------------------
    // --- RESP 2---
    case '+':{
        // simple string
        result.set<RedisValue::simple_string_t>(line.move());
        return 0;
    }
    case '-':{
        // simple error
        result.set<RedisValue::simple_error_t>(line.move());
        return 0;
    }
    case ':':{
        // integer
        int64_t integer;
        int err = stoll_view(*line, integer);
        if(!err){
            result.set<RedisValue::integer_t>(integer);
        }
        return err;
    }
    case '$':{
        // bulk string
        int64_t length;
        int err = stoll_view(*line, length);
        if(err || length < 0){
            return err ? err : (length == -1 ? 0 : EINVAL);
        }
        auto bulk = this->try_read_bulk(static_cast<size_t>(length));
        if(bulk.err){
            return bulk.err;
        }
        result.set<RedisValue::bulk_string_t>(bulk.move());
        return 0;
    }
    case '*':{
        // Array
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            return err ? err : EINVAL;
        }
        RedisValue::array_t vec;
        vec.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            vec.emplace_back();
            err = this->try_parse_recursion(vec[vec.size() - 1]);
            if(err){
                return err;
            }
        }
        result.set<RedisValue::array_t>(std::move(vec));
        return 0;
    }
    // --------------------------
    // --- RESP 3---
    case '_':{
        // Null
        result.set<RedisValue::null_t>(nullptr);
        return 0;
    }
    case '#':{
        // Boolean
        char tof;
        if(line->size() == 1){
            tof = (*line)[0];
            if(tof == 't' || tof == 'f'){
                result.set<RedisValue::bool_t>(tof == 't');
                return 0;
            }
        }
        return EINVAL;
    }
    case ',':{
        // Double
        double doublev;
        int err = stod_view(*line, doublev);
        if(!err){
            result.set<RedisValue::double_t>(doublev);
        }
        return err;
    }
    case '(':{
        // Big number
        result.set<RedisValue::big_number_t>(line.move());
        return 0;
    }
    case '!':{
        // Bulk error
        int64_t length;
        int err = stoll_view(*line, length);
        if(err || length < 0){
            return err ? err : EINVAL;
        }
        auto bulk = this->try_read_bulk(static_cast<size_t>(length));
        if(bulk.err){
            return bulk.err;
        }
        result.set<RedisValue::bulk_error_t>(bulk.move());
        return 0;
    }
    case '=':{
        // Verbatim string
        int64_t length;
        int err = stoll_view(*line, length);
        if(err || length < 0){
            return err ? err : EINVAL;
        }
        auto bulk = this->try_read_bulk(static_cast<size_t>(length));
        if(bulk.err){
            return bulk.err;
        }
        result.set<RedisValue::verbatim_string_t>(bulk.move());
        return 0;
    }
    case '%':{
        // Map
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            return err ? err : EINVAL;
        }
        RedisValue::map_t map;
        map.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            RedisValue key;
            RedisValue value;
            err = this->try_parse_recursion(key);
            if(err){ return err; }
            err = this->try_parse_recursion(value);
            if(err){ return err; }
            map.emplace_back(std::move(key), std::move(value));
        }
        result.set<RedisValue::map_t>(std::move(map));
        return 0;
    }
    case '|':{
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            return err ? err : EINVAL;
        }
        RedisValue::map_t metadata;
        metadata.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            RedisValue key;
            RedisValue value;
            err = this->try_parse_recursion(key);
            if(err){ return err; }
            err = this->try_parse_recursion(value);
            if(err){ return err; }
            metadata.emplace_back(std::move(key), std::move(value));
        }

        RedisValue value;
        err = this->try_parse_recursion(value);
        if(err){ return err; }

        result.set<RedisValue::attribute_t>(
            RedisValue::attribute_t(std::move(metadata), std::move(value))
        );
        return 0;
    }
    case '~':{
        // Set
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            return err ? err : EINVAL;
        }
        RedisValue::set_t set;
        set.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            set.emplace_back();
            err = this->try_parse_recursion(set[set.size() - 1]);
            if(err){
                return err;
            }
        }
        result.set<RedisValue::set_t>(std::move(set));
        return 0;
    }
    case '>':{
        // Push
        int64_t count;
        int err = stoll_view(*line, count);
        if(err || count < 0){
            return err ? err : EINVAL;
        }
        RedisValue::push_t push;
        push.reserve(count);
        err = 0;
        for(int64_t i = 0; i < count; i++){
            push.emplace_back();
            err = this->try_parse_recursion(push[push.size() - 1]);
            if(err){
                return err;
            }
        }
        result.set<RedisValue::push_t>(std::move(push));
        return 0;
    }
    default:{
        break;
    }
    }
    return -3;
}