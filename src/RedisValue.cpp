#include "../include/RedisValue.h"
using xredis::RedisValue;

void RedisValue::reset() noexcept{
    switch(this->value_type)
    {
    case RedisValue::TYPE::ARRAY         : { delete array; break; }
    case RedisValue::TYPE::PUSH          : { delete push; break; }
    case RedisValue::TYPE::MAP           : { delete mapv; break; }
    case RedisValue::TYPE::SET           : { delete setv; break; }

    case RedisValue::TYPE::SIMPLE_STRING : { delete simple_string; break; }
    case RedisValue::TYPE::SIMPLE_ERROR  : { delete simple_error; break; }
    case RedisValue::TYPE::BULK_STRING   : { delete bulk_string; break; }
    case RedisValue::TYPE::BIG_NUMBER    : { delete big_number; break; }
    case RedisValue::TYPE::BULK_ERROR    : { delete bulk_error; break; }
    case RedisValue::TYPE::VERBATIM_STRING : { delete verbatim_string; break; }

    case RedisValue::TYPE::ATTRIBUTE     : { delete attribute; break;}

    default:{ break; }
    }
}

std::string_view RedisValue::as_string() const noexcept{
    switch (this->value_type) {
    case RedisValue::TYPE::SIMPLE_STRING:   return *simple_string;
    case RedisValue::TYPE::BULK_STRING:     return *bulk_string;
    case RedisValue::TYPE::VERBATIM_STRING: return *verbatim_string;
    default: return {};
    }
}

std::string_view RedisValue::as_error() const noexcept{
    switch (this->value_type){
    case RedisValue::SIMPLE_ERROR: return *simple_error;
    case RedisValue::BULK_ERROR: return *bulk_error;
    default: return {};
    }
}

static void escape_string(std::string& out, const std::string_view in) noexcept {
    out.reserve(out.size() + in.size() + 8);

    for(unsigned char ch : in){
        switch(ch){
        case '\"': out.append("\\\"", 2); break;
        case '\\': out.append("\\\\", 2); break;
        case '\b': out.append("\\b", 2);  break;
        case '\f': out.append("\\f", 2);  break;
        case '\n': out.append("\\n", 2);  break;
        case '\r': out.append("\\r", 2);  break;
        case '\t': out.append("\\t", 2);  break;
        default:{
            if(ch < 0x20){
                static constexpr char hex[] = "0123456789abcdef";
                out.append("\\u00", 4);
                out.push_back(hex[ch >> 4]);
                out.push_back(hex[ch & 0xF]);
            }
            else{
                out.push_back(ch);
            }
        }
        }
    }
}

void RedisValue::dump_recursion(const RedisValue& value, std::string& str) noexcept{
    switch(value.value_type){
    case RedisValue::TYPE::NULLPTR:{
        str.append("null", 4);
        break;
    }
    case RedisValue::TYPE::INTEGER: {
        str += std::to_string(value.integer);
        break;
    }
    case RedisValue::TYPE::DOUBLE: {
        str += std::to_string(value.doublev);
        break;
    }
    case RedisValue::TYPE::BOOL: {
        std::string_view boolean = value.boolean ? "true" : "false";
        str += boolean;
        break;
    }
    case RedisValue::TYPE::SIMPLE_STRING:{
        str.push_back('\"');
        escape_string(str, *value.simple_string);
        str.push_back('\"');
        break;
    }
    case RedisValue::TYPE::SIMPLE_ERROR:{
        str.push_back('\"');
        escape_string(str, *value.simple_error);
        str.push_back('\"');
        break;
    }
    case RedisValue::TYPE::BULK_STRING:{
        str.push_back('\"');
        escape_string(str, *value.bulk_string);
        str.push_back('\"');
        break;
    }
    case RedisValue::TYPE::BIG_NUMBER:{
        str.push_back('\"');
        escape_string(str, *value.big_number);
        str.push_back('\"');
        break;
    }
    case RedisValue::TYPE::BULK_ERROR:{
        str.push_back('\"');
        escape_string(str, *value.bulk_error);
        str.push_back('\"');
        break;
    }
    case RedisValue::TYPE::VERBATIM_STRING:{
        str.push_back('\"');
        escape_string(str, *value.verbatim_string);
        str.push_back('\"');
        break;
    }
    case RedisValue::TYPE::ARRAY:{
        if(value.array->empty()){
            str.append("[]", 2);
        }
        else{
            str.push_back('[');
            auto it = value.array->cbegin();
            for(; it != value.array->cend() - 1; ++it){
                dump_recursion(*it, str);
                str.push_back(',');
            }
            dump_recursion(*it, str);
            str.push_back(']');
        }
        break;
    }
    case RedisValue::TYPE::PUSH:{
        constexpr std::string_view push_str = "{\"type\":\"push\",\"data\":[";
        str += push_str;
        if(value.push->empty()){
            str.append("]}", 2);
        }
        else{
            auto it = value.push->cbegin();
            for(; it != value.push->cend() - 1; ++it){
                dump_recursion(*it, str);
                str.push_back(',');
            }
            dump_recursion(*it, str);
            str.append("]}", 2);
        }
        break;
    }
    case RedisValue::TYPE::SET:{
        constexpr std::string_view set_str = "{\"type\":\"set\",\"data\":[";
        str += set_str;
        if(value.setv->empty()){
            str.append("]}", 2);
        }
        else{
            auto it = value.setv->cbegin();
            for(; it != value.setv->cend() - 1; ++it){
                dump_recursion(*it, str);
                str.push_back(',');
            }
            dump_recursion(*it, str);
            str.append("]}", 2);
        }
        break;
    }
    case RedisValue::TYPE::MAP:{
        constexpr std::string_view map_str = "{\"type\":\"map\",\"data\":[";
        str += map_str;
        if(value.mapv->empty()){
            str.append("]}", 2);
        }
        else{
            auto it = value.mapv->cbegin();
            for(; it != value.mapv->cend() - 1; ++it){
                str.push_back('[');
                dump_recursion(it->first, str); // key
                str.push_back(',');
                dump_recursion(it->second, str); // value
                str.append("],", 2);
            }
            str.push_back('[');
            dump_recursion(it->first, str); // key
            str.push_back(',');
            dump_recursion(it->second, str); // value
            str.append("]]}", 3);
        }
        break;
    }
    case RedisValue::TYPE::ATTRIBUTE:{
        constexpr std::string_view attribute_str = "{\"type\":\"attribute\",\"attributes\":[";
        str += attribute_str;
        const RedisValue::map_t& attbs = value.attribute->first;
        const RedisValue& actual_data = value.attribute->second;
        if(attbs.empty()){
            str.append("],\"data\":", 9);
        }
        else{
            auto it = attbs.cbegin();
            for(; it != attbs.cend() - 1; ++it){
                str.push_back('[');
                dump_recursion(it->first, str); // key
                str.push_back(',');
                dump_recursion(it->second, str); // value
                str.append("],", 2);
            }
            str.push_back('[');
            dump_recursion(it->first, str); // key
            str.push_back(',');
            dump_recursion(it->second, str); // value
            str.append("]],\"data\":", 10);
        }
        dump_recursion(actual_data, str);
        str.push_back('}');
        break;
    }
    }
}

std::string RedisValue::dump() const noexcept{
    std::string str;
    dump_recursion(*this, str);
    return str;
}


RedisValue::RedisValue(RedisValue&& other) noexcept{
    std::memcpy(static_cast<void*>(this), static_cast<const void*>(&other), sizeof(RedisValue));
    other.value_type = TYPE::NULLPTR;
}
RedisValue& RedisValue::operator=(RedisValue&& other) noexcept{
    this->reset();
    std::memcpy(static_cast<void*>(this), static_cast<const void*>(&other), sizeof(RedisValue));
    other.value_type = RedisValue::TYPE::NULLPTR;
    return *this;
}

// get && set
template<>
RedisValue::null_t& RedisValue::get<RedisValue::null_t>() noexcept {
    return null;
}
template<>
RedisValue::simple_string_t& RedisValue::get<RedisValue::simple_string_t>() noexcept {
    return *simple_string;
}
template<>
RedisValue::simple_error_t& RedisValue::get<RedisValue::simple_error_t>() noexcept {
    return *simple_error;
}
template<>
RedisValue::integer_t& RedisValue::get<RedisValue::integer_t>() noexcept {
    return integer;
}
template<>
RedisValue::bulk_string_t& RedisValue::get<RedisValue::bulk_string_t>() noexcept {
    return *bulk_string;
}
template<>
RedisValue::array_t& RedisValue::get<RedisValue::array_t>() noexcept {
    return *array;
}
template<>
RedisValue::bool_t& RedisValue::get<RedisValue::bool_t>() noexcept {
    return boolean;
}
template<>
RedisValue::double_t& RedisValue::get<RedisValue::double_t>() noexcept {
    return doublev;
}
template<>
RedisValue::big_number_t& RedisValue::get<RedisValue::big_number_t>() noexcept {
    return *big_number;
}
template<>
RedisValue::bulk_error_t& RedisValue::get<RedisValue::bulk_error_t>() noexcept {
    return *bulk_error;
}
template<>
RedisValue::verbatim_string_t& RedisValue::get<RedisValue::verbatim_string_t>() noexcept {
    return *verbatim_string;
}
template<>
RedisValue::map_t& RedisValue::get<RedisValue::map_t>() noexcept {
    return *mapv;
}
template<>
RedisValue::set_t& RedisValue::get<RedisValue::set_t>() noexcept {
    return *setv;
}
template<>
RedisValue::push_t& RedisValue::get<RedisValue::push_t>() noexcept {
    return *push;
}
template<>
RedisValue::attribute_t& RedisValue::get<RedisValue::attribute_t>() noexcept{
    return *attribute;
}