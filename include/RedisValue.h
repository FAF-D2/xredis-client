#ifndef redis_value_h
#define redis_value_h
#include<cstring>
#include<string>
#include<string_view>
#include<type_traits>
#include<utility>
#include<vector>
#include<cstdint>
#include<cstddef>
#include<string_view>

namespace xredis{
    struct RedisClientError{
        static inline std::string_view RING_OVERFLOW = "RING OVERFLOW";
        static inline std::string_view CONN_ERR = "CONN ERR";
        static inline std::string_view OP_CANCELLED = "OP CANCELLED";
        static inline std::string_view CROSS_SLOT = "CROSS SLOT";
    };


    class RedisValue{
    public:
        enum TYPE: int{
            NULLPTR = 0x00000000,
            // vector
            ARRAY, PUSH,
            // map
            SET, MAP,
            // string
            SIMPLE_STRING, SIMPLE_ERROR, BULK_STRING, 
            BIG_NUMBER, BULK_ERROR, VERBATIM_STRING,
            // basic
            INTEGER, BOOL, DOUBLE,
            // attribute
            ATTRIBUTE
        };

        RedisValue() noexcept: null(nullptr), value_type(TYPE::NULLPTR){}
        RedisValue(const RedisValue&) = delete;
        RedisValue& operator=(const RedisValue&) = delete;
        RedisValue(RedisValue&&) noexcept;
        RedisValue& operator=(RedisValue&&) noexcept;
        ~RedisValue() noexcept { this->reset(); }

        // --- type system ---
        template<size_t n>
        struct string_type: public std::string{
            using std::string::string;
        };
        template<size_t n>
        struct vector_type: public std::vector<RedisValue>{
            using std::vector<RedisValue>::vector;
        };

        // --- basic ---
        using null_t = std::nullptr_t;
        using integer_t = int64_t;
        using bool_t    = bool;
        using double_t  = double;

        // --- string ---
        using simple_string_t    = string_type<0>;
        using simple_error_t     = string_type<1>;
        using bulk_string_t      = string_type<2>;
        using big_number_t       = string_type<3>;
        using bulk_error_t       = string_type<4>;
        using verbatim_string_t  = string_type<5>;

        // --- container ---
        using array_t = vector_type<0>;
        using push_t  = vector_type<1>;
        using set_t = vector_type<2>;
        using map_t = std::vector<std::pair<RedisValue, RedisValue>>;
        using attribute_t = std::pair<map_t, RedisValue>;

        template<class T>
        T& get() noexcept;
        template<class T>
        T& as() noexcept { return this->get<T>(); }
        std::string_view as_string() const noexcept;
        std::string_view as_error() const noexcept;
        // for debug (json string)
        std::string dump() const noexcept;

        int type() const noexcept { return this->value_type; }
        bool is_nullptr() const noexcept { return this->value_type == RedisValue::TYPE::NULLPTR; }
        bool is_array() const noexcept { return this->value_type == RedisValue::TYPE::ARRAY; }
        bool is_push() const noexcept { return this->value_type == RedisValue::TYPE::PUSH; }
        bool is_simple_string() const noexcept { return this->value_type == RedisValue::TYPE::SIMPLE_STRING; }
        bool is_simple_error() const noexcept { return this->value_type == RedisValue::TYPE::SIMPLE_ERROR; }
        bool is_bulk_string() const noexcept { return this->value_type == RedisValue::TYPE::BULK_STRING; }
        bool is_big_number() const noexcept { return this->value_type == RedisValue::TYPE::BIG_NUMBER; }
        bool is_bulk_error() const noexcept { return this->value_type == RedisValue::TYPE::BULK_ERROR; }
        bool is_verbatim_string() const noexcept { return this->value_type == RedisValue::TYPE::VERBATIM_STRING; }
        bool is_map() const noexcept { return this->value_type == RedisValue::TYPE::MAP; }
        bool is_set() const noexcept { return this->value_type == RedisValue::TYPE::SET; }
        bool is_integer() const noexcept { return this->value_type == RedisValue::TYPE::INTEGER; }
        bool is_bool() const noexcept { return this->value_type == RedisValue::TYPE::BOOL; }
        bool is_double() const noexcept { return this->value_type == RedisValue::TYPE::DOUBLE; }
        bool is_attribute() const noexcept { return this->value_type == RedisValue::TYPE::ATTRIBUTE; }
        bool is_string_type() const noexcept { return is_simple_string() || is_bulk_string() || is_verbatim_string(); }
        bool is_error() const noexcept { return is_simple_error() || is_bulk_error(); }
        bool error() const noexcept { return is_simple_error() || is_bulk_error(); }

        template<class T, class V>
        void set(V&& value) noexcept{
            this->reset();
            this->_set<T>(std::forward<V>(value));
        }

    private:
        static void dump_recursion(const RedisValue& value, std::string& str) noexcept;
        void reset() noexcept;

        template<class T, class V>
        void _set(V&& value) noexcept{
            constexpr bool match_types = std::is_same_v<T, null_t> || std::is_same_v<T, integer_t>
                                    ||  std::is_same_v<T, bool_t> || std::is_same_v<T, double_t>
                                    ||  std::is_same_v<T, simple_string_t> || std::is_same_v<T, simple_error_t>
                                    ||  std::is_same_v<T, bulk_string_t> ||std::is_same_v<T, big_number_t>
                                    ||  std::is_same_v<T, bulk_error_t> || std::is_same_v<T, verbatim_string_t>
                                    ||  std::is_same_v<T, map_t> || std::is_same_v<T, set_t>
                                    ||  std::is_same_v<T, array_t> || std::is_same_v<T, push_t>
                                    ||  std::is_same_v<T, attribute_t>;
            static_assert(match_types, "RedisValue::set<T>(value): T is not a support type");

            if constexpr(std::is_same_v<T, null_t>){
                this->null = value;
                this->value_type = RedisValue::TYPE::NULLPTR;
                return;
            }
            if constexpr(std::is_same_v<T, integer_t>){
                this->integer = value;
                this->value_type = RedisValue::TYPE::INTEGER;
                return;
            }
            if constexpr(std::is_same_v<T, bool_t>){
                this->boolean = value;
                this->value_type = RedisValue::TYPE::BOOL;
                return;
            }
            if constexpr(std::is_same_v<T, double_t>){
                this->doublev = value;
                this->value_type = RedisValue::TYPE::DOUBLE;
                return;
            }

            if constexpr(std::is_same_v<T, simple_string_t>){
                this->simple_string = new simple_string_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::SIMPLE_STRING;
                return;
            }
            if constexpr(std::is_same_v<T, simple_error_t>){
                this->simple_error = new simple_error_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::SIMPLE_ERROR;
                return;
            }
            if constexpr(std::is_same_v<T, bulk_string_t>){
                this->bulk_string = new bulk_string_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::BULK_STRING;
                return;
            }
            if constexpr(std::is_same_v<T, big_number_t>){
                this->big_number = new big_number_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::BIG_NUMBER;
                return;
            }
            if constexpr(std::is_same_v<T, bulk_error_t>){
                this->bulk_error = new bulk_error_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::BULK_ERROR;
                return;
            }
            if constexpr(std::is_same_v<T, verbatim_string_t>){
                this->verbatim_string = new verbatim_string_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::VERBATIM_STRING;
                return;
            }

            if constexpr(std::is_same_v<T, map_t>){
                this->mapv = new map_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::MAP;
                return;
            }
            if constexpr(std::is_same_v<T, set_t>){
                this->setv = new set_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::SET;
                return;
            }
            if constexpr(std::is_same_v<T, array_t>){
                this->array = new array_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::ARRAY;
                return;
            }
            if constexpr(std::is_same_v<T, push_t>){
                this->push = new push_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::PUSH;
                return;
            }
            if constexpr(std::is_same_v<T, attribute_t>){
                this->attribute = new attribute_t(std::forward<V>(value));
                this->value_type = RedisValue::TYPE::ATTRIBUTE;
                return;
            }
        }

        union
        {
            null_t null;
            simple_string_t* simple_string;
            simple_error_t* simple_error;
            integer_t integer;
            bulk_string_t* bulk_string;
            array_t* array;
            bool_t boolean;
            double_t doublev;
            big_number_t* big_number;
            bulk_error_t* bulk_error;
            verbatim_string_t* verbatim_string;
            map_t* mapv;
            set_t* setv;
            push_t* push;
            attribute_t* attribute;
        };

        int value_type;
    };
}

#endif