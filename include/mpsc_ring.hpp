#ifndef mpsc_ring_hpp
#define mpsc_ring_hpp
#include<string>
#include<string_view>
#include<coroutine>
#include<atomic>
#include<type_traits>
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
    #include <immintrin.h>
    #define XREDIS_CPU_RELAX() _mm_pause()
#elif defined(__aarch64__) || defined(__arm__)
    #define XREDIS_CPU_RELAX() asm volatile("yield" ::: "memory")
#else
    #include <thread>
    #define XREDIS_CPU_RELAX() std::this_thread::yield()
#endif

namespace xredis{
    namespace details{
        // special MPSCRing for redis only as the consumer will only consume the ring after writing
        template<bool shared, size_t xcapacity>
        class MPSCRing{
            struct shared_ring{
                alignas(64) std::atomic_size_t head;
                std::atomic_size_t head_committed;
                alignas(64) std::atomic_size_t tail_allocated;
                std::atomic_size_t tail;
                alignas(64) std::coroutine_handle<> handles[xcapacity];
                alignas(64) std::string_view payloads[xcapacity];
            };

            struct noshared_ring{
                size_t head;
                size_t head_committed;
                size_t tail;
                std::coroutine_handle<> handles[xcapacity];
                std::string_view payloads[xcapacity];
            };

            using atomic_type = std::conditional_t<shared, std::atomic<size_t>, size_t>;
            using ring_type = std::conditional_t<shared, shared_ring, noshared_ring>;
            static constexpr size_t mask = xcapacity - 1;
            static constexpr size_t STOP_BIT = (1ULL << 63);
            
            ring_type ring;

            static consteval bool is_power_of_two(size_t x) noexcept {
                return x && ((x & (x - 1)) == 0);
            }

            template<bool s, typename std::enable_if<s, bool>::type = true>
            int _push_n(std::coroutine_handle<>* handles, std::string_view* payloads, size_t count) noexcept{
                while(true){
                    size_t t_alloc = this->ring.tail_allocated.load(std::memory_order_relaxed);
                    size_t head = this->ring.head.load(std::memory_order_acquire);

                    if(t_alloc & STOP_BIT){
                        return -2;
                    }
                    if(t_alloc - head >= xcapacity){
                        return -1;
                    }
                    if(!this->ring.tail_allocated.compare_exchange_weak(t_alloc, t_alloc + count, std::memory_order_relaxed)){
                        continue;
                    }
                    size_t pos = t_alloc & mask;
                    for(size_t i = 0; i < count; i++){
                        size_t idx = (pos + i) & mask;
                        this->ring.handles[idx] = handles[i];
                        this->ring.payloads[idx] = payloads[i];
                    }
                    while (this->ring.tail.load(std::memory_order_acquire) != t_alloc) {
                        XREDIS_CPU_RELAX();
                    }
                    this->ring.tail.store(t_alloc + count, std::memory_order_release);
                    return static_cast<int>(pos);
                }
            }
            template<bool s, typename std::enable_if<!s, bool>::type = true>
            int _push_n(std::coroutine_handle<>* handles, std::string_view* payloads, size_t count) noexcept{
                if(this->ring.tail & STOP_BIT){
                    return -2;
                }
                if(this->ring.tail - this->ring.head >= xcapacity){
                    return -1;
                }
                size_t pos = this->ring.tail & mask;
                this->ring.tail += count;
                for(size_t i = 0; i < count; i++){
                    size_t idx = (pos + i) & mask;
                    this->ring.handles[idx] = handles[i];
                    this->ring.payloads[idx] = payloads[i];
                }
                return static_cast<int>(pos);
            }

            template<bool s, typename std::enable_if<s, bool>::type = true>
            int _push(std::coroutine_handle<> h, std::string_view sv) noexcept{
                while(true){
                    size_t t_alloc = this->ring.tail_allocated.load(std::memory_order_relaxed);
                    size_t head = this->ring.head.load(std::memory_order_acquire);

                    if(t_alloc & STOP_BIT){
                        return -2;
                    }
                    if(t_alloc - head >= xcapacity){
                        return -1;
                    }
                    if(!this->ring.tail_allocated.compare_exchange_weak(t_alloc, t_alloc + 1, std::memory_order_relaxed)){
                        continue;
                    }
                    size_t pos = t_alloc & mask;
                    this->ring.handles[pos] = h;
                    this->ring.payloads[pos] = sv;
                    while (this->ring.tail.load(std::memory_order_acquire) != t_alloc) {
                        XREDIS_CPU_RELAX();
                    }
                    this->ring.tail.store(t_alloc + 1, std::memory_order_release);
                    return static_cast<int>(pos);
                }
            }
            template<bool s, typename std::enable_if<s, bool>::type = true>
            void _pop(std::coroutine_handle<>& h, std::string_view& sv) noexcept{
                size_t h_val = this->ring.head.load(std::memory_order_relaxed);
                size_t pos = h_val & mask;
                h = this->ring.handles[pos];
                sv = this->ring.payloads[pos];
                this->ring.head.store(h_val + 1, std::memory_order_release);
            }

            template<bool s, typename std::enable_if<!s, bool>::type = true>
            int _push(std::coroutine_handle<> h, std::string_view sv) noexcept{
                if(this->ring.tail & STOP_BIT){
                    return -2;
                }
                if(this->ring.tail - this->ring.head >= xcapacity){
                    return -1;
                }
                size_t pos = this->ring.tail & mask;
                ++this->ring.tail;
                this->ring.handles[pos] = h;
                this->ring.payloads[pos] = sv;
                return static_cast<int>(pos);
            }
            template<bool s, typename std::enable_if<!s, bool>::type = true>
            void _pop(std::coroutine_handle<>& h, std::string_view& sv) noexcept{
                size_t pos = this->ring.head & mask;            
                ++this->ring.head;
                h = this->ring.handles[pos];
                sv = this->ring.payloads[pos];
            }

            template<class T, bool s, typename std::enable_if<s, bool>::type = true>
            size_t _prep_writev(T* iov, size_t& num_evs) noexcept{
                size_t start = this->ring.head_committed.load(std::memory_order_acquire);
                size_t end = this->ring.tail.load(std::memory_order_acquire);
                num_evs = (end - start) < num_evs ? (end - start) : num_evs;
                this->ring.head_committed.fetch_add(num_evs, std::memory_order_release);
                size_t total_bytes = 0;
                for(size_t i = 0; i < num_evs; i++){
                    std::string_view& payload = this->ring.payloads[(start + i) & mask];
                    iov[i].iov_base = (void*)(payload.data());
                    iov[i].iov_len = payload.size();
                    total_bytes += payload.size();
                }
                return total_bytes;
            }

            template<class T, bool s, typename std::enable_if<!s, bool>::type = true>
            size_t _prep_writev(T* iov, size_t& num_evs) noexcept{
                size_t start = this->ring.head_committed;
                size_t end = this->ring.tail;
                num_evs = (end - start) < num_evs ? (end - start) : num_evs;
                this->ring.head_committed += num_evs;
                size_t total_bytes = 0;
                for(size_t i = 0; i < num_evs; i++){
                    std::string_view& payload = this->ring.payloads[(start + i) & mask];
                    iov[i].iov_base = (void*)(payload.data());
                    iov[i].iov_len = payload.size();
                    total_bytes += payload.size();
                }
                return total_bytes;
            }

            template<bool s, typename std::enable_if<s, bool>::type = true>
            void _drain(xredis::RedisValue& result_slot) noexcept{
                size_t old = this->ring.tail_allocated.fetch_or(STOP_BIT, std::memory_order_acq_rel);
                while(this->ring.tail.load() != old){
                    XREDIS_CPU_RELAX();
                }
                size_t head = this->ring.head.load(std::memory_order_relaxed);
                for(size_t i = head; i != old; i++){
                    xredis::RedisValue error;
                    error.set<xredis::RedisValue::simple_error_t>(std::string_view("CONN ERR"));
                    result_slot = std::move(error);
                    this->ring.handles[i & mask].resume();
                }
            }
            template<bool s, typename std::enable_if<!s, bool>::type = true>
            void _drain(xredis::RedisValue& result_slot) noexcept{
                size_t tail = this->ring.tail;
                this->ring.tail |= STOP_BIT;
                for(size_t i = this->ring.head; i != tail; i++){
                    xredis::RedisValue error;
                    error.set<xredis::RedisValue::simple_error_t>(std::string_view("CONN ERR"));
                    result_slot = std::move(error);
                    this->ring.handles[i & mask].resume();
                }
            }

            template<bool s, typename std::enable_if<s, bool>::type = true>
            void _reset() noexcept{
                this->ring.head.store(0, std::memory_order_relaxed);
                this->ring.head_committed.store(0, std::memory_order_relaxed);
                this->ring.tail.store(0, std::memory_order_relaxed);
                // last
                this->ring.tail_allocated.store(0, std::memory_order_release);
            }
            template<bool s, typename std::enable_if<!s, bool>::type = true>
            void _reset() noexcept{
                this->ring.head = 0;
                this->ring.head_committed = 0;
                this->ring.tail = 0;
            }

        public:
            static constexpr size_t capacity = xcapacity; 

            MPSCRing() noexcept: ring(){
                static_assert(MPSCRing::is_power_of_two(capacity), "MPSCRing::MPSCRing() the capacity must be the power of two");
            }

            ~MPSCRing() = default;

            int push(std::coroutine_handle<> h, std::string_view sv) noexcept{
                return this->_push<shared>(h, sv);
            }
            void pop(std::coroutine_handle<>& h, std::string_view& sv) noexcept{
                return this->_pop<shared>(h, sv);
            }

            int push_n(std::coroutine_handle<>* handles, std::string_view* payloads, size_t count) noexcept{
                return this->_push_n<shared>(handles, payloads, count);
            }

            void drain(xredis::RedisValue& result_slot) noexcept{
                return this->_drain<shared>(result_slot);
            }

            void reset() noexcept{
                return this->_reset<shared>();
            }

            template<class T>
            size_t prep_writev(T* iov, size_t& size) noexcept{
                return this->_prep_writev<T, shared>(iov, size);
            }
        };
    }
}


#endif