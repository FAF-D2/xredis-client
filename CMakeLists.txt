cmake_minimum_required(VERSION 3.20)
project(xredis-client LANGUAGES CXX)

option(XREDIS_ENABLE_TLS "Enable TLS support" OFF)
option(XNET_DISABLE_THREAD_SAFE "DISABLE xnet thread-safe mode" OFF)
option(XREDIS_ENABLE_SANITY "Enable ASAN and UBSAN for testing" OFF)
option(XREDIS_TEST_HEADERONLY "Test Header only Complie" OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O2 -fno-exceptions -fno-rtti")

include(CheckIPOSupported)
check_ipo_supported(RESULT lto_supported OUTPUT lto_error)

file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS 
    ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp
)

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    file(GLOB_RECURSE TEST_FILES CONFIGURE_DEPENDS 
        ${CMAKE_CURRENT_SOURCE_DIR}/test/*.cpp
    )
endif()

add_library(xredis-client STATIC ${SRC_FILES})

if(lto_supported)
    set_target_properties(xredis-client PROPERTIES INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)
endif()

target_include_directories(xredis-client PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(xredis-client PRIVATE uring)

if (XREDIS_ENABLE_TLS)
    find_package(OpenSSL REQUIRED)
    target_compile_definitions(xredis-client PUBLIC XREDIS_ENABLE_TLS)
    target_link_libraries(xredis-client PUBLIC OpenSSL::SSL OpenSSL::Crypto)
endif()

if (XNET_DISABLE_THREAD_SAFE)
    target_compile_definitions(xredis-client PUBLIC XNET_DISABLE_THREAD_SAFE)
endif()

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    enable_testing()

    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests)

    foreach(test_file ${TEST_FILES})
        get_filename_component(test_name ${test_file} NAME_WE)
        
        add_executable(${test_name} ${test_file})

        if(lto_supported)
            set_target_properties(${test_name} PROPERTIES INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)
        endif()
        
        target_link_libraries(${test_name} PRIVATE xredis-client)
        
        if (XREDIS_ENABLE_SANITY)
            target_compile_options(${test_name} PRIVATE -fsanitize=address -fsanitize=undefined -fno-omit-frame-pointer)
            target_link_options(${test_name} PRIVATE -fsanitize=address -fsanitize=undefined)
            
            target_compile_options(xredis-client PRIVATE -fsanitize=address -fsanitize=undefined -fno-omit-frame-pointer)
            target_link_options(xredis-client PRIVATE -fsanitize=address -fsanitize=undefined)
        endif()

        if (XREDIS_TEST_HEADERONLY)
            target_compile_definitions(${test_name} PRIVATE XREDIS_TEST_HEADERONLY)
            target_compile_definitions(xredis-client PRIVATE XREDIS_TEST_HEADERONLY)
        endif()
        
        add_test(NAME ${test_name} COMMAND ${test_name})

        set_tests_properties(${test_name} PROPERTIES WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
    endforeach()
endif()