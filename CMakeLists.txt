cmake_minimum_required(VERSION 3.13)
project(external_demo)

set(CMAKE_BUILD_TYPE Debug)

# 2. 强制开启调试信息（即使不小心设为 Release 也生效）
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g")

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-w)  # -w 是屏蔽所有警告，-Wall 是开启所有警告，-Wno-xxx 是屏蔽特定警告
endif()

add_definitions(-DASIO_STANDALONE -DRPC_USE_LOG -D_LOG_CONSOLE)

# add_definitions(-DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_OFF)

# ========== 核心指令：强制生成compile_commands.json ==========
# FORCE参数确保覆盖任何已有配置，每次cmake都生成
set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE BOOL "Generate compile_commands.json for clangd" FORCE)
# 可选：将compile_commands.json软链接到项目根目录（方便clangd识别）
execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink
    ${CMAKE_BINARY_DIR}/compile_commands.json
    ${CMAKE_SOURCE_DIR}/compile_commands.json
    RESULT_VARIABLE symlink_result
    ERROR_QUIET
)

# 关键修复：主动查找线程库（必须在target_link_libraries之前）
find_package(Threads REQUIRED)

# 添加 Asio_mrpc 子目录，递归编译库
add_subdirectory(Asio_mrpc)

# 设置可执行文件输出路径
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_HOME_DIRECTORY}/bin/)

# 包含库的头文件目录
include_directories(
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/include
    ${RPC_INCLUDE_DIR}      # 库的头文件目录（从子目录继承）
    ${RPC_INCLUDE_DIR}/mrpc # 新增：适配client.hpp在mrpc子目录的情况
    ${ASIO_INCLUDE_DIR}     # Asio 头文件目录（从子目录继承）
    ${SPDLOG_INCLUDE_DIR}   # spdlog 头文件目录（从子目录继承）
    ${WLOG_INCLUDE_DIR}     # wlog 头文件目录（从子目录继承）
    ${NLOHMANN_INCLUDE_DIR} # nlohmann/json 头文件目录（从子目录继承）
)
message(STATUS "MRPC_SRC_FILES in main: ${MRPC_SRC_FILES}")
foreach(file ${MRPC_SRC_FILES})
    message(STATUS "  - ${file}")
endforeach()

add_executable(node
     example/node.cpp
     raftnode.cpp
     ${MRPC_SRC_FILES} 
)

target_link_libraries(node 
    PRIVATE  # 推荐添加PRIVATE，明确依赖作用域
    ${ZK_LIB}           # ZooKeeper 库（从子目录继承）
    Threads::Threads    # 现在能找到这个目标了
)
# # # 创建可执行文件
add_executable(kv_store
     example/kv_store.cpp
     raftnode.cpp
     ${MRPC_SRC_FILES} 
)

target_link_libraries(kv_store 
    PRIVATE  # 推荐添加PRIVATE，明确依赖作用域
    ${ZK_LIB}           # ZooKeeper 库（从子目录继承）
    Threads::Threads    # 现在能找到这个目标了
)

add_executable(distributed_lock
     example/distributed_lock.cpp
     raftnode.cpp
     ${MRPC_SRC_FILES} 
)

target_link_libraries(distributed_lock 
    PRIVATE  # 推荐添加PRIVATE，明确依赖作用域
    ${ZK_LIB}           # ZooKeeper 库（从子目录继承）
    Threads::Threads    # 现在能找到这个目标了
)

add_executable(config_center
     example/config_center.cpp
     raftnode.cpp
     ${MRPC_SRC_FILES} 
)

target_link_libraries(config_center 
    PRIVATE  # 推荐添加PRIVATE，明确依赖作用域
    ${ZK_LIB}           # ZooKeeper 库（从子目录继承）
    Threads::Threads    # 现在能找到这个目标了
)

add_executable(test_callback
     example/test_callback.cpp
     raftnode.cpp
     ${MRPC_SRC_FILES} 
)

target_link_libraries(test_callback 
    PRIVATE  # 推荐添加PRIVATE，明确依赖作用域
    ${ZK_LIB}           # ZooKeeper 库（从子目录继承）
    Threads::Threads    # 现在能找到这个目标了
)

# add_executable(task_scheduler
#      example/task_scheduler.cpp
#      raftnode.cpp
#      ${MRPC_SRC_FILES} 
# )

# target_link_libraries(task_scheduler 
#     PRIVATE  # 推荐添加PRIVATE，明确依赖作用域
#     ${ZK_LIB}           # ZooKeeper 库（从子目录继承）
#     Threads::Threads    # 现在能找到这个目标了
# )
