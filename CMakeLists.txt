cmake_minimum_required(VERSION 3.13)
project(external_demo)

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

# 创建可执行文件
add_executable(main
     example/main.cpp
     raftnode.cpp
     ${MRPC_SRC_FILES} 
)

target_link_libraries(main 
    PRIVATE  # 推荐添加PRIVATE，明确依赖作用域
    ${ZK_LIB}           # ZooKeeper 库（从子目录继承）
    Threads::Threads    # 现在能找到这个目标了
)