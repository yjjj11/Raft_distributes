#!/bin/bash
# 定义日志目录（和你的程序日志目录保持一致）
LOG_DIR="./logs"
# 创建日志目录（避免文件无法写入）
mkdir -p ${LOG_DIR}

# 启动 Node 0：重定向日志 + 控制台输出 PID
./bin/$main 0 127.0.0.1 8000  2 127.0.0.1:8001  > ${LOG_DIR}/node0.log 2>&1 &
NODE0_PID=$!  # 获取上一个后台进程的PID
echo "Node 0 started (PID: ${NODE0_PID}, log: ${LOG_DIR}/node0.log)"

# 启动 Node 1：同理输出 PID
./bin/$main 1 127.0.0.1 8001 6 127.0.0.1:8000   > ${LOG_DIR}/node1.log 2>&1 &
NODE1_PID=$!
echo "Node 1 started (PID: ${NODE1_PID}, log: ${LOG_DIR}/node1.log)"