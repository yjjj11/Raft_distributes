#!/bin/bash

# 启动分布式锁集群
LOG_DIR="./logs"

# 创建日志目录
mkdir -p $LOG_DIR

# 启动三个节点
echo "启动分布式锁节点 0..."
./bin/distributed_lock 0 127.0.0.1 8000 1 127.0.0.1:8001 127.0.0.1:8002 &
NODE0_PID=$!

# 等待一下再启动其他节点
sleep 1

echo "启动分布式锁节点 1..."
./bin/distributed_lock 1 127.0.0.1 8001 2 127.0.0.1:8000 127.0.0.1:8002 &
NODE1_PID=$!

sleep 1

echo "启动分布式锁节点 2..."
./bin/distributed_lock 2 127.0.0.1 8002 2 127.0.0.1:8000 127.0.0.1:8001 &
NODE2_PID=$!

echo "分布式锁集群启动完成！"
echo "节点 0 PID: $NODE0_PID"
echo "节点 1 PID: $NODE1_PID"
echo "节点 2 PID: $NODE2_PID"
echo "日志文件位于: $LOG_DIR/"
echo "按 Ctrl+C 停止所有节点"

# 等待用户输入
wait $NODE0_PID $NODE1_PID $NODE2_PID
