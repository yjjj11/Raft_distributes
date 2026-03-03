#!/bin/bash

# ===================== 配置项 =====================
# 日志目录（和程序日志目录保持一致）
LOG_DIR="./logs"
# 节点配置（ID IP 端口 选举超时时间 节点列表）
NODE_CONFIGS=(
    "0 127.0.0.1 8000 1 127.0.0.1:8001 127.0.0.1:8002"
    "1 127.0.0.1 8001 2 127.0.0.1:8000 127.0.0.1:8002"
    "2 127.0.0.1 8002 2 127.0.0.1:8001 127.0.0.1:8000"
)
# 程序执行文件路径
BIN_FILE="./bin/node"
# ===================== 全局变量 =====================
# 存储节点 PID（key: 节点序号, value: PID）
declare -A NODE_PIDS
# 脚本运行标识
RUNNING=true

# ===================== 工具函数 =====================
# 检查节点是否存在
check_node_exists() {
    local node_id=$1
    if [[ ! ${NODE_CONFIGS[$node_id]+_} ]]; then
        echo "错误：节点 $node_id 不存在！有效节点序号：0/1/2"
        return 1
    fi
    return 0
}

# 检查节点是否运行
check_node_running() {
    local node_id=$1
    if [[ -z ${NODE_PIDS[$node_id]} || ! $(ps -p ${NODE_PIDS[$node_id]} -o pid=) ]]; then
        echo "提示：节点 $node_id 未运行"
        return 1
    fi
    return 0
}

# 启动单个节点
start_node() {
    local node_id=$1
    local log_file="logs/node_${node_id}.log"
    # 检查节点配置是否存在
    if ! check_node_exists $node_id; then
        return 1
    fi
    
    # 如果节点已运行，先停止
    if check_node_running $node_id; then
        echo "提示：节点 $node_id 已运行（PID: ${NODE_PIDS[$node_id]}），先停止..."
        kill_node $node_id > /dev/null 2>&1
    fi

    # 解析节点配置
    local config=(${NODE_CONFIGS[$node_id]})
    local ip=${config[1]}
    local port=${config[2]}
    local election_time=${config[3]}
    local peers="${config[4]} ${config[5]}"

    # 启动节点，重定向日志
    echo "启动节点 $node_id ..."
    $BIN_FILE $node_id $ip $port $election_time $peers >> "$log_file" 2>&1 &
    local pid=$!
    NODE_PIDS[$node_id]=$pid

    # 验证启动是否成功
    sleep 0.5
    if check_node_running $node_id; then
        echo "✅ 节点 $node_id 启动成功（PID: $pid，日志：${LOG_DIR}/node${node_id}.log）"
    else
        echo "❌ 节点 $node_id 启动失败！"
        unset NODE_PIDS[$node_id]
        return 1
    fi
    return 0
}

# 停止单个节点
kill_node() {
    local node_id=$1
    if ! check_node_exists $node_id; then
        return 1
    fi

    # 如果节点未运行，直接返回
    if ! check_node_running $node_id; then
        return 0
    fi

    # 强制杀死节点
    local pid=${NODE_PIDS[$node_id]}
    echo "停止节点 $node_id（PID: $pid）..."
    kill -9 $pid > /dev/null 2>&1
    wait $pid 2>/dev/null

    # 清理 PID 记录
    unset NODE_PIDS[$node_id]
    echo "✅ 节点 $node_id 已停止"
    return 0
}

# 重启单个节点
restart_node() {
    local node_id=$1
    echo "===== 重启节点 $node_id ====="
    kill_node $node_id
    sleep 0.5
    start_node $node_id
    echo "==========================="
}

# 显示菜单
show_menu() {
    echo -e "\n====================================="
    echo "          Raft 节点管理工具"
    echo "====================================="
    echo "当前节点状态："
    for node_id in 0 1 2; do
        if check_node_running $node_id; then
            echo "  节点 $node_id：✅ 运行中（PID: ${NODE_PIDS[$node_id]}）"
        else
            echo "  节点 $node_id：❌ 已停止"
        fi
    done
    echo -e "=====================================\n"
    echo "请选择操作："
    echo "  1. kill   - 停止指定节点"
    echo "  2. restart - 重启指定节点"
    echo "  3. exit   - 退出脚本（所有节点会被停止）"
    echo -n "输入操作序号（1/2/3）："
}

# 处理用户输入
handle_input() {
    local choice
    read choice

    case $choice in
        1)
            # Kill 操作
            echo -n "请输入要停止的节点序号（0/1/2）："
            local node_id
            read node_id
            kill_node $node_id
            ;;
        2)
            # Restart 操作
            echo -n "请输入要重启的节点序号（0/1/2）："
            local node_id
            read node_id
            restart_node $node_id
            ;;
        3)
            # Exit 操作
            echo "退出脚本，停止所有节点..."
            RUNNING=false
            ;;
        *)
            echo "❌ 无效的选择！请输入 1/2/3"
            ;;
    esac
}

# 清理函数（脚本退出时停止所有节点）
cleanup() {
    echo -e "\n===== 清理资源 ====="
    for node_id in 0 1 2; do
        kill_node $node_id > /dev/null 2>&1
    done
    echo "所有节点已停止，脚本退出！"
}

# ===================== 主程序 =====================
# 1. 初始化日志目录
mkdir -p ${LOG_DIR}
echo "日志目录：$LOG_DIR"

# 2. 一次性启动所有节点
echo -e "\n===== 启动所有 Raft 节点 ====="
for node_id in 0 1 2; do
    start_node $node_id
done

# 3. 设置退出清理函数
trap cleanup EXIT

# 4. 交互式循环
echo -e "\n===== 进入交互式管理模式 ====="
while $RUNNING; do
    show_menu
    handle_input
    # 操作完成后等待用户按回车继续
    if $RUNNING; then
        echo -n "按回车键继续..."
        read
    fi
done