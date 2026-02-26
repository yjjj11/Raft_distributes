#include "raftnode.hpp"
#include <iostream>
#include <random>

RaftNode::RaftNode(int node_id, const std::string& ip, int port, 
                   const std::vector<std::pair<std::string, int>>& peers)
    : node_id_(node_id), ip_(ip), port_(port), peers_(peers),
      server_(mrpc::server::get()), client_(mrpc::client::get()) {
    
    // 初始化next_index和match_index数组
    next_index_.resize(peers_.size());
    match_index_.resize(peers_.size());
    
    // 加载持久化状态
    load_state();
    
    // 设置RPC处理器
    setup_rpc_handlers();
    
    // 连接到其他节点
    connect_to_peers();
    
    // 初始化选举超时计时器
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(300, 600);  // 300-600ms随机选举超时
    last_heartbeat_time_ = std::chrono::steady_clock::now() - 
                          std::chrono::milliseconds(dis(gen));
}

void RaftNode::start() {
    if (running_) return;
    
    running_ = true;
    
    // 启动服务器
    server_.set_ip_port(ip_, port_);
    server_.set_server_name("raft_node_" + std::to_string(node_id_));
    server_.run();
    
    // 启动各个线程
    server_thread_ = std::thread([this]() {
        server_.accept();
    });
    
    // 启动选举超时监控线程
    election_thread_ = std::thread(&RaftNode::run_election_timeout, this);
    
    std::cout << "Raft node " << node_id_ << " started on " << ip_ << ":" << port_ << std::endl;
}

RaftNode::~RaftNode() {
    stop();
}