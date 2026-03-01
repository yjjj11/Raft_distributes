#include "../raftnode.hpp"
#include <iostream>
#include <vector>
#include <string>
#include <logger.hpp>
#include <signal.h>
#include <atomic>
#include "spdlog/sinks/basic_file_sink.h"
// 全局标志，用于通知主线程退出
std::atomic<bool> g_running{true};
static RaftNode* g_raft_node = nullptr;
// 信号处理函数
void signal_handler(int signal) {
    if (g_raft_node) {
        g_raft_node->stop();
    }
    // std::cout << "\nReceived signal " << signal << ", shutting down gracefully..." << std::endl;
    g_running = false;
}
void init_logger(int node_id) {
    std::filesystem::create_directories("logs");
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(
        "logs/raft_node_" + std::to_string(node_id) + ".log", true);
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();

    // 创建 logger
    auto logger = std::make_shared<spdlog::logger>(
        "raft_node_" + std::to_string(node_id),
            spdlog::sinks_init_list{file_sink, console_sink}
    );
    // 关键：给 logger 单独设置格式（时间高亮 + 去掉 logger 名）
    logger->set_pattern("%^[%Y-%m-%d %H:%M:%S.%e]%$ [%l] %v");
    logger->set_level(spdlog::level::debug);
    logger->flush_on(spdlog::level::debug);

    spdlog::set_default_logger(logger);
}
int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cout << "Usage: " << argv[0] << " <node_id> <ip> <port> [peer1_ip:port peer2_ip:port ...]" << std::endl;
        std::cout << "Example: " << argv[0] << " 0 127.0.0.1 8000 127.0.0.1:8001 127.0.0.1:8002" << std::endl;
        return 1;
    }
    
    int node_id = std::stoi(argv[1]);

    init_logger(node_id);
    
    std::string ip = argv[2];
    int port = std::stoi(argv[3]);
    int election_elapsed_time = std::stoi(argv[4])* 1000;
    spdlog::debug("Election timeout: {}ms", election_elapsed_time);
        
    
    // 解析集群节点信息
    std::vector<std::pair<std::string, int>> peers;
    for (int i = 5; i < argc; ++i) {
        std::string peer_addr = argv[i];
        size_t colon_pos = peer_addr.find(':');
        if (colon_pos != std::string::npos) {
            std::string peer_ip = peer_addr.substr(0, colon_pos);
            int peer_port = std::stoi(peer_addr.substr(colon_pos + 1));
            peers.emplace_back(peer_ip, peer_port);
        } else {
            std::cerr << "Invalid peer address format: " << peer_addr << std::endl;
            return 1;
        }
    }

    // spdlog::info("Initializing Raft node {} on {}:{}", node_id, ip, port);
    spdlog::info("Cluster peers:");
    for (const auto& peer : peers) {
        spdlog::info("{}:{}", peer.first, peer.second);
    }

    // 注册信号处理函数，用于捕获 SIGINT (Ctrl+C) 和 SIGTERM
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    try {
        // 创建Raft节点实例
        RaftNode node(node_id, ip, port, peers,election_elapsed_time);
        g_raft_node = &node;
        spdlog::debug("Raft node {} initialized on {}:{}", node_id, ip, port);
        // spdlog::info("Starting Raft node...");
        
        // 启动节点
        node.start_client();
        
        // spdlog::debug("Raft node started successfully!");
        
        // 保持主线程运行，直到收到退出信号
        while (g_running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        spdlog::debug("Raft node shutdown complete.");
        
    } catch (const std::exception& e) {
        spdlog::error("Error starting Raft node: {}", e.what());
        return 1;
    }

    return 0;
}