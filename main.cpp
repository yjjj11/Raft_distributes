#include "raftnode.hpp"
#include <iostream>
#include <vector>
#include <string>
#include <logger.hpp>
int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cout << "Usage: " << argv[0] << " <node_id> <ip> <port> [peer1_ip:port peer2_ip:port ...]" << std::endl;
        std::cout << "Example: " << argv[0] << " 0 127.0.0.1 8000 127.0.0.1:8001 127.0.0.1:8002" << std::endl;
        return 1;
    }

    int node_id = std::stoi(argv[1]);
    wlog::logger::get().init("logs/raft_node_" + std::to_string(node_id) + ".log");
    std::string ip = argv[2];
    int port = std::stoi(argv[3]);

    // 解析集群节点信息
    std::vector<std::pair<std::string, int>> peers;
    for (int i = 4; i < argc; ++i) {
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

    std::cout << "Initializing Raft node " << node_id << " on " << ip << ":" << port << std::endl;
    std::cout << "Cluster peers: ";
    for (const auto& peer : peers) {
        std::cout << peer.first << ":" << peer.second << " ";
    }
    std::cout << std::endl;

    try {
        // 创建Raft节点实例
        RaftNode node(node_id, ip, port, peers);
        
        std::cout << "Starting Raft node..." << std::endl;
        
        // 启动节点
        node.start();
        
        std::cout << "Raft node started successfully!" << std::endl;
        std::cout << "Press Ctrl+C to stop the node." << std::endl;
        
        // 保持主线程运行，等待用户中断
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    } catch (const std::exception& e) {
        std::cerr << "Error starting Raft node: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}