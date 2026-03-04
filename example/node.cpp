#include "../include/raftnode.hpp"
#include <signal.h>
#include <iostream>
// 全局标志，用于通知主线程退出
using namespace mrpc;
std::atomic<bool> g_running{true};
RaftNode* g_node = nullptr;
// 信号处理函数
void signal_handler(int signal) {
    g_running = false;
        if (g_node) {
            g_node->stop();
        }
        exit(0);
}

int main(int argc, char* argv[]) {
     // 注册信号处理函数，用于捕获 SIGINT (Ctrl+C) 和 SIGTERM
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    auto node = initialize_server(argc, argv);
    g_node = node.get();

    std::this_thread::sleep_for(std::chrono::seconds(2));
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    node->stop();
    return 0;
   spdlog::debug("Raft node shutdown complete.");
}