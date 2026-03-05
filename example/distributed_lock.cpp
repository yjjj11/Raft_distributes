#include "raftnode.hpp"
#include <signal.h>
#include <iostream>
#include "kv_store.hpp"
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
    KvService service(node);
    std::string key, value;
    int choice = 0;

    system("clear");
    std::cout << "Raft-based KV Store Client Started." << std::endl;

    bool ret= service.Get_lock("distributed_lock", 10000 ,20000);
    if (ret) {
        std::cout << "\n--------------------成功获取锁--------------------\n";
        std::this_thread::sleep_for(std::chrono::seconds(2));
        ret = service.Release_lock("distributed_lock");
        if (ret) {
            std::cout << "\n--------------------成功释放锁--------------------\n";
        } else {
            std::cout << "\n--------------------释放锁失败--------------------\n";
        }
    } else {
        std::cout << "\n--------------------获取锁失败--------------------\n";
    }
    while(1);
   spdlog::debug("Raft node shutdown complete.");
}