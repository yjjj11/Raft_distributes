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

    // 1. 创建状态机实例
    KvStore kv_store_impl;

    // 2. 将状态机的操作注册到 Raft 节点
    node->callback_reg.reg_callback("Put", &kv_store_impl, &KvStore::apply_put);
    node->callback_reg.reg_callback("Del", &kv_store_impl, &KvStore::apply_del);
    
    // 3. 创建服务层实例
    KvService service(node, kv_store_impl);
    system("clear");
    while (g_running) {
        // 4. 模拟用户操作
        std::cout<<"-------------------开始测试-------------------"<<std::endl;
        std::cout<<"请输入操作指令(Put/Del/Get): ";
        std::string cmd;
        std::cin>>cmd;
        if(cmd == "Put"){
            std::cout<<"请输入key: ";
            std::string key;
            std::cin>>key;
            std::cout<<"请输入value: ";
            std::string value;
            std::cin>>value;
            if(service.Put(key, value)){
                std::cout << "Key = " << key << ", Value = " << value << std::endl;
            }
        }
        else if(cmd == "Del"){
            std::cout<<"请输入key: ";
            std::string key;
            std::cin>>key;
            if(service.Del(key)){
                std::cout << "Del(key) = " << key << std::endl;
            }
        }
        else if(cmd == "Get"){
            std::cout<<"请输入key: ";
            std::string key;
            std::cin>>key;
            if(auto res=service.Get(key); res != ""){
                std::cout << "Get(key) = " << key << ", Value = " << res << std::endl;
            }
        }
        else{
            std::cout<<"无效指令"<<std::endl;
        }
        std::cin.get();
    }
    node->stop();
    return 0;
   spdlog::debug("Raft node shutdown complete.");
}