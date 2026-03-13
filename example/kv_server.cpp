#include "raftnode.hpp"
#include "kv_store.hpp"
#include <signal.h>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>

using namespace mrpc;
using json = nlohmann::json;
using json = nlohmann::json;
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
    // 注册信号处理函数
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // 初始化Raft节点
    auto node = initialize_server(argc, argv);
    g_node = node.get();

    // 初始化KV服务
    KvService service(node);

    // 注册RPC函数，参考任务调度服务端
    node->server_.reg_func("Put", [&service, &node](const std::string& key, const std::string& value)  {
        try {
            node->wait_for(service.Put(key, value));
        } catch (const std::exception& e) {
            spdlog::error("Put failed: {}", e.what());
        }
    });

    node->server_.reg_func("Get", [&service](const std::string& key) -> std::string {
        try {
            auto result = service.unsafe_get(key);
            return "";
        } catch (const std::exception& e) {
            spdlog::error("Get failed: {}", e.what());
            return "";
        }
    });

    node->server_.reg_func("Del", [&service, &node](const std::string& key) -> void {
        try {
            node->wait_for(service.Del(key));
        } catch (const std::exception& e) {
            spdlog::error("Del failed: {}", e.what());
        }
    });

    std::cout << "KV Store Server started. Node ID: " << node->node_id_ << std::endl;
    std::cout << "Press Ctrl+C to stop." << std::endl;

    // 保持服务器运行
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return 0;
}