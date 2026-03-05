#include "../include/raftnode.hpp"
#include <signal.h>
#include <iostream>
#include "../include/config_center.hpp"

std::atomic<bool> g_running{true};
RaftNode* g_node = nullptr;

void signal_handler(int) {
    g_running = false;
    if (g_node) g_node->stop();
    exit(0);
}

int main(int argc, char* argv[]) {
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    auto node = initialize_server(argc, argv);
    g_node = node.get();

    ConfigCenter config(node);
    
    
    config.WATCH_SET("database", [](const std::string& key, const std::string& value) -> bool   {
        std::cout << "数据库地址已更新: " << value << std::endl;
        return true;
    });

    config.WATCH_DELETE("database", [](const std::string& key) -> bool {
        std::cout << "数据库地址已删除: " << key << std::endl;
        return true;
    });
    
    
    std::this_thread::sleep_for(std::chrono::seconds(2));
    system("clear");
    std::cout << "-----------------------------准备修改数据库地址------------------------------------" << std::endl;
    std::string new_db_address;
    std::cout << "请输入新的数据库地址: ";
    std::cin >> new_db_address;
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    config.Set("database", new_db_address, now);


    while(1);
}