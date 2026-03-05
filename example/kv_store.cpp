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

    std::cout << "Raft-based KV Store Client Started." << std::endl;

   while (g_running) {
        // 显示操作菜单
        system("clear");
        std::cout << "\n========== Simple KV Store OS ==========" << std::endl;
        std::cout << "1. Put (Set a key-value pair)" << std::endl;
        std::cout << "2. Get (Retrieve a value by key)" << std::endl;
        std::cout << "3. Delete (Remove a key-value pair)" << std::endl;
        std::cout << "4. Compare And Swap (CAS)" << std::endl;
        std::cout << "5. Test False (Execute a test command)" << std::endl;
        std::cout << "6. Exit" << std::endl;
        std::cout << "========================================" << std::endl;
        std::cout << "Enter your choice (1-6): ";

        // 获取用户输入的选择
        if (!(std::cin >> choice)) {
            // 如果输入无效（例如输入了字母），则清空错误标志并忽略缓冲区
            std::cin.clear();
            std::cin.ignore(10000, '\n');
            std::cout << "Invalid input. Please enter a number between 1 and 6." << std::endl;
            continue;
        }

        // 根据选择执行相应操作
        switch (choice) {
            case 1: {
                std::cout << "Enter key: ";
                std::cin >> key;
                std::cout << "Enter value: ";
                std::cin >> value;
                auto id = service.Put(key, value);
                std::cout << "Put operation submitted with ID: " << id << std::endl;
                std::cin.get();
                std::cin.get();
                break;
            }
            case 2: {
                std::cout << "Enter key to get: ";
                std::cin >> key;
                auto result = service.Get(key);
                std::cout << "Value for key '" << key << "': " << result << std::endl;
                std::cin.get();
                std::cin.get();
                break;
            }
            case 3: {
                std::cout << "Enter key to delete: ";
                std::cin >> key;
                auto id = service.Del(key); // 假设 KvService 有 Delete 方法
                std::cout << "Delete operation submitted with ID: " << id << std::endl;
                std::cin.get();
                std::cin.get();
                break;
            }
            case 4: { // Compare And Swap
                std::string old_value, new_value;
                std::cout << "Enter key: ";
                std::cin >> key;
                std::cout << "Enter old value: ";
                std::cin >> old_value;
                std::cout << "Enter new value: ";
                std::cin >> new_value;
                
                // 假设 KvService 有一个 Cas 方法，它返回操作ID或结果
                auto id = service.Cas(key, old_value, new_value);
                
                // 这里可以根据您的 Cas 实现的具体返回类型进行调整
                // 例如，如果 Cas 返回 bool 表示是否成功，则可以这样打印：
                // if(result) { std::cout << "CAS successful.\n"; } else { std::cout << "CAS failed.\n"; }
                if(service.get_reply_by_id(id)){
                    std::cout << "CAS successful.\n";
                }else{
                    std::cout << "CAS failed.\n";
                }
                std::cout << "Compare And Swap operation submitted with ID: " << id << std::endl;
                std::cin.get();
                std::cin.get();
                break;
            }
            case 5: {
                // 调用原有的测试命令
                service.TestFalse();
                std::cout << "TestFalse command executed." << std::endl;
                std::cin.get();
                std::cin.get();
                break;
            }
            case 6: { // Exit
                std::cout << "Shutting down..." << std::endl;
                g_running = false;
                break;
            }
            default:
                std::cout << "Invalid choice. Please select a number between 1 and 6." << std::endl;
                break;
        }
    }
    node->stop();
    return 0;
   spdlog::debug("Raft node shutdown complete.");
}