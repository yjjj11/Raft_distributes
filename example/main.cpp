#include "raftnode.hpp"
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
class KvStore {
public:
    bool put(const std::string& key, const std::string& value) {
        std::cout << "Apply: " << key << " = " << value << std::endl;
        kv_store_[key] = value;
        return true;
    }
    std::string get(const std::string& key) {
        return kv_store_[key];
    }
private:
    std::unordered_map<std::string, std::string> kv_store_;
};
KvStore kv_store;


int main(int argc, char* argv[]) {
     // 注册信号处理函数，用于捕获 SIGINT (Ctrl+C) 和 SIGTERM
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    auto node = initialize_server(argc, argv);
    g_node = node.get();

    node->set_apply_callback([](int32_t log_index, const LogEntry& entry) -> bool {
        kv_store.put(entry.key, entry.value);
        return true;
    });

    std::cout<<"-----------------------------准备加载操作页面------------------------------------"<<std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    while (g_running) {
        system("clear");
        std::cout << "\n=====================================" << std::endl;
        std::cout << "          KV存储操作界面              " << std::endl;
        std::cout << "=====================================" << std::endl;
        std::cout << "  1. PUT   - 新增/修改键值对          " << std::endl;
        std::cout << "  2. GET   - 查询指定键的值           " << std::endl;
        std::cout << "  3. DELETE - 删除指定键值对           " << std::endl;
        std::cout << "  0. EXIT  - 退出操作界面             " << std::endl;
        std::cout << "=====================================\n" << std::endl;
        int op;
        if (!(std::cin >> op)) {
            std::cin.clear(); // 清除输入错误状态
            std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
            std::cout << "输入无效，请输入数字！" << std::endl;
            continue;
        }

        switch (op) {
            case 0: {
                g_running = false;
                break;
            }
            case 1: { // PUT操作
                std::string key, value;
                std::cout << "请输入键：";
                std::cin >> key;
                std::cout << "请输入值：";
                std::cin.ignore(); // 忽略换行符
                std::getline(std::cin, value); // 支持带空格的值
                LogEntry entry{0,key,value};
                bool success = node->submit(entry);
                if (success) {
                    std::cout << "✅ PUT成功：" << key << " = " << value << std::endl;
                } else {
                    std::cout << "❌ PUT失败（非Leader或日志提交失败）" << std::endl;
                }
                break;
            }
            case 2: { // GET操作
                std::string key;
                std::cout << "请输入要查询的键：";
                std::cin >> key;
 
                auto value =kv_store.get(key);
                if (value.empty()) {
                    std::cout << "❌ 查询失败：键 " << key << " 不存在" << std::endl;
                } else {
                    std::cout << "✅ 查询结果：" << key << " = " << value << std::endl;
                }
                std::cout << "按任意键继续..." << std::endl;
                std::cin.ignore(); // 忽略换行符
                std::getline(std::cin, value); // 等待用户输入任意键继续
                break;
            }
            // case 3: { // DELETE操作
            //     std::string key;
            //     std::cout << "请输入要删除的键：";
            //     std::cin >> key;

            //     bool success = node->kv_delete(key);
            //     if (success) {
            //         std::cout << "✅ DELETE成功：" << key << std::endl;
            //     } else {
            //         std::cout << "❌ DELETE失败（非Leader/键不存在/日志提交失败）" << std::endl;
            //     }
            //     break;
            // }
            default: {
                std::cout << "❌ 无效的操作类型，请输入0-3！" << std::endl;
                break;
            }
        }
    
    spdlog::debug("Raft node shutdown complete.");

    }
    node->stop();
    return 0;
   
}