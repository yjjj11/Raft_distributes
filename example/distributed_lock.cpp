#include "raftnode.hpp"
#include <signal.h>
#include <iostream>
#include <string>
#include <unordered_map>
#include <chrono>

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

class DistributedLock {
public:
    struct LockInfo {
        bool is_locked = false;
        std::string holder;
        std::chrono::steady_clock::time_point lock_time;
    };

    bool lock(const std::string& lock_name, const std::string& holder) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::cout<<"--------------------正在尝试获取锁----------------------------"<<std::endl;
        auto it = locks_.find(lock_name);
        if (it != locks_.end() && it->second.is_locked) {
            if(holder!=it->second.holder){
                spdlog::info("Lock {} is already held by {}", lock_name, it->second.holder);
                return false;
            }else{
                spdlog::info("Lock {} is already held by yourself", lock_name);
                std::cout<<"--------------------🔄锁已被你持有----------------------------"<<std::endl;
            }
        }
        LockInfo info;
        info.is_locked = true;
        info.holder = holder;
        info.lock_time = std::chrono::steady_clock::now();
        locks_[lock_name] = info;
        spdlog::info("{} acquired lock {}", holder, lock_name);
        std::cout<<"--------------------✅成功获取锁"<<lock_name<<"----------------------------"<<std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(15));
        return true;
    }

    bool unlock(const std::string& lock_name, const std::string& holder) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = locks_.find(lock_name);
        if (it == locks_.end() || !it->second.is_locked) {
            spdlog::info("Lock {} is not locked", lock_name);
            return false;
        }
        
        if (it->second.holder != holder) {
            spdlog::info("Lock {} is held by {}, not {}", lock_name, it->second.holder, holder);
            return false;
        }
        
        it->second.is_locked = false;
        std::cout<<"--------------------✅成功释放锁"<<lock_name<<"----------------------------"<<std::endl;
        spdlog::info("{} released lock {}", holder, lock_name);
        return true;
    }

    bool is_locked(const std::string& lock_name) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = locks_.find(lock_name);
        return it != locks_.end() && it->second.is_locked;
    }

    std::string get_holder(const std::string& lock_name) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = locks_.find(lock_name);
        if (it != locks_.end() && it->second.is_locked) {
            return it->second.holder;
        }
        return "";
    }

private:
    std::unordered_map<std::string, LockInfo> locks_;
    std::mutex mutex_;
};

DistributedLock distributed_lock;

int main(int argc, char* argv[]) {
    // 注册信号处理函数
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    auto node = initialize_server(argc, argv);
    g_node = node.get();
    
    node->set_apply_callback([](int32_t log_index, const LogEntry& entry) -> bool {
        if (entry.command_type == "LOCK") {
            distributed_lock.lock(entry.key, entry.value);
        } else if (entry.command_type == "UNLOCK") {
            distributed_lock.unlock(entry.key, entry.value);
        }
        return true;
    });

    std::cout << "-----------------------------分布式锁操作界面------------------------------------" << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    std::string holder_id = "client_" + std::to_string(std::rand() % 1000);
    std::cout << "您的客户端ID: " << holder_id << std::endl;
    
    while (g_running) {
        system("clear");
        std::cout << "\n=====================================" << std::endl;
        std::cout << "        分布式锁操作界面              " << std::endl;
        std::cout << "=====================================" << std::endl;
        std::cout << "  1. LOCK   - 获取锁                  " << std::endl;
        std::cout << "  2. UNLOCK - 释放锁                  " << std::endl;
        std::cout << "  3. CHECK  - 检查锁状态              " << std::endl;
        std::cout << "  0. EXIT   - 退出操作界面            " << std::endl;
        std::cout << "=====================================\n\n" << std::endl;

        std::cout << "请输入操作编号(0-3):";
        int op;
        if (!(std::cin >> op)) {
            std::cin.clear();
            std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
            std::cout << "输入无效，请输入数字！" << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(2));
            continue;
        }

        switch (op) {
            case 0: {
                g_running = false;
                break;
            }
            case 1: { // LOCK操作
                std::cout << "----------------------------获取锁---------------------------" << std::endl;
                std::string lock_name;
                std::cout << "请输入锁名称：";
                std::cin >> lock_name;
                
                LogEntry entry{0, lock_name, holder_id, "LOCK"};
                bool success = node->submit(entry);
                if (success) {
                    std::cout << "✅ 锁获取请求已提交：" << lock_name << std::endl;
                } else {
                    std::cout << "❌ 锁获取请求失败（非Leader或提交失败）" << std::endl;
                }
                std::this_thread::sleep_for(std::chrono::seconds(1));
                break;
            }
            case 2: { // UNLOCK操作
                std::cout << "----------------------------释放锁---------------------------" << std::endl;
                std::string lock_name;
                std::cout << "请输入锁名称：";
                std::cin >> lock_name;
                
                LogEntry entry{0, lock_name, holder_id, "UNLOCK"};
                bool success = node->submit(entry);
                if (success) {
                    std::cout << "✅ 锁释放请求已提交：" << lock_name << std::endl;
                } else {
                    std::cout << "❌ 锁释放请求失败（非Leader或提交失败）" << std::endl;
                }
                std::this_thread::sleep_for(std::chrono::seconds(1));
                break;
            }
            case 3: { // CHECK操作
                std::cout << "----------------------------检查锁状态---------------------------" << std::endl;
                std::string lock_name;
                std::cout << "请输入锁名称：";
                std::cin >> lock_name;
                
                bool is_locked = distributed_lock.is_locked(lock_name);
                if (is_locked) {
                    std::string holder = distributed_lock.get_holder(lock_name);
                    std::cout << "✅ 锁 " << lock_name << " 已被锁定，持有者：" << holder << std::endl;
                } else {
                    std::cout << "✅ 锁 " << lock_name << " 未被锁定" << std::endl;
                }
                std::cout << "按任意键继续..." << std::endl;
                std::cin.ignore();
                std::string dummy;
                std::getline(std::cin, dummy);
                break;
            }
            default: {
                std::cout << "❌ 无效的操作类型，请输入0-3！" << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(2));
                break;
            }
        }
    }
    node->stop();
    return 0;
}
