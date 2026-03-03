#include "raftnode.hpp"
#include <signal.h>
#include <iostream>
#include <map>
#include <chrono>

struct LockInfo {
    std::string owner;
    int64_t expire_at;
    bool is_locked = false;
};

class DistributedLockManager {
public:
    bool apply(const LogEntry& entry) {
        if (entry.command_type == "LOCK") try_lock(entry.key, entry.value, entry.timestamp, entry.ttl);
        else if (entry.command_type == "UNLOCK") try_unlock(entry.key, entry.value);
        return true;
    }

    bool is_locked(const std::string& name) {
        auto it = locks_.find(name);
        if (it == locks_.end() || !it->second.is_locked) return false;
        int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        return it->second.expire_at == 0 || it->second.expire_at > now;
    }

    std::string get_holder(const std::string& name) {
        return is_locked(name) ? locks_[name].owner : "None";
    }

private:
    void try_lock(const std::string& name, const std::string& owner, int64_t now, int64_t ttl) {
        auto& lock = locks_[name];
        if (lock.is_locked && (lock.expire_at == 0 || lock.expire_at > now)) {
            spdlog::warn("[LockManager] '{}' LOCK FAILED: already held by '{}'", name, lock.owner);
            return;
        }
        lock = {owner, ttl == 0 ? 0 : now + ttl, true};
        spdlog::warn("[LockManager] '{}' LOCK SUCCESS by '{}' (TTL: {}ms)", name, owner, ttl);
    }

    void try_unlock(const std::string& name, const std::string& owner) {
        auto it = locks_.find(name);
        if (it != locks_.end() && it->second.is_locked && it->second.owner == owner) {
            it->second.is_locked = false;
            spdlog::warn("[LockManager] '{}' UNLOCK SUCCESS by '{}'", name, owner);
        }
    }
    std::map<std::string, LockInfo> locks_;
};

DistributedLockManager g_lock_manager;
std::atomic<bool> g_running{true};
RaftNode* g_node = nullptr;

void signal_handler(int) {
    g_running = false;
    if (g_node) g_node->stop();
    exit(0);
}

int keep_trying_lock(LogEntry& entry, int64_t timeout_ms) {
    auto start_time = std::chrono::steady_clock::now();
    std::cout << "\n--------------------正在尝试获取锁--------------------\n";
    
    while (true) {
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time).count();
        if (elapsed > timeout_ms) return -1;
        
        // 如果当前没有被锁定，或者锁定者不是自己（且已过期），尝试竞争
        if (g_lock_manager.get_holder(entry.key) != entry.value) {
            entry.command_type = "LOCK";
            int64_t req_id = g_node->submit(entry);
            if (req_id != -1) {
                // 阻塞等待日志被应用到状态机 (仿照 kv_store 的 wait_for)
                g_node->wait_for(req_id);
                
                // 检查状态机应用后的结果：锁是否真的到了自己手里
                if (g_lock_manager.get_holder(entry.key) == entry.value) {
                    std::cout << "\n--------------------成功获取锁, 开始执行5s任务--------------------\n";
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                    
                    std::cout << "\n--------------------任务执行完成, 准备释放锁--------------------\n";
                    entry.command_type = "UNLOCK";
                    int64_t unl_req_id = g_node->submit(entry);
                    if (unl_req_id != -1) {
                        g_node->wait_for(unl_req_id);
                        return 0;
                    }
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500)); // 避免过于频繁的重试请求
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Usage: " << argv[0] << " <client_id> <node_id> <ip> <port> ..." << std::endl;
        return 1;
    }
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    auto node = initialize_server(argc, argv);
    g_node = node.get();
    
    node->set_apply_callback([](int32_t, const LogEntry& entry) {
        g_lock_manager.apply(entry);
        // 阻塞通知逻辑：仿照 kv_store 触发 promise
        g_node->lock_store_[entry.req_id].set_value(true);
        return true;
    });

    std::this_thread::sleep_for(std::chrono::seconds(3));
    system("clear");

    LogEntry entry;
    entry.key = "distributed_resource_lock";
    entry.value = argv[1]; // 使用命令行第一个参数作为 Client ID
    entry.ttl = 10000;     // 10s 锁超时

    int ret = keep_trying_lock(entry, 20000); // 20s 获取锁超时
    
    if (ret == 0) {
        std::cout << "\n>>> 全流程执行成功并已释放锁。\n";
    } else {
        std::cout << "\n>>> 获取锁超时（15s），程序退出。\n";
    }

    std::this_thread::sleep_for(std::chrono::seconds(2));
    g_node->stop();
    return 0;
}
