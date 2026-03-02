#include "raftnode.hpp"
#include <signal.h>
#include <iostream>
#include <map>

// 分布式锁信息结构体
struct LockInfo {
    std::string owner;      // 持有者标识
    int64_t expire_at;      // 过期时间点 (Unix ms)
    bool is_locked = false;
};

// 状态机：分布式锁管理器
class DistributedLockManager {
public:
    // 处理日志条目，应用到锁状态机
    bool apply(const LogEntry& entry) {
        int64_t now = entry.timestamp; // 使用日志自带的时间戳，确保所有节点一致
        
        if (entry.command_type == "LOCK") {
             try_lock(entry.key, entry.value, now, entry.ttl);
        } else if (entry.command_type == "UNLOCK") {
             try_unlock(entry.key, entry.value, now);
        }
        return true;
    }

    // 获取锁状态（本地查询，不保证最新，通常由 Leader 提供一致性读）
    bool is_locked(const std::string& lock_name) {
        auto it = locks_.find(lock_name);
        if (it == locks_.end()) return false;
        // 检查是否过期（注意：这里使用系统当前时间仅供展示，严格一致性读需走 Raft 协议）
        int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        return it->second.is_locked && (it->second.expire_at == 0 || it->second.expire_at > now);
    }

private:
    void try_lock(const std::string& lock_name, const std::string& owner, int64_t now, int64_t ttl) {
        auto& lock = locks_[lock_name];
        
        // 1. 如果锁已被占用且未过期，则获取失败
        if (lock.is_locked && (lock.expire_at == 0 || lock.expire_at > now)) {
            spdlog::warn("------------------------------------------------------------------");
            spdlog::warn("[LockManager] Lock '{}' already held by '{}', expires at {}", 
                         lock_name, lock.owner, lock.expire_at);
            spdlog::warn("------------------------------------------------------------------");
            return ;
        }

        // 2. 否则，获取锁或锁已过期，重新授权
        lock.is_locked = true;
        lock.owner = owner;
        lock.expire_at = (ttl == 0) ? 0 : (now + ttl);
        
        spdlog::warn("------------------------------------------------------------------");
        spdlog::warn("[LockManager] LOCK SUCCESS: '{}' acquired by '{}'", lock_name, owner);
        if (ttl > 0) spdlog::warn("[LockManager] TTL: {}ms, ExpireAt: {}", ttl, lock.expire_at);
        spdlog::warn("------------------------------------------------------------------");
    }

    void try_unlock(const std::string& lock_name, const std::string& owner, int64_t now) {
        auto it = locks_.find(lock_name);
        if (it == locks_.end() || !it->second.is_locked) {
            spdlog::warn("------------------------------------------------------------------");
            spdlog::warn("[LockManager] Lock '{}' already are released");
            spdlog::warn("------------------------------------------------------------------");
        }

        // 只有持有者才能释放锁（简单安全校验）
        if (it->second.owner == owner) {
            it->second.is_locked = false;
            spdlog::warn("------------------------------------------------------------------");
            spdlog::warn("[LockManager] UNLOCK SUCCESS: '{}' released by '{}'", lock_name, owner);
            spdlog::warn("------------------------------------------------------------------");
        }
    }

    std::map<std::string, LockInfo> locks_;
};

DistributedLockManager g_lock_manager;
std::atomic<bool> g_running{true};
RaftNode* g_node = nullptr;

void signal_handler(int signal) {
    g_running = false;
    if (g_node) g_node->stop();
    exit(0);
}

int main(int argc, char* argv[]) {
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    auto node = initialize_server(argc, argv);
    g_node = node.get();
    
    // 设置分布式锁状态机回调
    node->set_apply_callback([](int32_t log_index, const LogEntry& entry) -> bool {
        return g_lock_manager.apply(entry);
    });

    std::this_thread::sleep_for(std::chrono::seconds(2));
    int node_id = atoi(argv[1]);
    while (g_running) {
        system("clear");
        std::cout << "\n=====================================" << std::endl;
        std::cout << "        Raft 分布式锁交互界面         " << std::endl;
        std::cout << "=====================================" << std::endl;
        std::cout << "  1. LOCK   - 获取分布式锁            " << std::endl;
        std::cout << "  2. UNLOCK - 释放分布式锁            " << std::endl;
        std::cout << "  3. STATUS - 查看锁状态              " << std::endl;
        std::cout << "  0. EXIT   - 退出程序                " << std::endl;
        std::cout << "=====================================\n" << std::endl;

        std::cout << "请输入操作编号(0-3):";
        int op;
        if (!(std::cin >> op)) {
            std::cin.clear();
            std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
            continue;
        }

        if (op == 0) break;

        std::string lock_name, client_id;
        std::cout << "请输入锁名称: ";
        std::cin >> lock_name;

        if (op == 1) {
            int64_t ttl;
            std::cout << "请输入过期时间 (秒, 0表示永不过期): ";
            std::cin >> ttl;

            LogEntry entry;
            entry.command_type = "LOCK";
            entry.key = lock_name;
            entry.value = std::to_string(node_id);
            entry.ttl = ttl*1000;

            if (node->submit(entry)) {
                std::cout << ">>> LOCK 请求已提交至 Raft 集群，请观察日志确认应用结果。" << std::endl;
            }
        } else if (op == 2) {
            LogEntry entry;
            entry.command_type = "UNLOCK";
            entry.key = lock_name;
            entry.value = std::to_string(node_id);

            if (node->submit(entry)) {
                std::cout << ">>> UNLOCK 请求已提交至 Raft 集群。" << std::endl;
            }
        } else if (op == 3) {
            bool locked = g_lock_manager.is_locked(lock_name);
            std::cout << ">>> 锁 '" << lock_name << "' 当前状态: " << (locked ? "已锁定" : "未锁定/已过期") << std::endl;
        }

        std::cout << "\n按回车键继续...";
        std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        std::cin.get();
    }

    return 0;
}
