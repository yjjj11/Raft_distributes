#include "raftnode.hpp"
#include <signal.h>
#include <iostream>
#include <map>
#include <chrono>

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
        // 检查是否过期
        int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        return it->second.is_locked && (it->second.expire_at == 0 || it->second.expire_at > now);
    }

    std::string get_lock_holder(const std::string& lock_name) {
        auto it = locks_.find(lock_name);
        if (it == locks_.end() || !it->second.is_locked) {
            return "";
        }
        return it->second.owner;
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
            spdlog::warn("[LockManager] Lock '{}' already released", lock_name);
            spdlog::warn("------------------------------------------------------------------");
            return;
        }

        // 只有持有者才能释放锁（简单安全校验）
        if (it->second.owner == owner) {
            it->second.is_locked = false;
            spdlog::warn("------------------------------------------------------------------");
            spdlog::warn("[LockManager] UNLOCK SUCCESS: '{}' released by '{}'", lock_name, owner);
            spdlog::warn("------------------------------------------------------------------");
        } else {
            spdlog::warn("------------------------------------------------------------------");
            spdlog::warn("[LockManager] UNLOCK FAILED: '{}' is held by '{}', not '{}'", lock_name, it->second.owner, owner);
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

bool wait_for_lock_application(const std::string& lock_name, const std::string& owner, int64_t timeout_ms) {
    auto start = std::chrono::steady_clock::now();
    while (true) {
        if (g_lock_manager.is_locked(lock_name) && g_lock_manager.get_lock_holder(lock_name) == owner) {
            return true;
        }
        
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count();
        if (elapsed > timeout_ms) {
            return false;
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

bool wait_for_unlock_application(const std::string& lock_name, int64_t timeout_ms) {
    auto start = std::chrono::steady_clock::now();
    while (true) {
        if (!g_lock_manager.is_locked(lock_name)) {
            return true;
        }
        
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count();
        if (elapsed > timeout_ms) {
            return false;
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

int keep_trying_lock(LogEntry& entry, int64_t timeout_ms) {
    auto start_time = std::chrono::steady_clock::now();
    std::cout << "\n--------------------正在尝试获取锁--------------------\n";
    
    while (true) {
        // 检查是否超时
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time).count();
        if (elapsed > timeout_ms) {
            std::cout << "\n--------------------获取锁时间超时--------------------\n";
            return -1;
        }
        
        // 检查锁是否可用
        if (!g_lock_manager.is_locked(entry.key)) {
            // 锁可用，提交锁请求
            std::cout<<"锁可用，提交锁请求"<<std::endl;
            if (!g_node->submit(entry)) {
                std::cout << "\n--------------------提交锁请求失败--------------------\n";
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            
            // 等待锁被应用
            if (wait_for_lock_application(entry.key, entry.value, 5000)) {
                // 成功获取锁
                std::cout << "\n--------------------成功获取锁,开始执行任务--------------------\n";
                
                // 执行耗时5秒的任务
                std::this_thread::sleep_for(std::chrono::milliseconds(5000));
                
                std::cout << "\n--------------------任务执行完成,准备释放锁--------------------\n";
                
                // 释放锁

                entry.command_type = "UNLOCK";
                entry.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                
                if (g_node->submit(entry)) {
                    // 等待锁释放完成
                    if (wait_for_unlock_application(entry.key, 5000)) {
                        std::cout << "\n--------------------锁释放成功--------------------\n";
                    } else {
                        std::cout << "\n--------------------锁释放超时--------------------\n";
                    }
                } else {
                    std::cout << "\n--------------------提交释放锁请求失败--------------------\n";
                }
                
                return 0;
            } else {
                std::cout << "\n--------------------等待锁应用超时--------------------\n";
                // 继续尝试
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        } else {
            // 锁被占用，等待一段时间后重试
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
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
    system("clear");
    LogEntry entry;
    entry.command_type = "LOCK";
    entry.key = "lock";
    entry.value = argv[1];
    entry.ttl = 15000; // 设置15秒过期时间，确保足够执行5秒任务
    entry.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // // 设置20秒超时时间，足够获取锁和执行任务
    int ret = keep_trying_lock(entry, 20000);
    if (ret == -1) {
        std::cout << "\n--------------------获取锁时间超时--------------------\n";
    }
    // std::cout<<g_lock_manager.is_locked(entry.key)<<std::endl;
    // 等待一段时间确保所有操作完成
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    g_node->stop();
    return 0;
}


// 测试方法：
// 1. 打开两个终端，分别输入
//./bin/distributed_lock 1 127.0.0.1 8001 2 127.0.0.1:8000
//./bin/distributed_lock 0 127.0.0.1 8000 2 127.0.0.1:8001
// 2.同时启动2个节点，观察日志输出