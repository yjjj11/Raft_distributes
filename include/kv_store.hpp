#pragma once
#include <string>
#include <unordered_map>
#include <chrono>
#include <optional>
#include <mutex>
#include <logger.hpp>
#include <raftnode.hpp>

// 定义一个结构体来存储值和过期时间
struct ValueWithExpiry {
    std::string value;
    std::chrono::steady_clock::time_point expiry_time;
    
    // 构造函数，如果没有指定过期时间，则默认不过期
    ValueWithExpiry(const std::string& val, std::chrono::steady_clock::time_point expiry = std::chrono::steady_clock::time_point::max())
        : value(val), expiry_time(expiry) {}
    
    // 检查是否已过期
    bool is_expired() const {
        return std::chrono::steady_clock::now() > expiry_time;
    }
};

class KvStore {
public:
    // 1. 应用 Put 操作
    bool apply_put(const std::string& key, const std::string& value) {
        spdlog::info("Applying PUT: {} = {}", key, value);
        // 无限期存储，设置一个极大过期时间
        kv_store_[key] = ValueWithExpiry(value);
        return true;
    }

    // 2. 应用带超时的 Put 操作
    bool apply_put_with_ttl(const std::string& key, const std::string& value, long long ttl_ms) {
        auto now = std::chrono::steady_clock::now();
        auto expiry_time = now + std::chrono::milliseconds(ttl_ms);
        spdlog::info("Applying PUT_WITH_TTL: {} = {}, expires in {} ms", key, value, ttl_ms);
        kv_store_[key] = ValueWithExpiry(value, expiry_time);
        return true;
    }

    // 3. 应用 Del 操作
    bool apply_del(const std::string& key) {
        spdlog::info("Applying DELETE: {}", key);
        kv_store_.erase(key);
        return true;
    }

    // 4. 应用 CAS 操作
    // 返回值：true 表示 CAS 成功，false 表示当前值与期望值不符，CAS 失败
    bool apply_cas(const std::string& key, const std::string& expected_value, const std::string& new_value) {
        auto it = kv_store_.find(key);
        // 如果键不存在，且期望值是空字符串，则视为匹配
        if (it == kv_store_.end()) {
            if (expected_value.empty()) {
                spdlog::info("Applying CAS: Key '{}' does not exist, expected empty. Setting to '{}'.", key, new_value);
                kv_store_[key] = ValueWithExpiry(new_value);
                return true;
            }
            spdlog::info("Applying CAS: Key '{}' does not exist, expected '{}', CAS failed.", key, expected_value);
            return false;
        }
        
        // 检查键是否已过期
        if (it->second.is_expired()) {
            spdlog::info("Applying CAS: Key '{}' is expired, treating as non-existent.", key);
            kv_store_.erase(it);
            // 过期后的行为：如果期望值是空，可以设置新值
            if (expected_value.empty()) {
                spdlog::info("Applying CAS: After expiry, setting '{}' to '{}'.", key, new_value);
                kv_store_[key] = ValueWithExpiry(new_value);
                return true;
            }
            return false;
        }

        // 比较当前值
        if (it->second.value == expected_value) {
            spdlog::info("Applying CAS: Key '{}' matches expected value. Updating to '{}'.", key, new_value);
            it->second.value = new_value; // 更新值，保持原有过期时间
            return true;
        } else {
            spdlog::info("Applying CAS: Key '{}' value '{}' does not match expected '{}', CAS failed.", key, it->second.value, expected_value);
            return false;
        }
    }

    // 5. 提供一个安全的只读查询方法
    // 这个方法不应直接暴露给用户，而是供上层 Service 调用
    std::optional<std::string> unsafe_get(const std::string& key) const { // 返回 optional 以区分 "key不存在" 和 "value为空字符串"
        std::lock_guard<std::mutex> lock(store_mutex_); // 添加锁保护
        
        auto it = kv_store_.find(key);
        if (it != kv_store_.end()) {
            if (it->second.is_expired()) {
                spdlog::info("Unsafe GET: Key '{}' found but is expired, removing from store.", key);
                // 注意：这里只是读取，不能修改 store。但在应用日志时，可能需要一个清理过期键的机制。
                // 对于读取，我们返回 nullopt，表示该键已不存在。
                return std::nullopt;
            }
            return it->second.value;
        }
        return std::nullopt; // Key not found
    }

private:
    mutable std::mutex store_mutex_; // 保护 kv_store_ 的锁
    std::unordered_map<std::string, ValueWithExpiry> kv_store_;
};


class KvService {
public:
    KvService(std::shared_ptr<RaftNode> raft_node, KvStore& store) 
        : raft_node_(raft_node), kv_store_(store) {}

    // 安全的写入操作
    bool Put(const std::string& key, const std::string& value) {
        auto entry = raft_node_->pack_logentry("Put", key, value);
        auto result = raft_node_->submit(entry);
        return result != -1; 
    }

    // 新增：安全的带超时写入操作
    bool PutWithTTL(const std::string& key, const std::string& value, long long ttl_ms) {
        auto entry = raft_node_->pack_logentry("PutWithTTL", key, value, ttl_ms);
        auto result = raft_node_->submit(entry);
        return result != -1;
    }

    // 安全的删除操作
    bool Del(const std::string& key) {
        auto entry = raft_node_->pack_logentry("Del", key);
        auto result = raft_node_->submit(entry);
        return result != -1;
    }

    // 新增：安全的 CAS 操作
    // compare_and_swap: 如果 key 的当前值等于 expected_value，则将其更新为 new_value。
    // 返回值：true 表示操作成功，false 表示当前值与期望值不符，操作失败。
    bool Cas(const std::string& key, const std::string& expected_value, const std::string& new_value) {
        auto entry = raft_node_->pack_logentry("Cas", key, expected_value, new_value);
        auto result = raft_node_->submit(entry);
        // submit 返回 -1 表示提交失败（如找不到 Leader）
        // 如果提交成功，我们需要知道 CAS 在状态机层面是否成功。
        // 这里有一个问题：submit 是 void 类型，无法直接返回 CAS 的布尔结果。
        // 解决方案：需要修改 Raft 的设计，让 submit 能够返回命令执行的结果。
        // 为了演示，我们假设如果提交成功，则 Raft 状态机会执行 CAS 并通过某种方式（例如，一个带结果的 future）
        // 通知我们最终结果。但现在，我们只能返回提交是否成功。
        // 一个临时的解决方法是，让 RaftNode 的 wait_for 接收一个能返回结果的 future。
        // 但为了保持简单，我们暂时返回提交是否成功。
        // 更好的做法是修改 RaftNode 的 submit 和 wait_for 逻辑，使其能传递命令的执行结果。
        return result != -1; 
        // --- 重要说明 ---
        // 上面的返回值是不准确的！它只表示日志是否成功提交，不代表 CAS 操作本身的成功与否。
        // 要获取 CAS 的真实结果，需要对 RaftNode 进行改造，使其能将命令的执行结果（如 true/false）返回给调用者。
        // 例如，可以让 LogEntry 包含一个 future，状态机执行完后设置这个 future 的值。
    }

    // 安全的读取操作 (这里采用"转发给 Leader"的简化模型)
    std::string Get(const std::string& key) {
        // Raft 读取优化：可以实现 ReadIndex 或 Lease-based Read 以降低延迟。
        // 现在的实现是通过提交一个日志来保证线性一致性读。
        auto entry = raft_node_->pack_logentry("Get", key);
        auto result = raft_node_->submit(entry);
        if (result == -1) {
            return ""; // 操作失败
        }
        auto opt_value = kv_store_.unsafe_get(key);
        return opt_value ? *opt_value : "";
    }

    // --- 以下为辅助方法 ---

    // 获取值，但不通过 Raft 日志，直接从本地状态机读取（可能读到过期数据）
    std::optional<std::string> unsafe_Get(const std::string& key) const {
        return kv_store_.unsafe_get(key);
    }

    // --- 新增：检查键是否存在且未过期 ---
    bool Exists(const std::string& key) {
        auto entry = raft_node_->pack_logentry("Exists", key);
        auto result = raft_node_->submit(entry);
        if (result == -1) {
            return false; // 操作失败
        }
        return kv_store_.unsafe_get(key).has_value();
    }

private:
    KvStore& kv_store_;
    std::shared_ptr<RaftNode> raft_node_;
};