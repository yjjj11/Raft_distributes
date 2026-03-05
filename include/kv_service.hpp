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
    
    ValueWithExpiry() : expiry_time(std::chrono::steady_clock::time_point::max()) {
        value="";
    }
    // 构造函数，如果没有指定过期时间，则默认不过期
    ValueWithExpiry(const std::string& val, std::chrono::steady_clock::time_point expiry = std::chrono::steady_clock::time_point::max())
        : value(val), expiry_time(expiry) {}
    
    // 检查是否已过期
    bool is_expired() const {
        return std::chrono::steady_clock::now() > expiry_time;
    }
};


class KvService {
public:
    KvService(std::shared_ptr<RaftNode> raft_node) 
        : raft_node_(raft_node) {
            raft_node_->callback_reg.reg_callback("Put", this, &KvService::apply_put);
            raft_node_->callback_reg.reg_callback("Del", this, &KvService::apply_del);
            raft_node_->callback_reg.reg_callback("Cas", this, &KvService::apply_cas);
            raft_node_->callback_reg.reg_callback("PutWithTTL", this, &KvService::apply_put_with_ttl);
            raft_node_->callback_reg.reg_callback("TestFalse", this, &KvService::test_false);
            raft_node_->callback_reg.reg_callback("CasWithTTL", this, &KvService::apply_cas_with_ttl);
        }

    bool apply_put(const std::string& key, const std::string& value) {
        spdlog::info("Applying PUT: {} = {}", key, value);
        // 无限期存储，设置一个极大过期时间
        kv_store_[key] = ValueWithExpiry(value);

        auto it = watcher_reg.invokes_.find("put" );
        if (it != watcher_reg.invokes_.end()) {
            nlohmann::json args_json = nlohmann::json::array({key, value});
            it->second(args_json.dump());
            // watcher_reg.invokes_.erase(it);
        }
        return true;
    }

    bool apply_add_task_(const std::string& key, const std::string& value) {
        spdlog::info("Applying ADD_TASK: {} = {}", key, value);
        // 无限期存储，设置一个极大过期时间
        // kv_store_[key] = ValueWithExpiry(value);



        auto it = watcher_reg.invokes_.find("add_" + key);
        if (it != watcher_reg.invokes_.end()) {
            nlohmann::json args_json = nlohmann::json::array({key, value});
            it->second(args_json.dump());
            watcher_reg.invokes_.erase(it);
        }
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
                spdlog::info("Applying CAS: Key '{}' does not exist, expected empty. Setting to '{}' (from node{})", key, new_value,node_id_);
                kv_store_[key] = ValueWithExpiry(new_value);
                return true;
            }
            spdlog::info("Applying CAS: Key '{}' does not exist, expected '{}', CAS failed (from node{})", key, expected_value,node_id_);
            return false;
        }
        
        // 检查键是否已过期
        if (it->second.is_expired()) {
            spdlog::info("Applying CAS: Key '{}' is expired, treating as non-existent (from node{})", key,node_id_);
            kv_store_.erase(it);
            // 过期后的行为：如果期望值是空，可以设置新值
            if (expected_value.empty()) {
                spdlog::info("Applying CAS: After expiry, setting '{}' to '{}' (from node{})", key, new_value,node_id_);
                kv_store_[key] = ValueWithExpiry(new_value);
                return true;
            }
            return false;
        }

        // 比较当前值
        if (it->second.value == expected_value) {
            spdlog::info("Applying CAS: Key '{}' matches expected value. Updating to '{}' (from node{})", key, new_value,node_id_);
            it->second.value = new_value; // 更新值，保持原有过期时间
            return true;
        } else {
            spdlog::info("Applying CAS: Key '{}' value '{}' does not match expected '{}', CAS failed (from node{})", key, it->second.value, expected_value,node_id_);
            return false;
        }
    }

    bool apply_cas_with_ttl(const std::string& key, const std::string& expected_value, 
                        const std::string& new_value, long long ttl_ms) {
        auto it = kv_store_.find(key);
        // 键不存在 + 期望值为空 → 原子设置值+TTL
        if (it == kv_store_.end()) {
            if (expected_value.empty()) {
                spdlog::info("Applying CAS_TTL: Key '{}' does not exist, set to '{}' (TTL: {}ms  from node{})", 
                            key, new_value, ttl_ms,node_id_);
                auto expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(ttl_ms);
                kv_store_[key] = ValueWithExpiry(new_value, expiry);
                return true;
            }
            spdlog::info("Applying CAS_TTL: Key '{}' does not exist, expected '{}', failed (from node{})", 
                        key, expected_value,node_id_);
            return false;
        }

        // 键已过期 → 视为不存在
        if (it->second.is_expired()) {
            spdlog::info("Applying CAS_TTL: Key '{}' expired, treat as non-existent (from node{})", key,node_id_);
            kv_store_.erase(it);
            if (expected_value.empty()) {
                auto expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(ttl_ms);
                kv_store_[key] = ValueWithExpiry(new_value, expiry);
                return true;
            }
            return false;
        }

        // 期望值匹配 → 原子更新值+重置TTL
        if (it->second.value == expected_value) {
            spdlog::info("Applying CAS_TTL: Key '{}' match expected, update to '{}' (TTL: {}ms  from node{})", 
                        key, new_value, ttl_ms,node_id_);
            auto expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(ttl_ms);
            it->second = ValueWithExpiry(new_value, expiry);
            return true;
        } else {
            spdlog::info("Applying CAS_TTL: Key '{}' value '{}' != expected '{}', failed (from node{})", 
                        key, it->second.value, expected_value,node_id_);
            return false;
        }
    }

    bool test_false(){
        return false;
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

    int node_id_;
private:
    mutable std::mutex store_mutex_; // 保护 kv_store_ 的锁
    std::unordered_map<std::string, ValueWithExpiry> kv_store_;

public:
    // 安全的写入操作
    int64_t Put(const std::string& key, const std::string& value) {
        auto entry = raft_node_->pack_logentry("Put", key, value);
        auto result = raft_node_->submit(entry);
        if(result==-1){
            std::cout<<"Put 提交失败"<<std::endl;
        }
        return result; 
    }

    // 新增：安全的带超时写入操作
    int64_t PutWithTTL(const std::string& key, const std::string& value, long long ttl_ms) {
        auto entry = raft_node_->pack_logentry("PutWithTTL", key, value, ttl_ms);
        auto result = raft_node_->submit(entry);
        if(result==-1){
            std::cout<<"PutWithTTL 提交失败"<<std::endl;
        }
        return result;
    }

    // 安全的删除操作
    int64_t Del(const std::string& key) {
        auto entry = raft_node_->pack_logentry("Del", key);
        auto result = raft_node_->submit(entry);
        if(result==-1){
            std::cout<<"Del 提交失败"<<std::endl;
        }
        return result;
    }

    // 新增：安全的 CAS 操作
    // compare_and_swap: 如果 key 的当前值等于 expected_value，则将其更新为 new_value。
    // 返回值：true 表示操作成功，false 表示当前值与期望值不符，操作失败。
    int64_t Cas(const std::string& key, const std::string& expected_value, const std::string& new_value) {
        auto entry = raft_node_->pack_logentry("Cas", key, expected_value, new_value);
        auto result = raft_node_->submit(entry);
        if(result==-1){
            std::cout<<"Cas 提交失败"<<std::endl;
        }
        return result;
    }

    int64_t CasWithTTL(const std::string& key, const std::string& expected_value, 
                   const std::string& new_value, long long ttl_ms) {
        auto entry = raft_node_->pack_logentry("CasWithTTL", key, expected_value, new_value, ttl_ms);
        auto result = raft_node_->submit(entry);
        if (result == -1) {
            std::cout << "CasWithTTL 提交失败" << std::endl;
        }
        return result;
    }
    // 安全的读取操作 (这里采用"转发给 Leader"的简化模型)
    std::string Get(const std::string& key) {
        // Raft 读取优化：可以实现 ReadIndex 或 Lease-based Read 以降低延迟。
        // 现在的实现是通过提交一个日志来保证线性一致性读。
        auto entry = raft_node_->pack_logentry("barrier");
        auto result = raft_node_->submit(entry);
        if (result == -1) {
            std::cout<<"Get 提交失败"<<std::endl;
            return ""; // 操作失败
        }
        auto opt_value = unsafe_get(key);
        return opt_value ? *opt_value : "";
    }

    // --- 新增：检查键是否存在且未过期 ---
    bool Exists(const std::string& key) {
        auto entry = raft_node_->pack_logentry("barrier", key);
        auto result = raft_node_->submit(entry);
        if (result == -1) {
            std::cout<<"Exists 提交失败"<<std::endl;
            return false; // 操作失败
        }
        return unsafe_get(key).has_value();
    }

    // 新增：测试方法，返回 false
    bool TestFalse() {
        auto entry = raft_node_->pack_logentry("barrier");
        auto result = raft_node_->submit(entry);
        if (result == -1) {
            std::cout<<"TestFalse 提交失败"<<std::endl;
            return false; // 操作失败
        }
        std::cout<<"执行结果："<<result<<std::endl;
        return result;
    }

    bool get_reply_by_id(int64_t req_id){
        return raft_node_->wait_for(req_id);
    }

    bool Get_lock(std::string lock_name , long long lock_ttl , int64_t timeout_ms){
        // std::cout<<"Get_lock"<<std::endl;
        auto start_time = std::chrono::steady_clock::now();
        // std::cout << "\n--------------------正在尝试获取锁--------------------\n";
        auto lock_value=std::to_string(raft_node_->node_id_);
        while (1) {
            // 1. 检查是否超时
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start_time
            ).count();
            if (elapsed > timeout_ms) {
                // std::cout << "\n>>> 获取锁超时（" << timeout_ms << "ms),程序退出。\n";
                return false;
            }
            int64_t cas_ttl_req_id = CasWithTTL(lock_name, "", lock_value, lock_ttl);
            if (cas_ttl_req_id == -1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                continue;
            }
            bool cas_success = get_reply_by_id(cas_ttl_req_id);
            if(cas_success) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }

        return false;
    }
    bool Release_lock(std::string lock_name){
        auto lock_value=std::to_string(raft_node_->node_id_);
        int64_t cas_req_id = Cas(lock_name, lock_value, "");
        if (cas_req_id == -1) {
            // std::cout << "\n>>> 释放锁请求提交失败\n";
            return false;
        }
        bool cas_success = get_reply_by_id(cas_req_id);
        return cas_success;
    }
private:
    std::shared_ptr<RaftNode> raft_node_;

public:

    template<typename Function>
    void WATCH(std::string command_type,Function&& f) {
        watcher_reg.reg_callback(command_type, std::forward<Function>(f));
    }

    template<typename ClassType, typename ReturnType, typename... Args>
    void WATCH(std::string command_type, ClassType* instance, ReturnType (ClassType::*mem_func)(Args...)) {
        watcher_reg.reg_callback(command_type, instance, mem_func);
    }

    RegisterCallback watcher_reg;
};