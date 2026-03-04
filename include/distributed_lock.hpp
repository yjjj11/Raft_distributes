#pragma once
#include <string>
#include <map>
#include <chrono>
#include <logger.hpp>

struct LockInfo {
    std::string owner;
    int64_t expire_at;
    bool is_locked = false;
};

class DistributedLockManager {
public:
    bool is_locked(const std::string& name) {
        auto it = locks_.find(name);
        if (it == locks_.end() || !it->second.is_locked) return false;
        int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        return it->second.expire_at == 0 || it->second.expire_at > now;
    }

    std::string get_holder(const std::string& name) {
        return is_locked(name) ? locks_[name].owner : "None";
    }

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
