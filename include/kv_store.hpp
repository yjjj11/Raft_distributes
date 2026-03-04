#pragma once
#include <string>
#include <unordered_map>
#include <logger.hpp>

class KvStore {
public:
    void put(const std::string& key, const std::string& value) {
        spdlog::warn("----------------------------Apply: {} = {}---------------------------", key, value);
        kv_store_[key] = value;
    }

    std::string get(const std::string& key) {
        auto it = kv_store_.find(key);
        return it != kv_store_.end() ? it->second : "";
    }
    void del(const std::string& key) {
        spdlog::info("Applying DELETE: {}", key);
        auto it = kv_store_.find(key);
        if (it != kv_store_.end()) {
            kv_store_.erase(it);
            spdlog::info("DELETE: {} success", key);
        }
    }

private:
    std::unordered_map<std::string, std::string> kv_store_;
};
