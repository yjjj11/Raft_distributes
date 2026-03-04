#pragma once

#include <iostream>
#include <string>
#include "call_back.hpp"

struct ConfigItem {
    std::string value;
    int64_t version;
    int64_t timestamp;
};

class ConfigCenter {
public:
    RegisterCallback watcher_reg;

    std::string GET(const std::string& key) {
        auto it = configs_.find(key);
        return it != configs_.end() ? it->second.value : "";
    }

    int64_t GET_VERSION(const std::string& key) {
        auto it = configs_.find(key);
        return it != configs_.end() ? it->second.version : -1;
    }

    void LIST() {
        if (configs_.empty()) {
            std::cout << "配置中心为空" << std::endl;
            return;
        }
        std::cout << "\n========== 配置列表 ==========" << std::endl;
        for (const auto& [key, item] : configs_) {
            std::cout << "Key: " << key << std::endl;
            std::cout << "  Value: " << item.value << std::endl;
            std::cout << "  Version: " << item.version << std::endl;
            std::cout << "  Updated: " << item.timestamp << std::endl;
            std::cout << "------------------------------" << std::endl;
        }
    }

    template<typename Function>
    void WATCH_SET(const std::string& key, Function&& f) {
        watcher_reg.reg_callback("WATCH_SET_" + key, std::forward<Function>(f));
    }

    template<typename ClassType, typename ReturnType, typename... Args>
    void WATCH_SET(const std::string& key, ClassType* instance, ReturnType (ClassType::*mem_func)(Args...)) {
        watcher_reg.reg_callback("WATCH_SET_" + key, instance, mem_func);
    }

    template<typename Function>
    void WATCH_DELETE(const std::string& key, Function&& f) {
        watcher_reg.reg_callback("WATCH_DELETE_" + key, std::forward<Function>(f));
    }

    template<typename ClassType, typename ReturnType, typename... Args>
    void WATCH_DELETE(const std::string& key, ClassType* instance, ReturnType (ClassType::*mem_func)(Args...)) {
        watcher_reg.reg_callback("WATCH_DELETE_" + key, instance, mem_func);
    }

    void apply_set(const std::string& key, const std::string& value, int64_t timestamp) {
        auto& item = configs_[key];
        item.value = value;
        item.version++;
        item.timestamp = timestamp;

        spdlog::warn("[ConfigCenter] SET: {} = {} (version: {})", key, value, item.version);

        auto it = watcher_reg.invokes_.find("WATCH_SET_" + key);
        if (it != watcher_reg.invokes_.end()) {
            nlohmann::json args_json = nlohmann::json::array({key, value});
            it->second(args_json.dump());
        }
    }

    void apply_delete(const std::string& key) {
        auto it = configs_.find(key);
        if (it != configs_.end()) {
            spdlog::warn("[ConfigCenter] DELETE: {} (was version: {})", key, it->second.version);
            configs_.erase(it);
            
            auto watcher_it = watcher_reg.invokes_.find("WATCH_DELETE_" + key);
            if (watcher_it != watcher_reg.invokes_.end()) {
                nlohmann::json args_json = nlohmann::json::array({key});
                watcher_it->second(args_json.dump());
            }
        }
    }

private:
    std::map<std::string, ConfigItem> configs_;
};