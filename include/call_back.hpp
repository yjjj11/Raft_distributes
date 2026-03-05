#pragma once
#include <string>
#include <nlohmann/json.hpp>
#include <iostream>
#include <unordered_map>
#include <functional>
#include <tuple>
#include <stdexcept>
#include <type_traits>
#include "struct.hpp"
#include "spdlog/spdlog.h"

class RegisterCallback
{
public:
    using invoke_type = std::function<bool (const std::string& buffer)>;
    std::unordered_map<std::string, invoke_type> invokes_;

    // ========== 核心：修复后的函数执行逻辑 ==========
    template<typename... Args>
    static bool invoke_func(const std::function<bool(Args...)>& func, const std::string& buffer) {
        try {
            auto json = nlohmann::json::parse(buffer);
            if (!json.is_array()) {
                spdlog::error("[Invoke Error] Expected JSON array, got {}", json.type_name());
                return false;
            }
            
            // 检查JSON数组大小是否与参数数量匹配
            if (json.size() != sizeof...(Args)) {
                spdlog::error("[Invoke Error] Argument count mismatch: expected {}, got {}", sizeof...(Args), json.size());
                return false;
            }
            
            // 不再尝试默认构造，而是直接声明，然后由 from_json 填充
            std::tuple<std::remove_const_t<std::remove_reference_t<Args>>...> args;
            nlohmann::from_json(json, args);
            return std::apply(func, std::move(args));
        } catch (const std::exception& e) {
            spdlog::error("[Invoke Error] {}", e.what());
            return false;
        }
    }

    // ========== 重载1：适配普通函数/lambda ==========
    template<typename Function>
    std::enable_if_t<!std::is_member_function_pointer_v<std::decay_t<Function>>>
    reg_callback(const std::string& command_type, Function&& f) {
        auto func = std::function(f);
        invokes_[command_type] = [func](const std::string& buffer) -> bool {
            return invoke_func(func, buffer);
        };
        spdlog::debug("注册普通回调 | 命令类型: {}", command_type);
    }

    // ========== 重载2：适配类的非静态成员函数 ==========
    template<typename ClassType, typename ReturnType, typename... Args>
    void reg_callback(const std::string& command_type, ClassType* instance, ReturnType (ClassType::*mem_func)(Args...)) {
        std::function<bool(Args...)> bound_func = [instance, mem_func](Args... args) -> bool {
            return (instance->*mem_func)(std::forward<Args>(args)...);
        };

        invokes_[command_type] = [bound_func](const std::string& buffer) -> bool {
            return invoke_func(bound_func, buffer);
        };
        spdlog::debug("注册成员函数回调 | 命令类型: {}", command_type);
    }

    // ========== 触发回调 ==========
    bool trigger_by_logentry(const LogEntry& entry) {
        auto it = invokes_.find(entry.command_type);
        if (it != invokes_.end()) {
            spdlog::debug("[Trigger] 处理 LogEntry | 命令类型: {}", entry.command_type);
            return it->second(entry.buffer);
        } else {
            spdlog::error("[Error] LogEntry命令类型未注册: {}", entry.command_type);
            return true;
        }
    }
};