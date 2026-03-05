#include "call_back.hpp"
#include <iostream>

template<typename... Args>
LogEntry pack_logentry(const std::string& command_type, Args&&... args) {
    LogEntry entry;
    entry.command_type = command_type;
    // 核心：将任意参数打包为 JSON 数组
    nlohmann::json args_json = nlohmann::json::array({std::forward<Args>(args)...});
    entry.buffer = args_json.dump();  // 序列化到 buffer
    return entry;
}
bool set(const std::string& key, const std::string& value) {
    // 模拟业务逻辑：存储 key-value
    std::cout << "[Set Function] 执行 set 操作 | key: " << key << ", value: " << value << std::endl;
    return true;
}
int main() {
    // 1. 创建注册器实例
    RegisterCallback callback_reg;
    callback_reg.reg_callback("set_kv", set);
    // 2. 模拟调用 set_kv 命令
    std::string key = "name";
    std::string value = "Raft";
    LogEntry entry = pack_logentry("set_kv", key, value);
    callback_reg.trigger_by_logentry(entry);


    return 0;
}