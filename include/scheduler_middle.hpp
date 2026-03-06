
#pragma once
#include <string>
#include <chrono>
#include <thread>
#include <functional>
#include <memory>
#include <queue>
#include <mutex>
#include <optional>
#include <atomic>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

// 引入依赖的类型定义（保持与你的环境一致）
#include "kv_store.hpp"
#include <raftnode.hpp>
#include <random>

using json = nlohmann::json;

// ========== 1. 任务调度器类（仅负责调度和分发，无执行逻辑） ==========
class TaskScheduler {
public:
    // 构造函数
    TaskScheduler(std::shared_ptr<KvService> kv_service, std::shared_ptr<RaftNode> raft_node) 
        : kv_service_(kv_service),
          raft_node_(raft_node),
          stop_flag_(false),
          scheduler_thread_(nullptr) {
        
        // 注册KV操作监听
        kv_service_->WATCH("put", "task_content_and_status_update", [this](const std::string& key, const std::string& value) {
            if (key.substr(0, 5) == "task:") {
                return this->on_task_updated(key, value);
            } else if (key.substr(0, 12) == "task_status:") {
                return this->on_task_status_updated(key, value);
            }
            return true;
        });

        kv_service_->WATCH("PutWithTTL", "task_content_and_status_update_ttl", [this](const std::string& key, const std::string& value, long long ttl_ms) {
            (void)ttl_ms;
            if (key.substr(0, 5) == "task:") {
                return this->on_task_updated(key, value);
            } else if (key.substr(0, 12) == "task_status:") {
                return this->on_task_status_updated(key, value);
            }
            return true;
        });

        kv_service_->WATCH("Cas", "task_content_and_status_update_cas", [this](const std::string& key, const std::string& expected, const std::string& new_val, bool success) {
            if (success) {
                if (key.substr(0, 5) == "task:") {
                    return this->on_task_updated(key, new_val);
                } else if (key.substr(0, 12) == "task_status:") {
                    return this->on_task_status_updated(key, new_val);
                }
            }
            return true;
        });

        kv_service_->WATCH("CasWithTTL", "task_content_and_status_update_cas_ttl", [this](const std::string& key, const std::string& expected, const std::string& new_val, long long ttl_ms, bool success) {
            (void)ttl_ms;
            if (success) {
                if (key.substr(0, 5) == "task:") {
                    return this->on_task_updated(key, new_val);
                } else if (key.substr(0, 12) == "task_status:") {
                    return this->on_task_status_updated(key, new_val);
                }
            }
            return true;
        });

        kv_service_->WATCH("Del", "task_removed", [this](const std::string& key) {
            if (key.substr(0, 5) == "task:") {
                return this->on_task_removed(key);
            } else if (key.substr(0, 12) == "task_status:") {
                return this->on_task_status_removed(key);
            }
            return true;
        });
        
        spdlog::info("任务调度器 - 回调注册完成");
    }

    ~TaskScheduler() {
        stop();
    }

    // 初始化调度器（加载Pending任务到内存队列）
    void initialize_scheduler() {
        kv_service_->Put("scheduler:leader_lock", std::to_string(kv_service_->node_id_));
        spdlog::info("任务调度器 - 调度锁初始化获取完成");
        restore_tasks_from_kv();
    }

    // 判断当前节点是否为Leader
    bool is_leader() const {
        if (!raft_node_) {
            spdlog::warn("任务调度器 - RaftNode未初始化，默认返回非Leader状态");
            return false;
        }
        return raft_node_->is_leader(raft_node_->node_id_);
    }

    // 添加定时任务（仅存储，不执行）
    bool schedule_task(const ScheduledTask& task) {
        try {
            // 检查任务是否已存在
            std::string existing_task = kv_service_->Get("task:" + task.id);
            if (!existing_task.empty()) {
                spdlog::warn("任务调度器 - 任务 {} 已存在，执行更新逻辑", task.id);
                return update_task(task);
            }

            // 存储任务数据
            json task_json = task;
            int64_t put_result = kv_service_->Put("task:" + task.id, task_json.dump());
            if (put_result == -1) {
                spdlog::error("任务调度器 - 存储任务 {} 失败", task.id);
                return false;
            }

            // 设置任务状态为Pending
            TaskStatusInfo status_info;
            status_info.status = TaskStatus::Pending;
            status_info.created_at = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            status_info.updated_at = status_info.created_at;

            json status_json = status_info;
            int64_t status_result = kv_service_->Put("task_status:" + task.id, status_json.dump());
            if (status_result == -1) {
                spdlog::error("任务调度器 - 存储任务 {} 状态失败", task.id);
                return false;
            }

            // 更新任务列表
            update_task_list(task.id, true);

            spdlog::info("任务调度器 - 任务 {} 已添加，执行时间: {}", task.id, task.execute_at);
            return true;
        } catch (const std::exception& e) {
            spdlog::error("任务调度器 - 添加任务 {} 异常: {}", task.id, e.what());
            return false;
        }
    }

    // 更新任务
    bool update_task(const ScheduledTask& task) {
        try {
            json task_json = task;
            int64_t put_result = kv_service_->Put("task:" + task.id, task_json.dump());
            if (put_result == -1) {
                spdlog::error("任务调度器 - 更新任务 {} 失败", task.id);
                return false;
            }

            spdlog::info("任务调度器 - 任务 {} 已更新，执行时间: {}", task.id, task.execute_at);
            return true;
        } catch (const std::exception& e) {
            spdlog::error("任务调度器 - 更新任务 {} 异常: {}", task.id, e.what());
            return false;
        }
    }

    // 取消任务
    bool cancel_task(const TaskId& task_id) {
        try {
            // 删除任务数据
            int64_t del_result = kv_service_->Del("task:" + task_id);
            if (del_result == -1) {
                spdlog::error("任务调度器 - 删除任务 {} 失败", task_id);
                return false;
            }

            // 删除任务状态
            del_result = kv_service_->Del("task_status:" + task_id);
            if (del_result == -1) {
                spdlog::error("任务调度器 - 删除任务 {} 状态失败", task_id);
                return false;
            }

            // 删除执行器分配信息
            kv_service_->Del("task/" + task_id + "/executorid");

            // 更新任务列表
            update_task_list(task_id, false);

            // 从内存队列移除
            remove_task_from_memory_queue(task_id);

            spdlog::info("任务调度器 - 任务 {} 已取消", task_id);
            return true;
        } catch (const std::exception& e) {
            spdlog::error("任务调度器 - 取消任务 {} 异常: {}", task_id, e.what());
            return false;
        }
    }

    // 获取任务状态
    std::optional<TaskStatusInfo> get_task_status(const TaskId& task_id) {
        try {
            std::string status_str = kv_service_->Get("task_status:" + task_id);
            if (status_str.empty()) {
                return std::nullopt;
            }
            return json::parse(status_str).get<TaskStatusInfo>();
        } catch (const std::exception& e) {
            spdlog::error("任务调度器 - 获取任务 {} 状态异常: {}", task_id, e.what());
            return std::nullopt;
        }
    }

    // 启动调度器（仅Leader节点调用）
    void start() {
        if (scheduler_thread_ == nullptr) {
            stop_flag_ = false;
            initialize_scheduler();
            // 启动调度线程（仅负责分发任务）
            scheduler_thread_ = std::make_unique<std::thread>(&TaskScheduler::scheduler_loop, this);
            spdlog::info("任务调度器 - 已启动（当前节点为Leader）");
        } else {
            spdlog::warn("任务调度器 - 已处于运行状态，无需重复启动");
        }
    }

    // 停止调度器
    void stop() {
        stop_flag_ = true;
        
        if (scheduler_thread_ && scheduler_thread_->joinable()) {
            scheduler_thread_->join();
        }
        
        scheduler_thread_.reset();
        spdlog::info("任务调度器 - 已停止");
    }

    // 创建任务（字符串数据）
    ScheduledTask make_task_from_string(const std::string& task_type, 
                            const std::string& task_data, 
                            int64_t delay_seconds = 2,
                            const std::string& task_id = "") {
        ScheduledTask task;
        
        // 生成任务ID
        if (task_id.empty()) {
            auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(1000, 9999);
            task.id = "task_" + std::to_string(timestamp) + "_" + std::to_string(dis(gen));
        } else {
            task.id = task_id;
        }
        
        // 设置执行时间
        task.execute_at = std::chrono::duration_cast<std::chrono::milliseconds>(
            (std::chrono::system_clock::now() + std::chrono::seconds(delay_seconds)).time_since_epoch()
        ).count();
        
        // 构造payload
        try {
            json payload_json;
            payload_json["type"] = task_type;
            if (!task_data.empty()) {
                if (task_data.front() == '{' && task_data.back() == '}') {
                    payload_json["data"] = json::parse(task_data);
                } else {
                    payload_json["data"] = task_data;
                }
            }
            task.payload = payload_json.dump();
        } catch (const std::exception& e) {
            spdlog::error("任务调度器 - 构造任务 {} payload 失败: {}", task.id, e.what());
            task.payload = "{\"type\":\"" + task_type + "\",\"data\":\"" + task_data + "\"}";
        }
        
        // 默认状态
        task.status = TaskStatus::Pending;
        
        spdlog::info("任务调度器 - 创建任务 {} (类型: {}, 延迟: {}s)", task.id, task_type, delay_seconds);
        return task;
    }

    // 创建任务（JSON数据）
    ScheduledTask make_task_from_json(const std::string& task_type, 
                            const json& task_data, 
                            int64_t delay_seconds = 2,
                            const std::string& task_id = "") {
        return make_task_from_string(task_type, task_data.dump(), delay_seconds, task_id);
    }

    // 获取待分发的任务数量
    size_t get_pending_task_count() {
        std::lock_guard<std::mutex> lock(pending_tasks_mutex_);
        return pending_tasks_.size();
    }

private:
    // 任务队列项（仅用于调度）
    struct TaskQueueItem {
        TaskId id;
        ExecuteTimeMs execute_at;
        TaskStatus status;
        
        bool operator<(const TaskQueueItem& other) const {
            return execute_at > other.execute_at; // 最小堆，最早执行的任务在顶部
        }
    };

    // 从KV恢复Pending任务到内存队列
    void restore_tasks_from_kv() {
        try {
            std::string tasks_list_str = kv_service_->Get("scheduler:tasks_list");
            if (tasks_list_str.empty()) {
                spdlog::info("任务调度器 - 未找到任务列表，跳过恢复");
                return;
            }
            
            json tasks_list = json::parse(tasks_list_str);
            if (!tasks_list.is_array()) {
                spdlog::error("任务调度器 - 任务列表格式错误，跳过恢复");
                return;
            }
            
            for (const auto& task_id : tasks_list) {
                try {
                    TaskId tid = task_id.get<std::string>();
                    std::string task_str = kv_service_->Get("task:" + tid);
                    if (task_str.empty()) {
                        spdlog::warn("任务调度器 - 任务 {} 不存在，跳过恢复", tid);
                        continue;
                    }
                    
                    ScheduledTask task = json::parse(task_str).get<ScheduledTask>();
                    auto status_opt = get_task_status(tid);
                    
                    // 仅加载Pending状态任务
                    if (status_opt.has_value() && status_opt.value().status == TaskStatus::Pending) {
                        add_task_to_memory_queue(task);
                        spdlog::info("任务调度器 - 恢复Pending任务 {} 到内存队列", tid);
                    } else {
                        spdlog::info("任务调度器 - 任务 {} 状态非Pending，跳过恢复", tid);
                    }
                } catch (const std::exception& e) {
                    spdlog::error("任务调度器 - 解析任务 {} 失败: {}", task_id.get<std::string>(), e.what());
                }
            }
        } catch (const std::exception& e) {
            spdlog::error("任务调度器 - 恢复任务异常: {}", e.what());
        }
    }

    // 调度器主循环（核心：分发任务到执行器）
    void scheduler_loop() {
        spdlog::info("任务调度器 - 调度线程启动，开始分发任务");
        while (!stop_flag_) {
            try {
                distribute_pending_tasks();
                std::this_thread::sleep_for(std::chrono::milliseconds(100)); // 每100ms检查一次
            } catch (const std::exception& e) {
                spdlog::error("任务调度器 - 调度循环异常: {}", e.what());
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
        spdlog::info("任务调度器 - 调度线程停止");
    }

    // 分发待执行的Pending任务（核心逻辑）
    void distribute_pending_tasks() {
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        std::vector<TaskQueueItem> ready_tasks;
        
        // 筛选出到达执行时间的任务
        {
            std::lock_guard<std::mutex> lock(pending_tasks_mutex_);
            while (!pending_tasks_.empty()) {
                TaskQueueItem top = pending_tasks_.top();
                
                // 执行时间未到，退出循环
                if (top.execute_at > now) {
                    break;
                }

                // 校验任务状态（确保是Pending）
                auto status_opt = get_task_status(top.id);
                if (status_opt.has_value() && status_opt.value().status != TaskStatus::Pending) {
                    spdlog::info("任务调度器 - 任务 {} 状态非Pending，从队列移除", top.id);
                    pending_tasks_.pop();
                    continue;
                }
                
                // 加入待分发列表
                ready_tasks.emplace_back(std::move(top));
                pending_tasks_.pop();
            }
        }

        // 分发任务到执行器
        for (const auto& task_item : ready_tasks) {
            distribute_task_to_executor(task_item.id);
        }
    }

    // 分发单个任务到执行器（核心：写入KV的task/{id}/executorid键值对）
    void distribute_task_to_executor(const TaskId& task_id) {
        try {
            // 简单的执行器分配策略：轮询分配（你可以根据实际需求修改）
            static std::atomic<int> executor_index = 0;
            ExecutorId executor_id = "executor_" + std::to_string(executor_index++ % 3); // 假设有3个执行器
            
            // 写入KV：task/{id}/executorid = executor_id
            int64_t put_result = kv_service_->Put("task/" + task_id + "/executorid", executor_id);
            if (put_result == -1) {
                spdlog::error("任务调度器 - 分发任务 {} 到执行器 {} 失败", task_id, executor_id);
                // 重新加入队列
                add_task_back_to_queue(task_id);
                return;
            }

            // 更新任务状态为Executing
            update_task_status(task_id, TaskStatus::Executing);

            spdlog::info("任务调度器 - 任务 {} 已分发到执行器 {}", task_id, executor_id);
        } catch (const std::exception& e) {
            spdlog::error("任务调度器 - 分发任务 {} 异常: {}", task_id, e.what());
            // 重新加入队列
            add_task_back_to_queue(task_id);
        }
    }

    // 任务分发失败时重新加入队列
    void add_task_back_to_queue(const TaskId& task_id) {
        try {
            std::string task_str = kv_service_->Get("task:" + task_id);
            if (task_str.empty()) {
                spdlog::error("任务调度器 - 任务 {} 不存在，无法重新加入队列", task_id);
                return;
            }
            
            ScheduledTask task = json::parse(task_str).get<ScheduledTask>();
            add_task_to_memory_queue(task);
            spdlog::info("任务调度器 - 任务 {} 已重新加入调度队列", task_id);
        } catch (const std::exception& e) {
            spdlog::error("任务调度器 - 任务 {} 重新加入队列失败: {}", task_id, e.what());
        }
    }

    // 添加任务到内存队列
    void add_task_to_memory_queue(const ScheduledTask& task) {
        TaskQueueItem item;
        item.id = task.id;
        item.execute_at = task.execute_at;
        item.status = task.status;
        
        std::lock_guard<std::mutex> lock(pending_tasks_mutex_);
        pending_tasks_.push(item);
    }

    // 从内存队列移除任务
    void remove_task_from_memory_queue(const TaskId& task_id) {
        std::lock_guard<std::mutex> lock(pending_tasks_mutex_);
        
        std::priority_queue<TaskQueueItem> temp_queue;
        while (!pending_tasks_.empty()) {
            TaskQueueItem item = pending_tasks_.top();
            pending_tasks_.pop();
            if (item.id != task_id) {
                temp_queue.push(item);
            }
        }
        pending_tasks_ = std::move(temp_queue);
    }

    // 更新任务列表
    void update_task_list(const TaskId& task_id, bool add) {
        try {
            std::string tasks_list_str = kv_service_->Get("scheduler:tasks_list");
            json tasks_list = json::array();
            
            if (!tasks_list_str.empty()) {
                tasks_list = json::parse(tasks_list_str);
                if (!tasks_list.is_array()) {
                    tasks_list = json::array();
                }
            }
                
            if (add) {
                // 避免重复添加
                bool exists = false;
                for (const auto& id : tasks_list) {
                    if (id.get<std::string>() == task_id) {
                        exists = true;
                        break;
                    }
                }
                if (!exists) {
                    tasks_list.push_back(task_id);
                }
            } else {
                // 移除指定任务ID
                json new_list = json::array();
                for (const auto& id : tasks_list) {
                    if (id.get<std::string>() != task_id) {
                        new_list.push_back(id);
                    }
                }
                tasks_list = std::move(new_list);
            }
            
            // 保存到KV
            kv_service_->Put("scheduler:tasks_list", tasks_list.dump());
        } catch (const std::exception& e) {
            spdlog::error("任务调度器 - 更新任务列表失败: {}", e.what());
        }
    }

    // 更新任务状态
    void update_task_status(const TaskId& task_id, TaskStatus new_status, const std::string& error_msg = "") {
        try {
            TaskStatusInfo status_info;
            auto current_status = get_task_status(task_id);
            
            if (current_status.has_value()) {
                status_info = current_status.value();
            } else {
                status_info.created_at = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
            }
            
            status_info.status = new_status;
            status_info.updated_at = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            
            if (new_status == TaskStatus::Executing && status_info.started_at == 0) {
                status_info.started_at = status_info.updated_at;
            } else if (new_status == TaskStatus::Completed || new_status == TaskStatus::Failed) {
                status_info.completed_at = status_info.updated_at;
            }
            
            if (!error_msg.empty()) {
                status_info.error_message = error_msg;
            }
            
            // 保存状态到KV
            json status_json = status_info;
            kv_service_->Put("task_status:" + task_id, status_json.dump());
        } catch (const std::exception& e) {
            spdlog::error("任务调度器 - 更新任务 {} 状态失败: {}", task_id, e.what());
        }
    }

    // ========== KV事件回调 ==========
    bool on_task_updated(const std::string& key, const std::string& value) {
        try {
            spdlog::debug("任务调度器 - 任务内容更新: {}", key);
            ScheduledTask task = json::parse(value).get<ScheduledTask>();
            
            // 移除旧任务，重新添加（如果是Pending状态）
            remove_task_from_memory_queue(task.id);
            auto status_opt = get_task_status(task.id);
            if (status_opt.has_value() && status_opt.value().status == TaskStatus::Pending) {
                add_task_to_memory_queue(task);
            }
            
            return true;
        } catch (const std::exception& e) {
            spdlog::error("任务调度器 - 处理任务更新事件异常: {}", e.what());
            return false;
        }
    }

    bool on_task_status_updated(const std::string& key, const std::string& value) {
        try {
            TaskId task_id = key.substr(12);
            spdlog::debug("任务调度器 - 任务状态更新: {} -> {}", task_id, value);
            
            TaskStatusInfo status_info = json::parse(value).get<TaskStatusInfo>();
            
            // 非Pending状态，从内存队列移除
            if (status_info.status != TaskStatus::Pending) {
                remove_task_from_memory_queue(task_id);
            }
            
            return true;
        } catch (const std::exception& e) {
            spdlog::error("任务调度器 - 处理任务状态更新事件异常: {}", e.what());
            return false;
        }
    }

    bool on_task_removed(const std::string& key) {
        try {
            TaskId task_id = key.substr(5);
            spdlog::debug("任务调度器 - 任务删除事件: {}", task_id);
            
            remove_task_from_memory_queue(task_id);
            return true;
        } catch (const std::exception& e) {
            spdlog::error("任务调度器 - 处理任务删除事件异常: {}", e.what());
            return false;
        }
    }

    bool on_task_status_removed(const std::string& key) {
        try {
            TaskId task_id = key.substr(12);
            spdlog::debug("任务调度器 - 任务状态删除事件: {}", task_id);
            return true;
        } catch (const std::exception& e) {
            spdlog::error("任务调度器 - 处理任务状态删除事件异常: {}", e.what());
            return false;
        }
    }

    // 成员变量
    std::shared_ptr<KvService> kv_service_;
    std::shared_ptr<RaftNode> raft_node_;
    std::atomic<bool> stop_flag_;
    std::unique_ptr<std::thread> scheduler_thread_;
    
    // 待分发任务队列（最小堆）
    std::priority_queue<TaskQueueItem> pending_tasks_;
    mutable std::mutex pending_tasks_mutex_;
};


// 1. 创建KV和Raft节点
// auto raft_node = std::make_shared<RaftNode>();
// auto kv_service = std::make_shared<KvService>(raft_node);

// // 2. 创建并启动调度器（仅Leader节点）
// TaskScheduler scheduler(kv_service, raft_node);
// if (scheduler.is_leader()) {
//     scheduler.start();
    
//     // 添加测试任务
//     auto task = scheduler.make_task_from_string("test", "hello world", 1);
//     scheduler.schedule_task(task);
// }

// // 3. 创建并启动执行器
// TaskExecutor executor1(kv_service, "executor_0");
// TaskExecutor executor2(kv_service, "executor_1");
// TaskExecutor executor3(kv_service, "executor_2");

// // 注册任务处理器
// executor1.register_task_handler("test", [](const TaskPayload& payload) {
//     spdlog::info("执行器 executor_0 处理任务: {}", payload);
//     return true;
// });

// // 启动执行器
// executor1.start();
// executor2.start();
// executor3.start();