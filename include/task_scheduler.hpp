#pragma once
#include <string>
#include <chrono>
#include <thread>
#include <functional>
#include <memory>
#include <atomic>
#include <condition_variable>
#include <queue>
#include <mutex>
#include <map>
#include <set>
#include <algorithm>
#include <future>
#include <iostream>
#include <nlohmann/json.hpp>
#include "kv_store.hpp"
using json = nlohmann::json;
// 任务状态信息结构
struct TaskStatusInfo {
    TaskStatus status;
    int64_t created_at;
    int64_t updated_at;
    int64_t started_at = 0;
    int64_t completed_at = 0;
    std::string error_message = "";
};

// 为 TaskStatusInfo 定义序列化/反序列化
#define NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Type, ...)  \
    void to_json(nlohmann::json& nlohmann_json_j, const Type& nlohmann_json_t) { \
        NLOHMANN_JSON_EXPAND(NLOHMANN_JSON_PASTE(NLOHMANN_JSON_TO, __VA_ARGS__)) \
    } \
    void from_json(const nlohmann::json& nlohmann_json_j, Type& nlohmann_json_t) { \
        NLOHMANN_JSON_EXPAND(NLOHMANN_JSON_PASTE(NLOHMANN_JSON_FROM, __VA_ARGS__)) \
    }

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(TaskStatusInfo, status, created_at, updated_at, started_at, completed_at, error_message)

// 任务调度管理器 - 重构版本
class TaskScheduler {
public:
    TaskScheduler(std::shared_ptr<KvService> kv_service) 
        : kv_service_(kv_service), 
          stop_flag_(false),
          scheduler_thread_(nullptr),
          executor_thread_(nullptr) {
        
        // 注册KV事件监听器
        kv_service_->WATCH("task_added", this, &TaskScheduler::on_task_added);
        kv_service_->WATCH("task_updated", this, &TaskScheduler::on_task_updated);
        kv_service_->WATCH("task_removed", this, &TaskScheduler::on_task_removed);
        
        // 初始化调度器
        initialize_scheduler();
    }

    ~TaskScheduler() {
        shutdown();
    }

    // 初始化调度器
    void initialize_scheduler() {
        // 确保调度锁的初始状态
        kv_service_->Put("scheduler:leader_lock", std::to_string(kv_service_->node_id_));
        
        // 恢复任务状态
        restore_tasks_from_kv();
    }

    // 添加定时任务 - 简化版本，只负责存储任务
    bool schedule_task(const ScheduledTask& task) {
        try {
            // 序列化任务数据
            json task_json = task;
            std::string task_str = task_json.dump();

            // 检查任务是否已存在
            std::string existing_task = kv_service_->Get("task:" + task.id);
            if (!existing_task.empty()) {
                spdlog::warn("Task {} already exists, updating instead", task.id);
                return update_task(task);
            }

            // 存储任务数据 - 这会触发on_task_added事件
            int64_t put_result = kv_service_->Put("task:" + task.id, task_str);
            if (put_result == -1) {
                spdlog::error("Failed to store task {} in KV service", task.id);
                return false;
            }

            // 设置任务状态
            TaskStatusInfo status_info;
            status_info.status = TaskStatus::Pending;
            status_info.created_at = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            status_info.updated_at = status_info.created_at;

            json status_json = status_info;
            int64_t status_result = kv_service_->Put("task_status:" + task.id, status_json.dump());
            if (status_result == -1) {
                spdlog::error("Failed to store task status for {}", task.id);
                return false;
            }

            spdlog::info("Scheduled task {} for execution at {}", task.id, task.execute_at);
            return true;
        } catch (const std::exception& e) {
            spdlog::error("Exception in schedule_task: {}", e.what());
            return false;
        }
    }

    // 更新任务 - 简化版本，只负责更新存储
    bool update_task(const ScheduledTask& task) {
        try {
            // 序列化任务数据
            json task_json = task;
            std::string task_str = task_json.dump();

            // 更新任务数据 - 这会触发on_task_updated事件
            int64_t put_result = kv_service_->Put("task:" + task.id, task_str);
            if (put_result == -1) {
                spdlog::error("Failed to update task {} in KV service", task.id);
                return false;
            }

            spdlog::info("Updated task {} for execution at {}", task.id, task.execute_at);
            return true;
        } catch (const std::exception& e) {
            spdlog::error("Exception in update_task: {}", e.what());
            return false;
        }
    }

    // 取消任务
    bool cancel_task(const TaskId& task_id) {
        try {
            // 删除任务数据 - 这会触发on_task_removed事件
            int64_t del_result = kv_service_->Del("task:" + task_id);
            if (del_result == -1) {
                spdlog::error("Failed to delete task {} from KV service", task_id);
                return false;
            }

            // 删除任务状态
            del_result = kv_service_->Del("task_status:" + task_id);
            if (del_result == -1) {
                spdlog::error("Failed to delete task status for {}", task_id);
                return false;
            }

            spdlog::info("Cancelled task {}", task_id);
            return true;
        } catch (const std::exception& e) {
            spdlog::error("Exception in cancel_task: {}", e.what());
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

            json status_json = json::parse(status_str);
            TaskStatusInfo status_info = status_json.get<TaskStatusInfo>();
            return status_info;
        } catch (const std::exception& e) {
            spdlog::error("Exception in get_task_status for task {}: {}", task_id, e.what());
            return std::nullopt;
        }
    }

    // 启动调度器
    void start() {
        if (scheduler_thread_ == nullptr) {
            stop_flag_ = false;
            scheduler_thread_ = std::make_unique<std::thread>(&TaskScheduler::scheduler_loop, this);
            executor_thread_ = std::make_unique<std::thread>(&TaskScheduler::executor_loop, this);
        }
    }

    // 停止调度器
    void stop() {
        shutdown();
    }

    // 注册任务处理器
    void register_task_handler(const std::string& task_type, std::function<bool(const TaskPayload&)> handler) {
        std::lock_guard<std::mutex> lock(handlers_mutex_);
        task_handlers_[task_type] = handler;
    }

    // 获取待执行的任务数量
    size_t get_pending_task_count() {
        std::lock_guard<std::mutex> lock(pending_tasks_mutex_);
        return pending_tasks_.size();
    }

    // 获取任务总数
    size_t get_total_task_count() {
        std::string tasks_list = kv_service_->Get("scheduler:tasks_list");
        if (tasks_list.empty()) {
            return 0;
        }
        try {
            json tasks_json = json::parse(tasks_list);
            return tasks_json.size();
        } catch (...) {
            return 0;
        }
    }

private:
    // 任务队列项
    struct TaskQueueItem {
        TaskId id;
        ExecuteTimeMs execute_at;
        TaskStatus status;
        
        bool operator<(const TaskQueueItem& other) const {
            return execute_at > other.execute_at; // 最小堆，最早执行的任务在顶部
        }
    };

    // 从KV服务恢复任务
    void restore_tasks_from_kv() {
        try {
            // 从KV服务获取所有任务ID列表
            std::string tasks_list_str = kv_service_->Get("scheduler:tasks_list");
            if (!tasks_list_str.empty()) {
                json tasks_list = json::parse(tasks_list_str);
                
                for (const auto& task_id : tasks_list) {
                    std::string task_str = kv_service_->Get("task:" + task_id.get<std::string>());
                    if (!task_str.empty()) {
                        try {
                            json task_json = json::parse(task_str);
                            ScheduledTask task = task_json.get<ScheduledTask>();
                            
                            // 将任务添加到内存队列
                            add_task_to_memory_queue(task);
                        } catch (const std::exception& e) {
                            spdlog::error("Failed to parse task {}: {}", task_id.get<std::string>(), e.what());
                        }
                    }
                }
            }
        } catch (const std::exception& e) {
            spdlog::error("Exception during task restoration: {}", e.what());
        }
    }

    // 调度器主循环
    void scheduler_loop() {
        while (!stop_flag_) {
            try {
                process_scheduled_tasks();
                std::this_thread::sleep_for(std::chrono::milliseconds(100)); // 每100ms检查一次
            } catch (const std::exception& e) {
                spdlog::error("Exception in scheduler loop: {}", e.what());
                std::this_thread::sleep_for(std::chrono::seconds(1)); // 出错时等待1秒再继续
            }
        }
    }

    // 执行器主循环
    void executor_loop() {
        while (!stop_flag_) {
            try {
                process_execution_queue();
                std::this_thread::sleep_for(std::chrono::milliseconds(50)); // 每50ms检查一次执行队列
            } catch (const std::exception& e) {
                spdlog::error("Exception in executor loop: {}", e.what());
                std::this_thread::sleep_for(std::chrono::seconds(1)); // 出错时等待1秒再继续
            }
        }
    }

    // 处理定时任务
    void process_scheduled_tasks() {
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        std::vector<TaskQueueItem> ready_tasks;
        
        // 获取准备执行的任务
        {
            std::lock_guard<std::mutex> lock(pending_tasks_mutex_);
            while (!pending_tasks_.empty()) {
                TaskQueueItem top = pending_tasks_.top();
                if (top.execute_at <= now) {
                    ready_tasks.emplace_back(std::move(top));
                    pending_tasks_.pop();
                } else {
                    break; // 队列头部还未到执行时间
                }
            }
        }

        // 将准备执行的任务加入执行队列
        for (const auto& task_item : ready_tasks) {
            add_to_execution_queue(task_item.id);
        }
    }

    // 处理执行队列
    void process_execution_queue() {
        std::unique_lock<std::mutex> lock(execution_queue_mutex_);
        if (!execution_queue_.empty()) {
            TaskId task_id = execution_queue_.front();
            execution_queue_.pop();
            lock.unlock();

            // 执行任务
            execute_task(task_id);
        }
    }

    // 执行单个任务
    bool execute_task(const TaskId& task_id) {
        try {
            // 获取任务数据
            std::string task_str = kv_service_->Get("task:" + task_id);
            if (task_str.empty()) {
                spdlog::error("Task {} not found in KV service", task_id);
                return false;
            }

            json task_json = json::parse(task_str);
            ScheduledTask task = task_json.get<ScheduledTask>();

            // 更新任务状态为执行中
            update_task_status(task_id, TaskStatus::Executing);

            // 执行任务
            bool success = execute_task_payload(task);

            // 更新任务状态
            if (success) {
                update_task_status(task_id, TaskStatus::Completed);
                spdlog::info("Successfully executed task {}", task_id);
            } else {
                update_task_status(task_id, TaskStatus::Failed);
                spdlog::error("Failed to execute task {}", task_id);
            }

            return success;
        } catch (const std::exception& e) {
            spdlog::error("Exception executing task {}: {}", task_id, e.what());
            update_task_status(task_id, TaskStatus::Failed, e.what());
            return false;
        }
    }

    // 执行任务负载
    bool execute_task_payload(const ScheduledTask& task) {
        std::lock_guard<std::mutex> lock(handlers_mutex_);
        
        // 解析任务类型
        try {
            json payload_json = json::parse(task.payload);
            std::string task_type = payload_json.value("type", "default");
            
            auto it = task_handlers_.find(task_type);
            if (it != task_handlers_.end()) {
                return it->second(task.payload);
            } else {
                spdlog::warn("No handler registered for task type: {}", task_type);
                return false;
            }
        } catch (...) {
            spdlog::error("Invalid payload format for task: {}", task.id);
            return false;
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
        
        // 更新任务列表
        update_task_list(task.id, true);
    }

    // 从内存队列移除任务
    void remove_task_from_memory_queue(const TaskId& task_id) {
        std::lock_guard<std::mutex> lock(pending_tasks_mutex_);
        
        // 由于priority_queue没有直接的remove方法，我们需要重建队列
        std::priority_queue<TaskQueueItem> temp_queue;
        while (!pending_tasks_.empty()) {
            TaskQueueItem item = pending_tasks_.top();
            pending_tasks_.pop();
            if (item.id != task_id) {
                temp_queue.push(item);
            }
        }
        pending_tasks_ = std::move(temp_queue);
        
        // 更新任务列表
        update_task_list(task_id, false);
    }

    // 更新任务列表
    void update_task_list(const TaskId& task_id, bool add) {
        try {
            std::string tasks_list_str = kv_service_->Get("scheduler:tasks_list");
            json tasks_list;
            
            if (!tasks_list_str.empty()) {
                tasks_list = json::parse(tasks_list_str);
            }
            
            if (add) {
                // 添加任务ID到列表
                bool exists = false;
                for (const auto& existing_id : tasks_list) {
                    if (existing_id.get<std::string>() == task_id) {
                        exists = true;
                        break;
                    }
                }
                if (!exists) {
                    tasks_list.emplace_back(task_id);
                }
            } else {
                // 从列表中移除任务ID
                json new_list = json::array();
                for (const auto& existing_id : tasks_list) {
                    if (existing_id.get<std::string>() != task_id) {
                        new_list.emplace_back(existing_id);
                    }
                }
                tasks_list = std::move(new_list);
            }
            
            // 保存更新后的列表
            kv_service_->Put("scheduler:tasks_list", tasks_list.dump());
        } catch (const std::exception& e) {
            spdlog::error("Failed to update task list: {}", e.what());
        }
    }

    // 更新任务状态
    void update_task_status(const TaskId& task_id, TaskStatus new_status, const std::string& error_msg = "") {
        try {
            // 获取当前状态
            std::optional<TaskStatusInfo> current_status = get_task_status(task_id);
            TaskStatusInfo status_info;
            
            if (current_status.has_value()) {
                status_info = current_status.value();
            } else {
                status_info.created_at = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
            }
            
            // 更新状态信息
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
            
            // 保存状态
            json status_json = status_info;
            kv_service_->Put("task_status:" + task_id, status_json.dump());
        } catch (const std::exception& e) {
            spdlog::error("Failed to update task status for {}: {}", task_id, e.what());
        }
    }

    // 添加任务到执行队列
    void add_to_execution_queue(const TaskId& task_id) {
        std::lock_guard<std::mutex> lock(execution_queue_mutex_);
        execution_queue_.push(task_id);
    }

    // 事件处理回调 - 现在这些函数负责队列管理
    void on_task_added(const std::string& params) {
        try {
            json params_json = json::parse(params);
            if (params_json.size() >= 2) {
                std::string key = params_json[0];
                std::string value = params_json[1];
                
                if (key.substr(0, 5) == "task:") {
                    spdlog::info("Task added event: {}", key);
                    // 从KV事件中解析任务并添加到内存队列
                    json task_json = json::parse(value);
                    ScheduledTask task = task_json.get<ScheduledTask>();
                    add_task_to_memory_queue(task);
                }
            }
        } catch (const std::exception& e) {
            spdlog::error("Exception in on_task_added: {}", e.what());
        }
    }

    void on_task_updated(const std::string& params) {
        try {
            json params_json = json::parse(params);
            if (params_json.size() >= 2) {
                std::string key = params_json[0];
                std::string value = params_json[1];
                
                if (key.substr(0, 5) == "task:") {
                    spdlog::info("Task updated event: {}", key);
                    // 从KV事件中解析任务并更新内存队列
                    json task_json = json::parse(value);
                    ScheduledTask task = task_json.get<ScheduledTask>();
                    
                    // 先移除旧任务，再添加新任务
                    remove_task_from_memory_queue(task.id);
                    add_task_to_memory_queue(task);
                }
            }
        } catch (const std::exception& e) {
            spdlog::error("Exception in on_task_updated: {}", e.what());
        }
    }

    void on_task_removed(const std::string& params) {
        try {
            json params_json = json::parse(params);
            if (params_json.size() >= 1) {
                std::string key = params_json[0];
                
                if (key.substr(0, 5) == "task:") {
                    std::string task_id = key.substr(5); // 移除 "task:" 前缀
                    spdlog::info("Task removed event: {}", task_id);
                    // 从内存队列中移除任务
                    remove_task_from_memory_queue(task_id);
                }
            }
        } catch (const std::exception& e) {
            spdlog::error("Exception in on_task_removed: {}", e.what());
        }
    }

    // 关闭调度器
    void shutdown() {
        stop_flag_ = true;
        
        if (scheduler_thread_ && scheduler_thread_->joinable()) {
            scheduler_thread_->join();
        }
        
        if (executor_thread_ && executor_thread_->joinable()) {
            executor_thread_->join();
        }
    }

    // 成员变量
    std::shared_ptr<KvService> kv_service_;
    std::atomic<bool> stop_flag_;
    std::unique_ptr<std::thread> scheduler_thread_;
    std::unique_ptr<std::thread> executor_thread_;
    
    // 任务调度队列（最小堆，按执行时间排序）
    std::priority_queue<TaskQueueItem> pending_tasks_;
    mutable std::mutex pending_tasks_mutex_;
    
    // 任务执行队列
    std::queue<TaskId> execution_queue_;
    mutable std::mutex execution_queue_mutex_;
    
    // 任务处理器映射
    std::map<std::string, std::function<bool(const TaskPayload&)>> task_handlers_;
    mutable std::mutex handlers_mutex_;
    
    // 统计信息
    std::atomic<size_t> total_executed_{0};
    std::atomic<size_t> total_failed_{0};
};

// 示例日志库模拟
namespace spdlog {
    template<typename... Args>
    void info(const char* fmt, Args&&... args) {
        printf("[INFO] ");
        printf(fmt, args...);
        printf("\n");
    }
    
    template<typename... Args>
    void warn(const char* fmt, Args&&... args) {
        printf("[WARN] ");
        printf(fmt, args...);
        printf("\n");
    }
    
    template<typename... Args>
    void error(const char* fmt, Args&&... args) {
        printf("[ERROR] ");
        printf(fmt, args...);
        printf("\n");
    }
}

// 使用示例
void example_usage() {
    // 假设kv_service已经初始化
    // auto kv_service = std::make_shared<KvService>(raft_node);
    // auto scheduler = std::make_shared<TaskScheduler>(kv_service);
    
    /*
    // 注册任务处理器
    scheduler->register_task_handler("data_process", [](const TaskPayload& payload) {
        // 实际的任务处理逻辑
        spdlog::info("Processing data task: {}", payload);
        return true; // 返回执行结果
    });
    
    // 创建并调度任务
    ScheduledTask task;
    task.id = "task_001";
    task.execute_at = std::chrono::duration_cast<std::chrono::milliseconds>(
        (std::chrono::system_clock::now() + std::chrono::minutes(1)).time_since_epoch()
    ).count();
    task.payload = R"({"type":"data_process","data":"some_data"})";
    task.status = TaskStatus::Pending;
    
    if (scheduler->schedule_task(task)) {
        spdlog::info("Task scheduled successfully");
    } else {
        spdlog::error("Failed to schedule task");
    }
    
    // 启动调度器
    scheduler->start();
    */
}
