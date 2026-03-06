#pragma once
#include <string>
#include <chrono>
#include <thread>
#include <functional>
#include <memory>
#include <queue>
#include <mutex>
#include <map>
#include <nlohmann/json.hpp>
#include "kv_store.hpp"
#include <random>
#include <atomic>
#include <spdlog/spdlog.h>

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
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(TaskStatusInfo, status, created_at, updated_at, started_at, completed_at, error_message)

// 任务调度管理器 - 恢复Leader判断逻辑，需手动启动
class TaskScheduler {
public:
    // 构造函数保留RaftNode参数，但不再自动处理Leader状态，仅保存引用供外部判断
    TaskScheduler(std::shared_ptr<KvService> kv_service, std::shared_ptr<RaftNode> raft_node) 
        : kv_service_(kv_service),
          raft_node_(raft_node),
          stop_flag_(false),
          scheduler_thread_(nullptr),
          executor_thread_(nullptr) {
        
        // 1. 注册 Put 操作监听
        // - task: 前缀 → 任务内容更新（on_task_updated）
        // - task_status: 前缀 → 任务状态更新（on_task_status_updated）
        kv_service_->WATCH("put", "task_content_and_status_update", [this](const std::string& key, const std::string& value) {
            if (key.substr(0, 5) == "task:") {
                return this->on_task_updated(key, value);
            } else if (key.substr(0, 12) == "task_status:") {
                return this->on_task_status_updated(key, value);
            }
            return true;
        });

        // 2. 注册 PutWithTTL 操作监听（同 Put 逻辑）
        kv_service_->WATCH("PutWithTTL", "task_content_and_status_update_ttl", [this](const std::string& key, const std::string& value, long long ttl_ms) {
            (void)ttl_ms; // 屏蔽未使用参数警告
            if (key.substr(0, 5) == "task:") {
                return this->on_task_updated(key, value);
            } else if (key.substr(0, 12) == "task_status:") {
                return this->on_task_status_updated(key, value);
            }
            return true;
        });

        // 3. 注册 Cas 操作监听
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

        // 4. 注册 CasWithTTL 操作监听
        kv_service_->WATCH("CasWithTTL", "task_content_and_status_update_cas_ttl", [this](const std::string& key, const std::string& expected, const std::string& new_val, long long ttl_ms, bool success) {
            (void)ttl_ms; // 屏蔽未使用参数警告
            if (success) {
                if (key.substr(0, 5) == "task:") {
                    return this->on_task_updated(key, new_val);
                } else if (key.substr(0, 12) == "task_status:") {
                    return this->on_task_status_updated(key, new_val);
                }
            }
            return true;
        });

        // 5. 注册 Del 操作监听
        kv_service_->WATCH("Del", "task_removed", [this](const std::string& key) {
            if (key.substr(0, 5) == "task:") {
                return this->on_task_removed(key);
            } else if (key.substr(0, 12) == "task_status:") {
                return this->on_task_status_removed(key);
            }
            return true;
        });
        
        spdlog::info("回调注册完成");
        
    }

    ~TaskScheduler() {
        stop();
    }

    // 初始化调度器（仅初始化锁和恢复任务，不启动线程）
    void initialize_scheduler() {
        // 确保调度锁的初始状态
        kv_service_->Put("scheduler:leader_lock", std::to_string(kv_service_->node_id_));
        
        spdlog::info("调度锁初始化获取完成");

        // 恢复任务状态（仅加载任务到内存，不启动执行）
        restore_tasks_from_kv();
    }

    // 新增：判断当前节点是否为Leader（供外部调用）
    bool is_leader() const {
        if (!raft_node_) {
            spdlog::warn("RaftNode未初始化，默认返回非Leader状态");
            return false;
        }
        return raft_node_->is_leader(raft_node_->node_id_);
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

            // 存储任务数据 - 触发on_task_updated事件
            int64_t put_result = kv_service_->Put("task:" + task.id, task_str);
            if (put_result == -1) {
                spdlog::error("Failed to store task {} in KV service", task.id);
                return false;
            }

            // 设置任务状态 - 触发on_task_status_updated事件
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

            // 更新任务数据 - 触发on_task_updated事件
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
            // 删除任务数据 - 触发on_task_removed事件
            int64_t del_result = kv_service_->Del("task:" + task_id);
            if (del_result == -1) {
                spdlog::error("Failed to delete task {} from KV service", task_id);
                return false;
            }

            // 删除任务状态 - 触发on_task_status_removed事件
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

    // 启动调度器（仅当外部确认是Leader时调用）
    void start() {
        // 双重检查：避免重复启动
        if (scheduler_thread_ == nullptr && executor_thread_ == nullptr) {
            stop_flag_ = false;
            // 初始化调度器（加载任务）
            initialize_scheduler();
            // 启动调度和执行线程
            scheduler_thread_ = std::make_unique<std::thread>(&TaskScheduler::scheduler_loop, this);
            executor_thread_ = std::make_unique<std::thread>(&TaskScheduler::executor_loop, this);
            spdlog::info("任务调度器已启动（当前节点为Leader）");
        } else {
            spdlog::warn("调度器已处于运行状态，无需重复启动");
        }
    }

    // 停止调度器
    void stop() {
        stop_flag_ = true;
        
        // 停止调度/执行线程
        if (scheduler_thread_ && scheduler_thread_->joinable()) {
            scheduler_thread_->join();
        }
        if (executor_thread_ && executor_thread_->joinable()) {
            executor_thread_->join();
        }
        
        scheduler_thread_.reset();
        executor_thread_.reset();
        spdlog::info("任务调度器已停止");
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

    ScheduledTask make_task_from_string(const std::string& task_type, 
                            const std::string& task_data, 
                            int64_t delay_seconds = 2,
                            const std::string& task_id = "") {
        ScheduledTask task;
        
        // 1. 生成/设置任务ID（自动生成规则：task_时间戳_随机数）
        if (task_id.empty()) {
            auto now = std::chrono::system_clock::now();
            auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(1000, 9999);
            task.id = "task_" + std::to_string(timestamp) + "_" + std::to_string(dis(gen));
        } else {
            task.id = task_id;
        }
        
        // 2. 设置执行时间（当前时间 + 延迟秒数）
        task.execute_at = std::chrono::duration_cast<std::chrono::milliseconds>(
            (std::chrono::system_clock::now() + std::chrono::seconds(delay_seconds)).time_since_epoch()
        ).count();
        
        // 3. 构造任务payload（自动拼接type和data）
        try {
            json payload_json;
            payload_json["type"] = task_type;
            // 解析业务数据并合并（支持JSON对象/纯字符串）
            if (!task_data.empty()) {
                if (task_data.front() == '{' && task_data.back() == '}') {
                    payload_json["data"] = json::parse(task_data);
                } else {
                    payload_json["data"] = task_data;
                }
            }
            task.payload = payload_json.dump();
        } catch (const std::exception& e) {
            spdlog::error("Failed to make task payload: {}", e.what());
            // 降级为简单拼接
            task.payload = "{\"type\":\"" + task_type + "\",\"data\":\"" + task_data + "\"}";
        }
        
        // 4. 设置默认状态
        task.status = TaskStatus::Pending;
        
        spdlog::info("Created task {} (type: {}, delay: {}s)", task.id, task_type, delay_seconds);
        return task;
    }

    // 重载：支持直接传入JSON对象作为任务数据
    ScheduledTask make_task_from_json(const std::string& task_type, 
                            const json& task_data, 
                            int64_t delay_seconds = 2,
                            const std::string& task_id = "") {
        return make_task_from_string(task_type, task_data.dump(), delay_seconds, task_id);
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
        if (tasks_list_str.empty()) {
            spdlog::info("No tasks list found, skip restore");
            return;
        }
        
        json tasks_list;
        try {
            tasks_list = json::parse(tasks_list_str);
            if (!tasks_list.is_array()) {
                spdlog::error("Tasks list is not an array, skip restore");
                return;
            }
        } catch (const json::parse_error& e) {
            spdlog::error("Parse tasks list failed: {}, skip restore", e.what());
            return;
        }
        
        for (const auto& task_id : tasks_list) {
            try {
                TaskId tid = task_id.get<std::string>();
                std::string task_str = kv_service_->Get("task:" + tid);
                if (task_str.empty()) {
                    spdlog::warn("Task {} not found in KV, skip restore", tid);
                    continue;
                }
                
                json task_json = json::parse(task_str);
                ScheduledTask task = task_json.get<ScheduledTask>();

                // ========== 核心修改：只加载 Pending 状态的任务 ==========
                std::optional<TaskStatusInfo> status_opt = get_task_status(tid);
                if (!status_opt.has_value()) {
                    spdlog::warn("Task {} has no status info, skip restore", tid);
                    continue;
                }
                
                TaskStatusInfo status_info = status_opt.value();
                if (status_info.status != TaskStatus::Pending) {
                    spdlog::info("Task {} status is {}, skip restore", tid, static_cast<int>(status_info.status));
                    continue;
                }
                // ======================================================
                
                // 将任务添加到内存队列
                add_task_to_memory_queue(task);
                spdlog::info("Restored pending task {} from KV", tid);
            } catch (const std::exception& e) {
                spdlog::error("Failed to parse task {}: {}", task_id.get<std::string>(), e.what());
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
            
            // 先判断执行时间
            if (top.execute_at > now) {
                break; // 队列头部还未到执行时间
            }

            // ========== 核心修改：执行前校验任务状态 ==========
            std::optional<TaskStatusInfo> status_opt = get_task_status(top.id);
            if (status_opt.has_value()) {
                TaskStatusInfo status_info = status_opt.value();
                if (status_info.status != TaskStatus::Pending) {
                    // 状态已变更，从队列中移除，不加入执行队列
                    spdlog::info("Task {} status is {}, remove from pending queue", 
                                top.id, static_cast<int>(status_info.status));
                    pending_tasks_.pop();
                    continue;
                }
            }
            // ======================================================
            
            // 时间到了且状态是Pending，加入执行队列
            ready_tasks.emplace_back(std::move(top));
            pending_tasks_.pop();
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

            // 更新任务状态为执行中（会触发on_task_status_updated）
            update_task_status(task_id, TaskStatus::Executing);

            // 执行任务
            bool success = execute_task_payload(task);

            // 更新任务状态（会触发on_task_status_updated）
            if (success) {
                update_task_status(task_id, TaskStatus::Completed);
                spdlog::info("Successfully executed task {}", task_id);
            } else {
                update_task_status(task_id, TaskStatus::Failed);
                spdlog::error("Failed to execute task {}", task_id);
            }

            remove_task_from_memory_queue(task_id);
            return success;
        } catch (const std::exception& e) {
            spdlog::error("Exception executing task {}: {}", task_id, e.what());
            update_task_status(task_id, TaskStatus::Failed, e.what());
            remove_task_from_memory_queue(task_id);
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
                try {
                    tasks_list = json::parse(tasks_list_str);
                    // 确保是数组类型
                    if (!tasks_list.is_array()) {
                        tasks_list = json::array();
                    }
                } catch (const json::parse_error& e) {
                    spdlog::error("Parse tasks list failed: {}, reset to empty array", e.what());
                    tasks_list = json::array();
                }
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
            int64_t put_result = kv_service_->Put("scheduler:tasks_list", tasks_list.dump());
            if (put_result == -1) {
                spdlog::error("Failed to update tasks list for task {}", task_id);
            }
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
            
            // 保存状态（会触发on_task_status_updated）
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

    // ==================== 拆分后的回调函数 ====================
    // 1. 任务内容更新（task: 前缀）- 处理任务本身的修改
    bool on_task_updated(const std::string& key, const std::string& value) {
        try {
            spdlog::info("[Task Content Update] Task updated event: {}", key);
            json task_json = json::parse(value);
            ScheduledTask task = task_json.get<ScheduledTask>();
            
            // 移除旧任务，添加新任务（更新内存队列）
            remove_task_from_memory_queue(task.id);
            add_task_to_memory_queue(task);
            return true;
        } catch (const std::exception& e) {
            spdlog::error("Exception in on_task_updated: {}", e.what());
            return false;
        }
    }

    // 2. 任务状态更新（task_status: 前缀）- 仅处理状态变更，不修改任务队列
    bool on_task_status_updated(const std::string& key, const std::string& value) {
        try {
            std::string task_id = key.substr(12); // 截取 task_status: 后的任务ID
            spdlog::info("[Task Status Update] Task status updated event: {} -> {}", task_id, value);
            
            // 解析状态信息（可根据需要扩展状态变更后的逻辑）
            json status_json = json::parse(value);
            TaskStatusInfo status_info = status_json.get<TaskStatusInfo>();
            
            // 示例：如果任务状态变为 Failed，可记录告警/重试等逻辑
            if (status_info.status == TaskStatus::Failed) {
                spdlog::warn("Task {} failed: {}", task_id, status_info.error_message);
            }
            return true;
        } catch (const std::exception& e) {
            spdlog::error("Exception in on_task_status_updated: {}", e.what());
            return false;
        }
    }

    // 3. 任务删除（task: 前缀）- 移除内存队列中的任务
    bool on_task_removed(const std::string& key) {
        try {
            std::string task_id = key.substr(5); // 截取 task: 后的任务ID
            spdlog::info("[Task Remove] Task removed event: {}", task_id);
            
            // 从内存队列移除任务
            remove_task_from_memory_queue(task_id);
            return true;
        } catch (const std::exception& e) {
            spdlog::error("Exception in on_task_removed: {}", e.what());
            return false;
        }
    }

    // 4. 任务状态删除（task_status: 前缀）- 仅清理状态相关逻辑
    bool on_task_status_removed(const std::string& key) {
        try {
            std::string task_id = key.substr(12); // 截取 task_status: 后的任务ID
            spdlog::info("[Task Status Remove] Task status removed event: {}", task_id);
            
            // 可添加状态删除后的清理逻辑（如记录日志、统计等）
            return true;
        } catch (const std::exception& e) {
            spdlog::error("Exception in on_task_status_removed: {}", e.what());
            return false;
        }
    }

    // 关闭调度器（内部调用）
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
    std::shared_ptr<RaftNode> raft_node_;      // 保留RaftNode引用供外部判断Leader
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