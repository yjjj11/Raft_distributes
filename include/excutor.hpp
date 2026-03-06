#include "kv_store.hpp"
#include <unordered_set>
class TaskExecutor {
public:
    // 构造函数
    TaskExecutor(std::shared_ptr<KvService> kv_service, const ExecutorId& executor_id)
        : kv_service_(kv_service),
          executor_id_(executor_id),
          stop_flag_(false),
          worker_thread_(nullptr) {
        spdlog::info("任务执行器 {} - 初始化完成", executor_id_);
    }

    ~TaskExecutor() {
        stop();
    }

    // 注册任务处理器（按任务类型）
    void register_task_handler(const std::string& task_type, std::function<bool(const TaskPayload&)> handler) {
        std::lock_guard<std::mutex> lock(handlers_mutex_);
        task_handlers_[task_type] = handler;
        spdlog::info("任务执行器 {} - 注册任务处理器: {}", executor_id_, task_type);
    }

    // 启动执行器（轮询KV获取分配给自己的任务）
    void start() {
        if (worker_thread_ == nullptr) {
            stop_flag_ = false;
            // 启动工作线程
            worker_thread_ = std::make_unique<std::thread>(&TaskExecutor::worker_loop, this);
            spdlog::info("任务执行器 {} - 已启动", executor_id_);
        } else {
            spdlog::warn("任务执行器 {} - 已处于运行状态，无需重复启动", executor_id_);
        }
    }

    // 停止执行器
    void stop() {
        stop_flag_ = true;
        
        if (worker_thread_ && worker_thread_->joinable()) {
            worker_thread_->join();
        }
        
        worker_thread_.reset();
        spdlog::info("任务执行器 {} - 已停止", executor_id_);
    }

    // 获取当前执行器ID
    ExecutorId get_executor_id() const {
        return executor_id_;
    }

private:
    // 工作线程主循环（轮询KV获取任务）
    void worker_loop() {
        spdlog::info("任务执行器 {} - 工作线程启动，开始轮询任务", executor_id_);
        while (!stop_flag_) {
            try {
                // 轮询获取分配给自己的任务
                poll_assigned_tasks();
                std::this_thread::sleep_for(std::chrono::milliseconds(50)); // 每50ms轮询一次
            } catch (const std::exception& e) {
                spdlog::error("任务执行器 {} - 工作循环异常: {}", executor_id_, e.what());
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
        spdlog::info("任务执行器 {} - 工作线程停止", executor_id_);
    }

    // 轮询获取分配给自己的任务
    void poll_assigned_tasks() {
        try {
            // 1. 获取所有任务列表
            std::string tasks_list_str = kv_service_->Get("scheduler:tasks_list");
            if (tasks_list_str.empty()) {
                return;
            }
            
            json tasks_list = json::parse(tasks_list_str);
            if (!tasks_list.is_array()) {
                return;
            }
            
            // 2. 遍历任务，检查是否分配给自己
            for (const auto& task_id : tasks_list) {
                TaskId tid = task_id.get<std::string>();
                
                // 跳过已处理的任务
                {
                    std::lock_guard<std::mutex> lock(processed_tasks_mutex_);
                    if (processed_tasks_.count(tid)) {
                        continue;
                    }
                }
                
                // 3. 获取任务的执行器分配信息
                std::string assigned_executor = kv_service_->Get("task/" + tid + "/executorid");
                if (assigned_executor.empty()) {
                    continue;
                }
                
                // 4. 如果分配给自己，执行任务
                if (assigned_executor == executor_id_) {
                    execute_assigned_task(tid);
                    
                    // 标记为已处理
                    std::lock_guard<std::mutex> lock(processed_tasks_mutex_);
                    processed_tasks_.insert(tid);
                }
            }
        } catch (const std::exception& e) {
            spdlog::error("任务执行器 {} - 轮询任务异常: {}", executor_id_, e.what());
        }
    }

    // 执行分配给自己的任务
    void execute_assigned_task(const TaskId& task_id) {
        try {
            spdlog::info("任务执行器 {} - 开始执行任务 {}", executor_id_, task_id);
            
            // 1. 获取任务数据
            std::string task_str = kv_service_->Get("task:" + task_id);
            if (task_str.empty()) {
                spdlog::error("任务执行器 {} - 任务 {} 不存在", executor_id_, task_id);
                update_task_status(task_id, TaskStatus::Failed, "任务数据不存在");
                return;
            }
            
            ScheduledTask task = json::parse(task_str).get<ScheduledTask>();
            
            // 2. 执行任务逻辑
            bool success = execute_task_payload(task);
            
            // 3. 更新任务状态
            if (success) {
                update_task_status(task_id, TaskStatus::Completed);
                spdlog::info("任务执行器 {} - 任务 {} 执行成功", executor_id_, task_id);
            } else {
                update_task_status(task_id, TaskStatus::Failed, "任务执行失败");
                spdlog::error("任务执行器 {} - 任务 {} 执行失败", executor_id_, task_id);
            }
            
            // 4. 删除执行器分配信息
            kv_service_->Del("task/" + task_id + "/executorid");
        } catch (const std::exception& e) {
            spdlog::error("任务执行器 {} - 执行任务 {} 异常: {}", executor_id_, task_id, e.what());
            update_task_status(task_id, TaskStatus::Failed, e.what());
        }
    }

    // 执行任务负载
    bool execute_task_payload(const ScheduledTask& task) {
        std::lock_guard<std::mutex> lock(handlers_mutex_);
        
        try {
            json payload_json = json::parse(task.payload);
            std::string task_type = payload_json.value("type", "default");
            
            auto it = task_handlers_.find(task_type);
            if (it != task_handlers_.end()) {
                // 调用注册的处理器
                return it->second(task.payload);
            } else {
                spdlog::warn("任务执行器 {} - 未找到任务类型 {} 的处理器", executor_id_, task_type);
                return false;
            }
        } catch (...) {
            spdlog::error("任务执行器 {} - 任务 {} payload 格式错误", executor_id_, task.id);
            return false;
        }
    }

    // 更新任务状态到KV
    void update_task_status(const TaskId& task_id, TaskStatus new_status, const std::string& error_msg = "") {
        try {
            // 获取当前状态
            std::string status_str = kv_service_->Get("task_status:" + task_id);
            TaskStatusInfo status_info;
            
            if (!status_str.empty()) {
                status_info = json::parse(status_str).get<TaskStatusInfo>();
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
            
            // 保存到KV
            json status_json = status_info;
            kv_service_->Put("task_status:" + task_id, status_json.dump());
        } catch (const std::exception& e) {
            spdlog::error("任务执行器 {} - 更新任务 {} 状态失败: {}", executor_id_, task_id, e.what());
        }
    }

    // 成员变量
    std::shared_ptr<KvService> kv_service_;
    ExecutorId executor_id_;          // 执行器唯一ID
    std::atomic<bool> stop_flag_;     // 停止标志
    std::unique_ptr<std::thread> worker_thread_; // 工作线程
    
    // 任务处理器映射
    std::map<std::string, std::function<bool(const TaskPayload&)>> task_handlers_;
    mutable std::mutex handlers_mutex_;
    
    // 已处理的任务ID（避免重复执行）
    std::unordered_set<TaskId> processed_tasks_;
    mutable std::mutex processed_tasks_mutex_;
};