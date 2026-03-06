#include "task_scheduler.hpp"
#include <atomic>
#include <thread>
#include <iostream>

using namespace mrpc;
std::atomic<bool> g_running{true};
RaftNode* g_node = nullptr;

void signal_handler(int signal) {
    g_running = false;
    if (g_node) {
        g_node->stop();
    }
    exit(0);
}

int main(int argc, char* argv[]) {
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    auto node = initialize_server(argc, argv);
    g_node = node.get();
    auto kv_service = std::make_shared<KvService>(node);
    TaskScheduler scheduler(kv_service, node); 

    system("clear");

    scheduler.register_task_handler("data_process", [](const TaskPayload& payload) {
        auto json=nlohmann::json::parse(payload);
        std::cout << "Processing data task: " << json["data"] << std::endl;
        return true;
    });
    std::cout << "注册任务处理函数完成" << std::endl;

    node->server_.reg_func("make_and_submit", [&](const std::string& type, const std::string& payload) {
        auto task = scheduler.make_task_from_string(type, payload);
        if(scheduler.schedule_task(task)) {
            std::cout << "Task scheduled successfully" << std::endl;
            return true;
        } else {
            std::cout << "Failed to schedule task" << std::endl;
            return false;
        } 
    });

    std::cout << "等待节点状态..." << std::endl;

    std::atomic<bool> scheduler_running{false}; // 记录调度器是否正在运行

    while (g_running) {
        bool is_leader = g_node->is_leader(g_node->node_id_); // 调用 RaftNode 的 is_leader 接口

        if (is_leader && !scheduler_running) {
            // 当前是 Leader，且调度器未启动 → 启动调度器
            scheduler.start();
            scheduler_running = true;
            std::cout << "✅ 当前节点成为 Leader，启动任务调度器" << std::endl;
        } else if (!is_leader && scheduler_running) {
            // 当前不是 Leader，且调度器正在运行 → 停止调度器
            scheduler.stop();
            scheduler_running = false;
            std::cout << "⚠️ 当前节点失去 Leader 身份，停止任务调度器" << std::endl;
        }

        // 每5秒打印一次任务统计（如果调度器在运行）
        static int count = 0;
        if (++count % 5 == 0 && scheduler_running) {
            size_t pending = scheduler.get_pending_task_count();
            // std::cout << "📊 Pending tasks: " << pending 
            //           << ", Total tasks: " << scheduler.get_total_task_count() << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    if (scheduler_running) {
        scheduler.stop();
    }

    return 0;
}