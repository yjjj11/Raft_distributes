#pragma once
#include <logger.hpp>
#include <mrpc/client.hpp>
#include <mrpc/server.hpp>
#include <vector>
#include <string>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <thread>
#include <chrono>
#include "call_back.hpp"
using namespace std::chrono_literals;
using namespace mrpc;
class RaftNode {
public:
    RaftNode(int node_id, const std::string& ip, int port, 
             const std::vector<std::pair<std::string, int>>& peers,
             int election_elapsed_time);
    ~RaftNode();

    // 启动节点
    void start_client();
    
    // 停止节点
    void stop();

    // RPC处理函数
    VoteReply handle_vote_request(const VoteRequest& request);
    AppendReply handle_append_request(const AppendRequest& request);
    void handle_heartbeat_request(const AppendRequest& request,AppendReply& reply);

    int64_t submit(const LogEntry& entry);
    using StateMachineCallback = std::function<bool(int32_t log_index, const LogEntry& entry)>;

    // 设置回调函数的接口
    void set_apply_callback(StateMachineCallback callback) {
        std::lock_guard<std::mutex> lock(mutex_);
        apply_callback_ = std::move(callback);
    }
    template<typename... Args>
    LogEntry pack_logentry(const std::string& command_type, Args&&... args) {
        LogEntry entry;
        entry.command_type = command_type;
        // 核心：将任意参数打包为 JSON 数组
        nlohmann::json args_json = nlohmann::json::array({std::forward<Args>(args)...});
        entry.buffer = args_json.dump();  // 序列化到 buffer
        return entry;
    }
    

    void wait_for(int64_t req_id);
    int node_id_;
    std::unordered_map<int64_t, std::promise<bool>> lock_store_;
    RegisterCallback callback_reg;
private:
    // 节点基本信息
    std::string ip_;
    int port_;
    StateMachineCallback apply_callback_;
    int64_t request_id_{0};
    // 集群配置
    int election_elapsed_time_;
    std::vector<std::pair<std::string, int>> peers_;  // 存储其他节点的地址信息
    std::vector<std::shared_ptr<mrpc::connection>> peer_connections_;  // 连接到其他节点的连接
    
    // Raft核心状态
    std::atomic<State> state_{FOLLOWER};
    std::atomic<int32_t> current_term_{0};
    std::atomic<int32_t> voted_for_{-1};  // -1表示未投票
     int total_nodes_count_;
    
    // 日志相关
    std::vector<LogEntry> log_;
    std::atomic<int32_t> commit_index_{-1};
    std::atomic<int32_t> last_applied_{-1};
    
    // 领导者特有状态
    std::vector<int32_t> next_index_;   // 发送给每个节点的下一个日志索引
    std::vector<int32_t> match_index_;  // 每个节点已复制的最高日志索引
    
    // 控制变量
    std::atomic<bool> running_{false};
    std::thread server_thread_;
    std::thread election_thread_;
    std::thread heartbeat_thread_;
    std::thread apply_thread_;
    
    // 互斥锁
    mutable std::mutex mutex_;
    mutable std::mutex conns_mutex_;
    mutable std::mutex apply_mutex_;
    std::condition_variable apply_cv_;
    // RPC服务器和客户端
    mrpc::server& server_;
    mrpc::client& client_;

    // 定时器相关
    std::chrono::steady_clock::time_point last_heartbeat_time_;
    std::chrono::steady_clock::time_point last_election_time_;
    // 内部辅助函数
    void run_election_timeout();
    void send_heartbeats();
    void run_apply_loop();
    void setup_rpc_handlers() ;
    void start_server();
    
    // 日志一致性检查
    void apply_logs_to_state_machine(int32_t from_index, int32_t to_index);
    bool check_if_log_is_ok(const AppendRequest& request);
    
    // 状态转换
    void become_follower(int32_t new_term);
    void become_follower_withlock(int32_t new_term);
    void become_candidate();
    void become_leader();
    bool is_leader(int node_id);
        
    // RPC调用辅助函数
    bool send_vote_request(const VoteRequest& request, size_t peer_idx, VoteReply& reply);
    bool send_append_entries(const AppendRequest& request, size_t peer_idx, AppendReply& reply);
    
    // 日志管理
    int32_t get_last_log_index() const;
    int32_t get_last_log_term() const;
    
    // 持久化相关（模拟）
    void save_state();
    void load_state();
    
    // 安全检查
    bool is_log_up_to_date(int32_t last_log_term, int32_t last_log_index) const;

    int find_leader();


};


void init_logger(int node_id);

std::shared_ptr<RaftNode> initialize_server(int argc, char* argv[]);