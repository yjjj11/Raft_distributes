#pragma once
#include "../include/raftnode.hpp"
#include <iostream>
#include <random>

RaftNode::RaftNode(int node_id, const std::string& ip, int port, 
                   const std::vector<std::pair<std::string, int>>& peers,
                   int election_elapsed_time)
    : node_id_(node_id), ip_(ip), port_(port), peers_(peers),
      server_(mrpc::server::get()), client_(mrpc::client::get()) {

    // 初始化next_index和match_index数组
    next_index_.resize(peers_.size());
    match_index_.resize(peers_.size());
    total_nodes_count_ = 1;
    spdlog::warn("--------------------------------total_nodes_count_: {}", total_nodes_count_);
    election_elapsed_time_ = election_elapsed_time;
    // 加载持久化状态s
    load_state();
    
    // 设置RPC处理器
    setup_rpc_handlers();
    

    // 连接到其他节点
    start_server();

    // 启动日志应用线程
    apply_thread_ = std::thread(&RaftNode::run_apply_loop, this);

    // spdlog::info("Connected to peers---------------------------------:");
}

RaftNode::~RaftNode() {
    stop();
}

void RaftNode::stop() {
    if (!running_) return;
    
    spdlog::debug("Stopping Raft node {}...", node_id_);
    
    running_ = false;
    
    if (election_thread_.joinable()) {
        election_thread_.join();
    }
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }
    apply_cv_.notify_all();
    if (apply_thread_.joinable()) {
        apply_thread_.join();
    }
    client_.shutdown();
    // 关闭服务器
    server_.shutdown();
    
    // 等待线程结束
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
    spdlog::debug("Raft node {} stopped.", node_id_);
}

void RaftNode::become_candidate() {
    //    每次发起选举，Candidate 都会开启一个新的任期
    current_term_++;
    state_ = CANDIDATE;
    voted_for_ = node_id_;
        // 5. 记录日志
    spdlog::error("Node {} became CANDIDATE at term {}", node_id_, current_term_);
    last_election_time_ = std::chrono::steady_clock::now();
    save_state();
}

void RaftNode::become_follower_withlock(int32_t new_term) {
    std::lock_guard<std::mutex> lock(mutex_); // 保护共享状态的修改
    become_follower(new_term);
}

void RaftNode::become_follower(int32_t new_term) {
    last_heartbeat_time_ = std::chrono::steady_clock::now();//假装立马收到了leader心跳

    // 更新任期为发现的更高任期
    if (new_term >= current_term_) {
        current_term_.store(new_term);
    }

    // 转换为跟随者状态
    state_.store(FOLLOWER);

    // 重置投票信息
    voted_for_.store(-1);
    // 记录日志
    spdlog::error("Node {} became FOLLOWER at term {}", node_id_, current_term_.load());

    // 持久化状态 (任期和投票信息)
    save_state();
}

void RaftNode::become_leader() {
    {
        std::lock_guard<std::mutex> lock(mutex_); 
        if (state_.load() == LEADER) {
            spdlog::warn("Node {} attempted to become leader but was already leader.", node_id_);
            return;
        }

        spdlog::error("Node {} became LEADER at term {}.", node_id_, current_term_.load());

        // 1. 转换状态
        state_.store(LEADER);

        // 2. 初始化 Leader 特有状态
        int32_t last_log_idx = get_last_log_index();
        for (size_t i = 0; i < next_index_.size(); ++i) {
            // 对于集群中的每个节点，初始化其 next_index 为新领导者的最后日志条目索引 + 1
            next_index_[i] = last_log_idx + 1;
        }
        for (size_t i = 0; i < match_index_.size(); ++i) {
            // 对于集群中的每个节点，初始化其 match_index 为 -1 (假设索引从 -1 开始表示空日志)
            match_index_[i] = -1;
        }

        // 3. 持久化状态 (虽然成为 Leader 本身不改变 current_term 或 voted_for，但状态改变)
        save_state();
    } 

    if (heartbeat_thread_.joinable()) {
        // 如果心跳线程意外还在运行，先等待它结束
        spdlog::warn("Heartbeat thread was still running when becoming leader. Joining...");
        heartbeat_thread_.join();
    }
    heartbeat_thread_ = std::thread(&RaftNode::send_heartbeats, this);
}

void RaftNode::setup_rpc_handlers() {
    // 注册RPC处理函数
    server_.reg_func("vote_request", 
                     [this](const VoteRequest& req) -> VoteReply { 
                         return this->handle_vote_request(req); 
                     });
    server_.reg_func("append_request", 
                     [this](const AppendRequest& req) -> AppendReply { 
                         return this->handle_append_request(req); 
                     });
                    
    server_.reg_func("is_leader", 
                     [this](int node_id) -> bool { 
                         return this->is_leader(node_id); 
                     });
    server_.reg_func("submit", 
                     [this](const LogEntry& entry) -> int64_t { 
                         return this->submit(entry); 
                     });
}

bool RaftNode::is_leader(int node_id) {
    spdlog::debug("Node {}: Checking if node {} is leader.", node_id_, node_id);
    return (state_.load() == LEADER);
}
void RaftNode::start_server() {
    if (running_) return;
    
    running_ = true;
    
    // 启动服务器
    server_.set_ip_port(ip_, port_);
    server_.set_server_name("raft_node_" + std::to_string(node_id_));
    server_.run();
    
    // 启动各个线程
    server_thread_ = std::thread([this]() {
        server_.accept();
        // spdlog::info("server accept---------------------------------:");
    });
    
    spdlog::info("------------------------------------Raft node {} started on {}:{}------------------------------------", node_id_, ip_, port_);
}

void RaftNode::start_client() {
    client_.run();
    
    std::unique_lock<std::mutex> lock(conns_mutex_);
    for (const auto& peer : peers_) {
        auto conn = client_.connect(peer.first, peer.second,1);
        if (conn) {
            peer_connections_.push_back(conn);
            total_nodes_count_++;
            spdlog::debug("Connected to peer: {}:{}", peer.first, peer.second);
        } else {
            spdlog::error("Failed to connect to peer: {}:{}", peer.first, peer.second);
            peer_connections_.push_back(nullptr);  // 保持索引对应
        }
    }
    for (int i = 0; i < peer_connections_.size(); ++i) {
        if (!peer_connections_[i]) {
            auto conn = client_.connect(peers_[i].first, peers_[i].second,5);
            if (conn) {
                peer_connections_[i] = conn;
                total_nodes_count_++;
                spdlog::debug("Connected to peer: {}:{}", peers_[i].first, peers_[i].second);
            } else {
                spdlog::error("Failed to connect to peer: {}:{}", peers_[i].first, peers_[i].second);
                peer_connections_[i] = nullptr;  // 保持索引对应
            }
        }
    }

    // 启动选举超时监控线程
    election_thread_ = std::thread(&RaftNode::run_election_timeout, this);
}

bool RaftNode::send_vote_request(const VoteRequest& request, size_t peer_idx, VoteReply& reply) {
    std::lock_guard<std::mutex> lock(conns_mutex_);
    if (peer_idx >= peer_connections_.size() || !peer_connections_[peer_idx]) {
        return false;
    }
    auto result = peer_connections_[peer_idx]->call<VoteReply>("vote_request", request);
    if (result.error_code() == mrpc::ok) {
        reply = result.value();
        return true;
    }else {
        spdlog::error("Failed: {} \nWhen vote_request on node{} : {}:{}", result.error_msg(), node_id_, peers_[peer_idx].first, peers_[peer_idx].second);
        peer_connections_[peer_idx] = nullptr;
        total_nodes_count_--;//如果连接超时了就默认为该节点下线了，置为空指针并减少集群数量
    }
    return false;
}

bool RaftNode::send_append_entries(const AppendRequest& request, size_t peer_idx, AppendReply& reply) {
    std::lock_guard<std::mutex> lock(conns_mutex_);
    if (peer_idx >= peer_connections_.size() || !peer_connections_[peer_idx]) {
        return false;
    }
    
    auto result = peer_connections_[peer_idx]->call<AppendReply>("append_request", request);
    if (result.error_code() == mrpc::ok) {
        reply = result.value();
        return true;
    }else{
        spdlog::error("Failed: {} \nWhen append_entries on node{} : {}:{}", result.error_msg(), node_id_, peers_[peer_idx].first, peers_[peer_idx].second);
        if(running_) {
            peer_connections_[peer_idx] = nullptr;
            total_nodes_count_--;//如果连接超时了就默认为该节点下线了，置为空指针并减少集群数量

        }
    }
    return false;
}

void RaftNode::save_state() {
    // 模拟持久化存储
    // 在实际实现中，这里会写入磁盘
    // 例如，可以将 current_term_ 和 voted_for_ 写入一个状态文件
}

void RaftNode::load_state() {
    // 模拟从持久化存储加载
    // 在实际实现中，这里会从磁盘读取
    // 例如，从状态文件中恢复 current_term_ 和 voted_for_
    // 如果没有状态文件，则初始化为默认值
}

int32_t RaftNode::get_last_log_index() const {
    // 日志索引通常从 0 开始
    // 如果日志为空，则最后索引为 -1 (或根据规范定义的初始值)
    if (log_.empty()) {
        return -1; 
    }
    return log_.size() - 1;
}

int32_t RaftNode::get_last_log_term() const {
    // 如果日志为空，则最后任期为 0
    if (log_.empty()) {
        return -0; 
    }
    // 返回最后一个日志条目的任期
    return log_.back().term;
}


VoteReply RaftNode::handle_vote_request(const VoteRequest& request) {
    spdlog::debug("Node {} received vote request: term={}, candidateId={}, lastLogIndex={}, lastLogTerm={}",
                  node_id_, request.term, request.candidateId, request.lastLogIndex, request.lastLogTerm);

    VoteReply reply{.term = current_term_.load(), .voteGranted = false}; // 默认拒绝投票

    // Rule 1: 如果请求的任期小于当前节点的任期，拒绝投票
    if (request.term < current_term_) {
        spdlog::debug("Node {} rejecting vote request from node {}: request term {} < current term {}",
                      node_id_, request.candidateId, request.term, current_term_.load());
        return reply; // reply.term 已设为 current_term，voteGranted 为 false
    }

    // Rule 2: 如果请求的任期大于当前节点的任期，转换为 Follower
    if (request.term > current_term_) {
        spdlog::debug("Node {} discovered higher term {} in vote request, becoming follower.", 
                     node_id_, request.term);
        become_follower_withlock(request.term); // 这会更新 current_term_, state_, voted_for_, 并重置 last_heartbeat_time_ 和持久化
        // 此时 current_term_ 已更新，reply.term 会在后面被更新
        reply.term = current_term_.load(); // 确保回复携带最新的任期
    }

    // Rule 3: 检查是否已经投票给了其他候选人 (在同一任期内)
    // 注意：只有在 request.term == current_term_ 时，voted_for_ 才有意义
    if (request.term == current_term_ && voted_for_ != -1 && voted_for_ != request.candidateId) {
        spdlog::debug("Node {} rejecting vote request from node {}: already voted for node {} in term {}.",
                      node_id_, request.candidateId, voted_for_.load(), current_term_.load());
        return reply; // 已投票给其他人，拒绝
    }

    // Rule 4: 检查候选人的日志是否至少和自己一样新 (Log Up-to-Date Test)
    if (!is_log_up_to_date(request.lastLogTerm, request.lastLogIndex)) {
        spdlog::debug("Node {} rejecting vote request from node {}: its log is not up-to-date.",
                      node_id_, request.candidateId);
        return reply; // 日志不够新，拒绝
    }

    // 如果所有条件都满足，则授予投票
    std::lock_guard<std::mutex> lock(mutex_);
    voted_for_ = request.candidateId;
    reply.voteGranted = true;

    // 持久化状态 (任期和投票信息)
    save_state();

    spdlog::debug("Node {} granted vote to node {} for term {}.", node_id_, request.candidateId, request.term);
    return reply;
}

bool RaftNode::is_log_up_to_date(int32_t last_log_term, int32_t last_log_index) const {
    int32_t my_last_log_term = get_last_log_term();
    int32_t my_last_log_index = get_last_log_index();

    if (last_log_term != my_last_log_term) {
        return last_log_term > my_last_log_term;
    }
    return last_log_index >= my_last_log_index;
}

AppendReply RaftNode::handle_append_request(const AppendRequest& request) {
    AppendReply reply;
    reply.term = current_term_.load(); // 默认回复当前任期
    reply.success = false; // 默认失败

    //检查消息任期部分----------------------------------------
    // 1. 检查任期
    if (request.term < reply.term) {
        // 请求任期过旧，拒绝
        spdlog::debug("Node {} rejecting heartbeat from node {}: request term {} < current term {}.",
                      node_id_, request.leaderId, request.term, current_term_.load());
        return reply;
    }

    // 2. 如果任期更高，更新本地任期并成为 Follower
    if (request.term > reply.term) {
        spdlog::debug("Node {} discovered higher term {} in heartbeat from node {}, becoming follower.", node_id_, request.term, request.leaderId);
        become_follower_withlock(request.term);
        reply.term = current_term_.load();
    }

    // 3. 重置选举超时计时器（因为收到了 Leader 的有效消息）
    std::lock_guard<std::mutex> lock(mutex_); // 获取锁保护所有状态
    last_heartbeat_time_ = std::chrono::steady_clock::now();

    //检查消息一致性部分----------------------------------------
    reply.success = check_if_log_is_ok(request);
    if(!reply.success) return reply; // 日志不一致，拒绝



    // handle_heartbeat_request(request,reply);
    // 5. 如果日志一致，开始追加日志条目，能走到这里说明previndex  && prevterm  已经匹配了
    // 5a. 如果本地日志在 prevLogIndex+1 位置存在日志，且与传入的日志冲突，则删除冲突的日志及后续所有日志
    // 例如：本地 [1,2,3,4], Leader [1,2,5,6] (prevLogIndex=1, prevLogTerm=2)
    // 当前在 index=2 的本地日志是 3 (term=?)，Leader 传来的 entries[0] 是 5 (term=?)    ，index和term唯一确定一条日志，我们比较term时index是确定的，因此比较term即可
    // 如果 3.term != 5.term，则删除 3 和 4。
    int32_t local_next_index = request.prevLogIndex + 1;
    size_t added_count = 0;
    for (const auto& entry : request.entries) {
        if (local_next_index < static_cast<int32_t>(log_.size())) {
            // 检查本地日志是否与 Leader 的日志冲突
            if (log_[local_next_index].term != entry.term) {
                // 冲突，删除本地从 local_next_index 开始的所有日志，然后
                log_.erase(log_.begin() + local_next_index, log_.end());
                log_.push_back(entry); // 追加 Leader 的日志
                added_count++;
            }
            local_next_index++;
        } else {
            // 本地日志较短，追加 Leader 的日志
            log_.push_back(entry);
            local_next_index++;
            added_count++;
        }
    }
    
    if (request.leaderCommit > commit_index_) {
        int32_t last_new_log_index = request.prevLogIndex + request.entries.size();
        commit_index_.store(std::min(request.leaderCommit, last_new_log_index));
        // 7. 唤醒日志应用线程
        apply_cv_.notify_one();
    }
    
    reply.success = true;
    reply.term = current_term_.load();
    if(added_count > 0)spdlog::debug("Node {}: AppendEntries successful, added {} entries.  from node {}", node_id_, added_count, request.leaderId);
    last_heartbeat_time_ = std::chrono::steady_clock::now();
    return reply;
}

bool RaftNode::check_if_log_is_ok(const AppendRequest& request){
    bool log_ok = true;
    if (request.prevLogIndex >= 0) { // 如果前置日志索引有效
        if (request.prevLogIndex >= static_cast<int32_t>(log_.size())) {
            // 前置日志索引超出本地日志范围,本地没有leader的这个位置的日志
             spdlog::debug("Node {}: AppendEntries failed, prevLogIndex {} out of range.", node_id_, request.prevLogIndex);
            log_ok = false;
        } else {
            // 检查任期是否匹配
            if (log_[request.prevLogIndex].term != request.prevLogTerm) {
                spdlog::debug("Node {}: AppendEntries failed, prevLogTerm mismatch at index {}.", node_id_, request.prevLogIndex);
                log_ok = false;
            }
        }
    }
    if (!log_ok) spdlog::debug("Node {}: Heartbeat consistency check failed.", node_id_);
    return log_ok;
}

void RaftNode::apply_logs_to_state_machine(int32_t from_index, int32_t to_index) {
    spdlog::debug("Node {}: Applying logs from index {} to {} to state machine.", node_id_, from_index, to_index);

    // 1. 批量获取日志条目，减少锁的持有时间
    std::vector<LogEntry> entries_to_apply;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (int32_t i = from_index; i <= to_index; ++i) {
            if (i < 0 || i >= static_cast<int32_t>(log_.size())) {
                spdlog::warn("Node {}: Invalid log index {} when applying to state machine.", node_id_, i);
                continue;
            }
            entries_to_apply.push_back(log_[i]); // 拷贝日志条目
        }
    } // lock 作用域结束

    // 2. 在无锁状态下应用日志
    for (size_t idx = 0; idx < entries_to_apply.size(); ++idx) {
        const auto& entry = entries_to_apply[idx];
        int32_t log_index = from_index + idx; // 计算出原始日志索引

        spdlog::debug("Node {}: 正在准备将index={}的日志条目 {} 应用到状态机。", node_id_, log_index, entry.command_type);
        bool success = false;
        try {
            success = callback_reg.trigger_by_logentry(entry);
            if(!success){
                spdlog::error("Node {}: Callback failed for log index {}: {}", node_id_, log_index, entry.command_type);
                continue;
            }
        } catch (const std::exception& e) {
            spdlog::error("Node {}: Callback failed for log index {}: {}", node_id_, log_index, e.what());
            continue;
        }

        // 3. 通知等待者 (同样在无锁状态下)
        auto it = lock_store_.find(entry.req_id);
        if (it != lock_store_.end()) {
            it->second.set_value(success);
        }
    }

    // 4. 最后，一次性更新 last_applied_，再次加锁
    {
        std::lock_guard<std::mutex> lock(apply_mutex_); // 使用专门的 apply_mutex_
        last_applied_.store(to_index);
    }
}

void RaftNode::run_apply_loop() {
    spdlog::info("Node {} starting apply loop thread.", node_id_);
    while (running_) {
        std::unique_lock<std::mutex> lock(apply_mutex_);
        apply_cv_.wait(lock, [this] {
            return !running_ || commit_index_.load() > last_applied_.load();
        });

        if (!running_) break;

        int32_t from = last_applied_.load() + 1;
        int32_t to = commit_index_.load();
        
        lock.unlock(); // 允许在应用日志时有新的提交触发 notify
        if (from <= to) {
            apply_logs_to_state_machine(from, to);
        }
    }
    spdlog::info("Node {} apply loop thread stopping.", node_id_);
}


void RaftNode::send_heartbeats() {
    spdlog::info("Node {} starting heartbeat thread for term {}.", node_id_, current_term_.load());

    // 计算心跳间隔，例如选举超时时间的 1/5
    auto heartbeat_interval = std::chrono::milliseconds( election_elapsed_time_ / 5);
    bool is_follower = false;
    while (running_ && state_.load() == LEADER) {
        // 构建心跳请求 (空的日志条目 AppendEntries RPC)
        AppendRequest heartbeat_req;
        {
            std::lock_guard<std::mutex> lock(mutex_); // 保护共享状态的读取
            heartbeat_req.term = current_term_;
            heartbeat_req.leaderId = node_id_;
            heartbeat_req.leaderCommit = commit_index_.load(); // 发送 Leader 的提交索引
        }


        // spdlog::debug("Node {} sending heartbeat to all followers.", node_id_);
        for (size_t i = 0; i < peer_connections_.size(); ++i) {
            if (!running_ || state_.load() != LEADER) break; // 检查状态

            if(!peer_connections_[i]){
                std::unique_lock<std::mutex> lock(conns_mutex_);
                peer_connections_[i] = client_.connect(peers_[i].first, peers_[i].second,1);//尝试重新连接
                next_index_[i] = 0;
                match_index_[i] = -1;
                if(peer_connections_[i]) total_nodes_count_++;
                else continue;
            }

            {
                std::unique_lock<std::mutex> lock(mutex_);
                heartbeat_req.prevLogIndex = next_index_[i] - 1;
                heartbeat_req.prevLogTerm = 0; // 默认值
                if (heartbeat_req.prevLogIndex >= 0 && heartbeat_req.prevLogIndex < static_cast<int32_t>(log_.size())) {
                    heartbeat_req.prevLogTerm = log_[heartbeat_req.prevLogIndex].term;
                }

                heartbeat_req.entries.clear();
                if (next_index_[i] < static_cast<int32_t>(log_.size())) {
                    for (size_t j = next_index_[i]; j < log_.size(); ++j) {
                        heartbeat_req.entries.push_back(log_[j]);
                    }
                } else {
                    // 如果 next_index_[i] 超过了日志长度，说明 Follower 已经同步了所有日志，发送心跳
                    heartbeat_req.entries = {}; // 空 entries
                }
            }
            if(!heartbeat_req.entries.empty()){
                spdlog::debug("将发送新日志消息到follwer set {} = {} term {}", heartbeat_req.entries[0].command_type, heartbeat_req.entries[0].term);
            }
            // 发送心跳请求到每个 Follower
            AppendReply reply;
            if(send_append_entries(heartbeat_req, i, reply)){
                // spdlog::debug("Heartbeat to node {} Reply term: {} Success: {}", i+1, reply.term, reply.success);
                if(reply.term > current_term_.load()){
                    become_follower_withlock(reply.term);
                    is_follower = true;
                    break;
                }
                std::unique_lock<std::mutex> lock(mutex_);
                // spdlog::debug("开始根据心跳回复结果更新match_index_和next_index_");
                if (reply.success) {
                    int32_t num_entries_sent = static_cast<int32_t>(heartbeat_req.entries.size());
                    // Follower 匹配的日志索引是其 prevLogIndex + 成功发送的数量
                    int32_t new_match_index = heartbeat_req.prevLogIndex + num_entries_sent;
                    // spdlog::debug("根据心跳回复结果更新match_index_和next_index_为{}", new_match_index);
                    match_index_[i] = std::max(match_index_[i], new_match_index);
                    next_index_[i] = std::max(next_index_[i], new_match_index + 1);
                    // spdlog::info("Node {}: Updated match_index[{}] to {}, next_index[{}] to {}.", node_id_, i, match_index_[i], i, next_index_[i]);
                } else {
                    // 4. 失败处理：递减 next_index，尝试重发s
                    if (next_index_[i] > 0) next_index_[i]--;

                    spdlog::warn("Node {}: AppendEntries to {} failed, decrementing next_index to {}.",node_id_, i, next_index_[i]);
                }
            } 
            
        }
        // spdlog::debug("Heartbeats send Over!");
        if(is_follower){
            spdlog::warn("----------------------心跳中途发现自己变成了follwer-----------------------");
            break; //如果从心跳中发现自己已经不是领导者了，就跳出循环
        }
        if (running_ && state_.load() == LEADER) {
            {
                std::unique_lock<std::mutex> lock(mutex_);
                // spdlog::debug("开始根据match_index_更新commit_index_");
                int32_t new_commit_index = commit_index_.load();
                // 遍历所有 match_index，寻找一个 N，使得 N > commit_index，并且 match_index[i] >= N 的节点数量超过半数
                for (int32_t n = log_.size() - 1; n > commit_index_.load(); --n) {
                    if (n < 0) continue; // 安全检查
                    // 检查当前日志索引 n 的任期是否与当前任期相同
                    // spdlog::info("Node {}: Checking log index {} with term {}.", node_id_, n, log_[n].term);
                    if (log_[n].term == current_term_) {
                        int count = 1; // Leader 自己算一个
                        // spdlog::info("Node {}: Counting votes for log index {}.", node_id_, n);
                        for (size_t i = 0; i < match_index_.size(); ++i) {
                            if (match_index_[i] >= n) {
                                count++;
                                // spdlog::info("Node {}: Vote granted for log index {} by node {}.", node_id_, n, i);
                            }
                        }
                        // spdlog::info("Node {}: Total votes for log index {} is {}.", node_id_, n, count);
                        if (count > total_nodes_count_ / 2) {
                            // spdlog::info("Node {}: Log index {} is committed with {} votes.", node_id_, n, count);
                            new_commit_index = n;
                            break; // 找到最大的 N
                        }
                    }
                }
                // spdlog::info("Node {}: New commit_index candidate is {}.", node_id_, new_commit_index);
                if (new_commit_index != commit_index_.load()) {
                    // spdlog::debug("根据match_index_更新commit_index_为{}", new_commit_index);
                    commit_index_.store(new_commit_index);
                    // spdlog::debug("Node {}: Updated commit_index to {}.", node_id_, commit_index_.load());
                    // 唤醒日志应用线程
                    apply_cv_.notify_one();
                }
            }
        }
        std::this_thread::sleep_for(heartbeat_interval);

    } // while loop end

    spdlog::info("Node {} heartbeat thread stopping.", node_id_);
}

//具体选举逻辑
void RaftNode::run_election_timeout() {
    last_heartbeat_time_ = std::chrono::steady_clock::now();
    spdlog::info("------------------------------------Into run_election_timeout------------------------------------");
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(3000, 4000); // 1000-3000ms 随机选举超时

    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        std::unique_lock<std::mutex> lock(mutex_);

        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - last_heartbeat_time_).count();

        State current_state = state_.load();
        
        // 检查是否超时
        if (elapsed > election_elapsed_time_) { //已经election_elapsed_time_毫秒没有收到leader的心跳了，本节点开始尝试选举
            if (current_state == FOLLOWER || current_state == CANDIDATE) {//进入选举阶段
                election_elapsed_time_ = dis(gen);//重新设置选举超时时间
                // 转换为候选人状态，开始新的选举
                become_candidate();
                
                lock.unlock();

                VoteRequest vote_req;
                {
                     std::lock_guard<std::mutex> lock(mutex_); // 获取锁以安全读取日志信息
                     vote_req.term = current_term_;
                     vote_req.candidateId = node_id_;
                     vote_req.lastLogIndex = get_last_log_index();
                     vote_req.lastLogTerm = get_last_log_term();
                }
                spdlog::info("------------------------------------Vote begain--------------------------------------");
                size_t vote_count = 1;
                // 发送投票请求
                for (size_t i=0;i<peer_connections_.size();i++) {
                    if(state_.load() != CANDIDATE){//预防在选举的过程中诞生了新的leader,处理心跳后在主线程已经降级为了follower,此时应该停止选举
                        spdlog::info("-----------------------------------Leader 诞生，停止选举------------------------------------");
                        break;
                    }
                    if (!peer_connections_[i]) {
                        std::unique_lock<std::mutex> lock(conns_mutex_);
                        next_index_[peer_connections_.size()] = 0;
                        match_index_[peer_connections_.size()] = -1;
                        peer_connections_[i] = client_.connect(peers_[i].first, peers_[i].second,1);//尝试重新连接
                        if(peer_connections_[i]) total_nodes_count_++;
                        else continue;   //还是连接不上就跳过
                    }
                    VoteReply reply;
                        if (send_vote_request(vote_req, i, reply)) {
                            spdlog::debug("Success! Result: reply.term={}, voteGranted={}", reply.term, reply.voteGranted);
                            if(reply.term > current_term_){//如果收到了更高的任期，应该转换为follower
                                become_follower_withlock(reply.term);
                                break;
                            }else if (reply.voteGranted) {
                                vote_count++;
                            }
                        } 
                }

                if(state_.load() == FOLLOWER )continue; //如果在投票过程中，本节点已经成为了follower,则应该停止投票

                //计算选举总时长，如果超时了就重新选举
                auto now = std::chrono::steady_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - last_election_time_).count();
                if(elapsed > election_elapsed_time_){
                    become_candidate();
                    //本轮选举超时，应该重新发起选举，这里的逻辑如何实现还有待思考
                    spdlog::debug("------------------------------------Election_begin_end_timeout: {}ms------------------------------------", elapsed);
                    continue;
                }
                
                spdlog::info("----------------------total_nodes_count_: {}, vote_count: {}-------------------", total_nodes_count_, vote_count);
                // 如果没有选举时长没有超时并且检查获得多数投票
                if (vote_count > (total_nodes_count_ / 2)) become_leader();
                else become_follower_withlock(current_term_);
            }
        }
        // std::this_thread::sleep_for(50ms);
    }
}
// 在 raftnode.cpp 中实现 handle_append_request (仅心跳版本)

int RaftNode::find_leader(){
    std::unique_lock<std::mutex> lock(conns_mutex_);
    for(int i=0;i<peer_connections_.size();i++){
        auto conn=peer_connections_[i];
        if(!conn) {
            std::unique_lock<std::mutex> lock(conns_mutex_);
            peer_connections_[i] = client_.connect(peers_[i].first, peers_[i].second,1);//尝试重新连接
            if(peer_connections_[i]) total_nodes_count_++;
            else continue;   //还是连接不上就跳过
        }
        // spdlog::debug("Node {}: Sending is_leader request to node {}.", node_id_, i);
        auto reply = conn->call<bool>("is_leader", i);
        // spdlog::debug("Node {}: Received is_leader reply from node {}: {}", node_id_, i, reply.value());
        if(reply.error_code() == mrpc::ok && reply.value()){
            return i;
        }
    }
    return -1;
}

int64_t RaftNode::submit(const LogEntry& entry) {
    // 1. 检查当前节点是否为 Leader
    int64_t request_id = -1;
    if (state_.load() != LEADER) {
        spdlog::warn("Node {}: Not leader, cannot submit log entry.", node_id_);
        int leader_id = find_leader();
        if(leader_id == -1){
            spdlog::warn("Node {}: Failed to find leader.", node_id_);
            request_id = -1;
        }
        auto conn=peer_connections_[leader_id];
        if(!conn) {
            std::unique_lock<std::mutex> lock(conns_mutex_);
            spdlog::warn("Node {}: trying to connect to leader {}.", node_id_, leader_id);
            peer_connections_[leader_id] = client_.connect(peers_[leader_id].first, peers_[leader_id].second,1);//尝试重新连接
            if(peer_connections_[leader_id]) total_nodes_count_++;
            else request_id = -1;   //还是连接不上就返回失败
        }

        spdlog::debug("Node {}: Sending log entry {} to leader {}", node_id_, entry.command_type, leader_id);
        auto reply = conn->call<int64_t>("submit", entry);
        if (reply.error_code() == mrpc::ok) {
            spdlog::info("成功向leader {}提交日志条目 {}", leader_id, entry.command_type);
            request_id = reply.value();
        } else {
            spdlog::error("Node {}: Failed to submit log entry to leader {}: {}", node_id_, leader_id, reply.error_msg());
            request_id = -1;
        }

    }
    else{
        spdlog::debug("进入正式push阶段了");
        // 2. 加锁写入日志（保证日志的原子性）
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto new_entry = std::move(entry);
            new_entry.term = current_term_.load();
            request_id_++;
            new_entry.req_id = request_id_;
            request_id = new_entry.req_id;
            
            log_.push_back(new_entry);
            spdlog::info("Node {}: Submitted log entry (index {}, term {}) type {}", 
                        node_id_, log_.size()-1, current_term_.load(), new_entry.command_type);
        }
    }
    
    if(request_id == -1){
        return request_id;
    }
    return wait_for(request_id);
    
}

void init_logger(int node_id) {
    std::filesystem::create_directories("logs");
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(
        "logs/raft_node_" + std::to_string(node_id) + ".log", true);

    // 创建 logger
    auto logger = std::make_shared<spdlog::logger>(
        "raft_node_" + std::to_string(node_id),
            spdlog::sinks_init_list{file_sink}
    );
    // 关键：给 logger 单独设置格式（时间高亮 + 去掉 logger 名）
    logger->set_pattern("%^[%Y-%m-%d %H:%M:%S.%e]%$ [%l] %v");
    logger->set_level(spdlog::level::debug);
    logger->flush_on(spdlog::level::debug);

    spdlog::set_default_logger(logger);
}


bool RaftNode::wait_for(int64_t req_id) {
    auto future = lock_store_[req_id].get_future();
    
    // 等待最多 1 秒
    auto status = future.wait_for(std::chrono::seconds(5));

    if (status == std::future_status::ready) {
        // 日志已被应用，Future 已被设置
        return future.get();
        spdlog::debug("Node {}: Log entry with req_id {} has been applied to the state machine.", node_id_, req_id);
    } else {
        // 等待超时
        spdlog::warn("Node {}: Timeout waiting for log entry with req_id {} to be applied. Request may have been lost or cluster is unavailable.", node_id_, req_id);
        // 可选：在这里也可以选择移除超时的 promise，避免内存泄漏（见下方注释）
    }
    
    // 无论成功或超时，都需要清理 map 中的条目
    // 注意：如果 wait_for 超时，此时尚未调用 set_value，erase 会销毁未完成的 promise，
    // 这会导致其 associated future 的行为未定义。但在我们的场景下，由于已经超时，
    // 我们不再关心它的结果，所以这种清理是合理的。
    lock_store_.erase(req_id);
}


std::shared_ptr<RaftNode> initialize_server(int argc, char* argv[]){
    if (argc < 4) {
        std::cout << "Usage: " << argv[0] << " <node_id> <ip> <port> [peer1_ip:port peer2_ip:port ...]" << std::endl;
        std::cout << "Example: " << argv[0] << " 0 127.0.0.1 8000 127.0.0.1:8001 127.0.0.1:8002" << std::endl;
        exit(1);
    }
    
    int node_id = std::stoi(argv[1]);

    init_logger(node_id);
    
    std::string ip = argv[2];
    int port = std::stoi(argv[3]);
    int election_elapsed_time = std::stoi(argv[4])* 1000;
    spdlog::debug("Election timeout: {}ms", election_elapsed_time);
        
    
    // 解析集群节点信息
    std::vector<std::pair<std::string, int>> peers;
    for (int i = 5; i < argc; ++i) {
        std::string peer_addr = argv[i];
        size_t colon_pos = peer_addr.find(':');
        if (colon_pos != std::string::npos) {
            std::string peer_ip = peer_addr.substr(0, colon_pos);
            int peer_port = std::stoi(peer_addr.substr(colon_pos + 1));
            peers.emplace_back(peer_ip, peer_port);
        } else {
            std::cerr << "Invalid peer address format: " << peer_addr << std::endl;
            exit(1);
        }
    }

    // spdlog::info("Initializing Raft node {} on {}:{}", node_id, ip, port);
    spdlog::info("Cluster peers:");
    for (const auto& peer : peers) {
        spdlog::info("{}:{}", peer.first, peer.second);
    }
        // 创建Raft节点实例
    auto node = std::make_shared<RaftNode>(node_id, ip, port, peers,election_elapsed_time);
    spdlog::debug("Raft node {} initialized on {}:{}", node_id, ip, port);
    
    // 启动节点
    node->start_client();   
    
    std::this_thread::sleep_for(std::chrono::seconds(2));
    return node;
}