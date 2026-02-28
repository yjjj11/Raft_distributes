#pragma once
#include "raftnode.hpp"
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
    total_nodes_count_ = peers_.size()+1;
    election_elapsed_time_ = election_elapsed_time;
        
    // 加载持久化状态
    load_state();
    
    // 设置RPC处理器
    setup_rpc_handlers();
    

    // 连接到其他节点
    start_server();

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
    spdlog::error("Node {} became candidate at term {}", node_id_, current_term_);
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
    if (new_term > current_term_) {
        current_term_.store(new_term);
    }
    // 如果传入的 new_term 与 current_term 相等，也可能需要转换（例如，收到了合法的 AppendEntries）
    // 但最常见的情况是 new_term > current_term_
    if (new_term >= current_term_) {
        current_term_.store(new_term);
    }

    // 转换为跟随者状态
    state_.store(FOLLOWER);

    // 重置投票信息
    voted_for_.store(-1);
    // 记录日志
    spdlog::error("Node {} became follower at term {}", node_id_, current_term_.load());

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

        spdlog::error("Node {} became leader at term {}.", node_id_, current_term_.load());

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
    
    for (const auto& peer : peers_) {
        auto conn = client_.connect(peer.first, peer.second,5);
        if (conn) {
            peer_connections_.push_back(conn);
            spdlog::debug("Connected to peer: {}:{}", peer.first, peer.second);
        } else {
            spdlog::error("Failed to connect to peer: {}:{}", peer.first, peer.second);
            peer_connections_.push_back(nullptr);  // 保持索引对应
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
        return -1; // Raft 论文中初始值常为 -1，PrevLogIndex 为 -1 表示没有前置日志
    }
    return log_.back().index;
}

int32_t RaftNode::get_last_log_term() const {
    // 如果日志为空，则最后任期为 0
    if (log_.empty()) {
        return 0; 
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

AppendReply RaftNode::handle_append_request(const AppendRequest& request) {
    AppendReply reply;
    reply.term = current_term_.load(); // 默认回复当前任期
    reply.success = false; // 默认失败

    // 1. 检查任期
    if (request.term < reply.term) {
        // 请求任期过旧，拒绝
        spdlog::debug("Node {} rejecting heartbeat from node {}: request term {} < current term {}.",
                      node_id_, request.leaderId, request.term, current_term_.load());
        return reply;
    }

    // 2. 如果任期更高，更新本地任期并成为 Follower
    if (request.term > reply.term) {
        spdlog::debug("Node {} discovered higher term {} in heartbeat, becoming follower.", node_id_, request.term);
        become_follower_withlock(request.term);
    }

    // 3. 重置选举超时计时器（因为收到了 Leader 的有效消息）
    std::lock_guard<std::mutex> lock(mutex_); // 获取锁保护所有状态
    last_heartbeat_time_ = std::chrono::steady_clock::now();

    handle_heartbeat_request(request,reply);
    
    return reply; // reply.success = true
}

void RaftNode::handle_heartbeat_request(const AppendRequest& request,AppendReply& reply){
    // 4. 检查日志一致性 (PrevLogIndex and PrevLogTerm)
    bool log_ok = true;
    if (request.prevLogIndex >= 0) { // 如果前置日志索引有效
        if (request.prevLogIndex >= static_cast<int32_t>(log_.size())) {
            // 前置日志索引超出本地日志范围,本地没有leader的这个位置的日志
            log_ok = false;
        } else {
            // 检查任期是否匹配
            if (log_[request.prevLogIndex].term != request.prevLogTerm) {
                log_ok = false;
            }
        }
    }
    if (!log_ok) spdlog::debug("Node {}: Heartbeat consistency check failed.", node_id_);

    // 5. 如果是一次有效的心跳（任期合法，日志一致），则成功
    // 因为 entries 为空，所以没有日志需要追加
    reply.success = true;
    reply.term = current_term_.load(); // 确保回复携带最新的任期

    if (request.leaderCommit > commit_index_) {
        int32_t last_local_log_index = get_last_log_index(); // 使用辅助函数更清晰
        commit_index_.store(std::min(request.leaderCommit, last_local_log_index));
        // 可能需要应用已提交的日志到状态机
    }

    spdlog::info("Node {}: Received and accepted heartbeat from leader {}.", node_id_, request.leaderId);
}

bool RaftNode::is_log_up_to_date(int32_t last_log_term, int32_t last_log_index) const {
    int32_t my_last_log_term = get_last_log_term();
    int32_t my_last_log_index = get_last_log_index();

    if (last_log_term != my_last_log_term) {
        return last_log_term > my_last_log_term;
    }
    return last_log_index >= my_last_log_index;
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
            // 1. 设置基本字段
            heartbeat_req.term = current_term_;
            heartbeat_req.leaderId = node_id_;
            heartbeat_req.leaderCommit = commit_index_.load(); // 发送 Leader 的提交索引
        }
        heartbeat_req.prevLogIndex = -1; // 这需要根据 next_index_[i] 动态设置
        heartbeat_req.prevLogTerm = 0; // 这也需要根据 prevLogIndex 动态设置
        // 3. 心跳的关键：entries 为空
        heartbeat_req.entries = {}; // 空列表
        // spdlog::debug("Node {} sending heartbeat to all followers.", node_id_);
        for (size_t i = 0; i < peer_connections_.size(); ++i) {
            if(!peer_connections_[i]){
                std::unique_lock<std::mutex> lock(conns_mutex_);
                peer_connections_[i] = client_.connect(peers_[i].first, peers_[i].second,0.5);//尝试重新连接
                if(peer_connections_[i]) total_nodes_count_++;
                else continue;
            }
            if (state_.load() != LEADER)   break; 
            // 为每个 Follower 动态设置 PrevLogIndex 和 PrevLogTerm
            {
                std::unique_lock<std::mutex> lock(mutex_);
                heartbeat_req.prevLogIndex = next_index_[i] - 1;
                if (heartbeat_req.prevLogIndex >= 0) {
                    heartbeat_req.prevLogTerm = log_[heartbeat_req.prevLogIndex].term;
                }else{
                    heartbeat_req.prevLogTerm = 0;
                }
            }
            // 发送心跳请求到每个 Follower
            AppendReply reply;
            if(send_append_entries(heartbeat_req, i, reply)){
                spdlog::debug("Heartbeat to node {} Reply term: {} Success: {}", i+1, reply.term, reply.success);
                if(reply.term > current_term_.load()){
                    become_follower_withlock(reply.term);
                    is_follower = true;
                    break;
                }
                if(reply.success) spdlog::trace("Heartbeat to peer {} successful.", i);
                else next_index_[i] = std::max(1, next_index_[i] - 1);
            } 
            
        }
        // spdlog::debug("Heartbeats send Over!");
        if(is_follower)break; //如果从心跳中发现自己已经不是领导者了，就跳出循环
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
    std::uniform_int_distribution<> dis(3000, 10000); // 1000-3000ms 随机选举超时

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
                size_t vote_count = 0;
                // 发送投票请求
                for (size_t i=0;i<peer_connections_.size();i++) {
                    if(state_.load() == FOLLOWER){//预防在选举的过程中诞生了新的leader,处理心跳后在主线程已经降级为了follower,此时应该停止选举
                        spdlog::info("-----------------------------------Leader 诞生，停止选举------------------------------------");
                        break;
                    }
                    if (!peer_connections_[i]) {
                        std::unique_lock<std::mutex> lock(conns_mutex_);
                        peer_connections_[i] = client_.connect(peers_[i].first, peers_[i].second,0.5);//尝试重新连接
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
                
                vote_count++;
                spdlog::info("------------------------------------Vote count: {}------------------------------------", vote_count);
                // 如果没有选举时长没有超时并且检查获得多数投票
                if (vote_count > (total_nodes_count_ / 2)) become_leader();
                else become_follower_withlock(current_term_);
            }
        }
        // std::this_thread::sleep_for(50ms);
    }
}
// 在 raftnode.cpp 中实现 handle_append_request (仅心跳版本)
