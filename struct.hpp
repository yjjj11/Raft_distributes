#include <vector>
#include <string>
#include <nlohmann/json.hpp>

// 引入nlohmann json宏支持
using json = nlohmann::json;

// 先引入自定义的Raft消息结构体（和之前定义的保持一致）
struct LogEntry {
    int32_t term = 0;          // 任期号，默认初始化0
    std::string key;           // 键
    std::string value;         // 值
    std::string command_type;  // 命令（PUT/DEL/LOCK/UNLOCK）
    int64_t timestamp = 0;     // Leader 产生该日志时的 Unix 时间戳（毫秒）
    int64_t ttl = 0;           // 锁的过期时间（毫秒），0 表示永不过期
    int64_t req_id = 0;       // 锁的唯一标识符
};

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(LogEntry, term, key, value, command_type, timestamp, ttl, req_id)

// 投票请求结构体（对应 VoteRequest 消息）
struct VoteRequest {
    int32_t term = 0;          // 当前候选人的任期号
    int32_t candidateId = 0;   // 请求投票的候选人ID
    int32_t lastLogIndex = 0;  // 候选人最后日志条目的索引
    int32_t lastLogTerm = 0;   // 候选人最后日志条目的任期号
};

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(VoteRequest, term, candidateId, lastLogIndex, lastLogTerm)

// 投票回复结构体（对应 VoteReply 消息）
struct VoteReply {
    int32_t term = 0;          // 当前节点的任期号
    bool voteGranted = false;  // 是否授予投票（默认拒绝）
};

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(VoteReply, term, voteGranted)

// 日志追加请求结构体（对应 AppendRequest 消息）
struct AppendRequest {
    int32_t term = 0;          // 领导者的任期号
    int32_t leaderId = 0;      // 领导者ID（用于重定向）
    int32_t prevLogIndex = 0;  // 新日志前一条的索引
    int32_t prevLogTerm = 0;   // 新日志前一条的任期号
    std::vector<LogEntry> entries;  // 要追加的日志条目列表（repeated）
    int32_t leaderCommit = 0;  // 领导者的提交索引
};

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(AppendRequest, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)

// 日志追加回复结构体（对应 AppendReply 消息）
struct AppendReply {
    int32_t term = 0;          // 当前节点的任期号
    bool success = false;      // 日志匹配是否成功（默认失败）
};

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(AppendReply, term, success)

// Raft节点状态枚举（补充缺失的定义）
enum State {
    FOLLOWER,
    CANDIDATE,
    LEADER
};

// 定义枚举序列化（如果需要在网络中传输）
NLOHMANN_JSON_SERIALIZE_ENUM(State, {
    {State::FOLLOWER, "FOLLOWER"},
    {State::CANDIDATE, "CANDIDATE"},
    {State::LEADER, "LEADER"}
})