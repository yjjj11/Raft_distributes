#include <vector>
#include <string>
#include <nlohmann/json.hpp>

// 引入nlohmann json宏支持
using json = nlohmann::json;

// 先引入自定义的Raft消息结构体（和之前定义的保持一致）
struct LogEntry {
    int32_t term = 0;          // 任期号，默认初始化0
    std::string command_type;  // 命令（PUT/DEL/LOCK/UNLOCK）
    int64_t req_id = 0;       // 锁的唯一标识符
    std::string buffer;       // 命令参数（JSON格式）
};

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(LogEntry, term, command_type, req_id, buffer)

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

enum class TaskStatus {
    Pending = 0,
    Executing = 1,
    Completed = 2,
    Failed = 3
};

namespace nlohmann {
    template<>
    struct adl_serializer<TaskStatus> {
        static void to_json(json& j, const TaskStatus& s) {
            j = static_cast<int>(s);
        }
        static void from_json(const json& j, TaskStatus& s) {
            s = static_cast<TaskStatus>(j.get<int>());
        }
    };
}

using TaskId = std::string;
using TaskPayload = std::string;
using ExecuteTimeMs = int64_t; // Unix timestamp in milliseconds
using ExecutorId = std::string;
// 任务结构体
struct ScheduledTask {
    TaskId id;
    ExecuteTimeMs execute_at; // 执行时间戳 (毫秒)
    TaskPayload payload;      // 任务负载
    TaskStatus status;        // 任务状态
};

// 使用宏自动定义序列化/反序列化逻辑
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(ScheduledTask, id, execute_at, payload, status);


// 任务状态信息结构
struct TaskStatusInfo {
    TaskStatus status;
    int64_t created_at;
    int64_t updated_at;
    int64_t started_at = 0;
    int64_t completed_at = 0;
    std::string error_message = "";
};

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(TaskStatusInfo, status, created_at, updated_at, started_at, completed_at, error_message);