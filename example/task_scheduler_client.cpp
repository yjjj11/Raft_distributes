#include <iostream>
#include <string>
#include "task_scheduler.hpp"
#include <limits>  // 用于清空输入缓冲区
#include <queue>
using namespace mrpc;
using namespace std;

// 服务端节点配置（修改为你的实际地址）
const string SERVER_IP = "127.0.0.1";
const uint16_t SERVER_PORT = 8000;
std::vector<std::pair<std::string, int64_t>> conns = {
    {"127.0.0.1", 8000},
    {"127.0.0.1", 8001}
};
// std::queue<std::shared_ptr<connection>> conn_list;
// bool init_conn(client& client_) {
//     for(auto& conn : conns) {
//         conn_list.push(client_.connect(conn.first, conn.second, 2));
//     }
//     std::cout<<"📌 连接服务端成功！"<<std::endl;
// }

size_t conn_index = 0;
bool submit_task(client& client, int conn_index, const string& task_type, const string& task_payload) {
    try {
        auto conn=client.connect(conns[conn_index].first, conns[conn_index].second, 2);
        auto result = conn->call<bool>("make_and_submit", task_type, task_payload);
        if (result.error_msg() == "success") {
            if(result.value()) {
                cout << "\n✅ 任务提交成功！" << endl;
                return true;
            } else {
                cerr << "\n❌ 任务提交失败！" << endl;
                return false;
            }
        } else {
            cerr << "\n❌ 任务提交失败！" << (result.error_msg() != "ok" ? "错误信息：" + result.error_msg() : "") << endl;
            return false;
        }
    } catch (const exception& e) {
        cerr << "\n❌ 调用异常：" << e.what() << endl;
        return false;
    }
}

int main() {
    cout << "📌 初始化任务调度终端..." << endl;
   client& client_ = mrpc::client::get();
    client_.run();
    auto conn = client_.connect(SERVER_IP, SERVER_PORT, 2);

    bool running = true;
    std::cout << "📌 连接服务端成功！" << std::endl;

    while (running) {
        // 绘制简易界面
        system("cls");
        cout << "========================================" << endl;
        cout << "          任务调度终端 [v1.0]           " << endl;
        cout << "========================================" << endl;
        cout << "  服务端地址：" << SERVER_IP << ":" << SERVER_PORT << endl;
        cout << "========================================" << endl;
        cout << "  操作说明：输入任务内容提交，输入 q 退出" << endl;
        cout << "========================================" << endl;

        // 输入任务类型（默认data_process）
        string task_type = "data_process";
        cout << "\n📌 任务类型（默认data_process）：";
        string input_type;
        getline(cin, input_type);
        if (!input_type.empty()) {
            task_type = input_type;
        }

        // 输入任务内容
        cout << "📝 任务内容（JSON/普通字符串）：";
        string task_payload;
        getline(cin, task_payload);


        cout << "选择要连接的节点索引：";
        int conn_index;
        cin >> conn_index;
        // 退出逻辑
        if (task_payload == "q" || task_payload == "Q") {
            cout << "\n👋 正在退出终端..." << endl;
            running = false;
            continue;
        }

        // 空内容校验
        if (task_payload.empty()) {
            cerr << "\n⚠️  任务内容不能为空！按任意键继续..." << endl;
            cin.ignore(numeric_limits<streamsize>::max(), '\n');
            cin.get();
            continue;
        }

        // 提交任务
        cout << "\n🚀 正在提交任务...";
        submit_task(client_, conn_index, task_type, task_payload);

        // 等待用户确认后继续
        cout << "\n\n按任意键返回主界面...";
        cin.ignore(numeric_limits<streamsize>::max(), '\n');
        cin.get();
    }

    cout << "\n✅ 终端已安全退出" << endl;
    return 0;
}