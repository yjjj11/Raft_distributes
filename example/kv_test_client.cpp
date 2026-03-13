
#include "raftnode.hpp"
#include <random>
using namespace mrpc;

// 测试配置结构体
struct TestConfig {
    std::string server_ip = "127.0.0.1";
    int server_port = 8080;
    int num_threads = 10;
    int requests_per_thread = 100;
    int key_range = 1000;
};

// 全局统计变量
std::atomic<long long> total_requests{0};
std::atomic<long long> total_latency{0};
std::vector<long long> latencies;
std::mutex latencies_mutex;

// 随机数生成器
std::mt19937 gen(std::random_device{}());
std::uniform_int_distribution<> key_dist(1, 1000);
std::uniform_int_distribution<> op_dist(0, 2); // 0: put, 1: get, 2: delete

// 工作线程函数
void worker_thread(int thread_id, const TestConfig& config) {
    // 获取单例客户端
    client& client_instance = client::get();

    // 连接到服务器
    auto conn = client_instance.connect(config.server_ip, config.server_port, 2);
    if (!conn) {
        std::cerr << "Thread " << thread_id << " failed to connect to "
                  << config.server_ip << ":" << config.server_port << std::endl;
        return;
    }

    for (int i = 0; i < config.requests_per_thread; ++i) {
        // 生成随机key和value
        int key_num = key_dist(gen);
        std::string key = "key_" + std::to_string(key_num);
        std::string value = "value_" + std::to_string(key_num) + "_thread_" + std::to_string(thread_id);

        // 随机选择操作
        int op = op_dist(gen);
        auto start_time = std::chrono::high_resolution_clock::now();

        try {
            if (op == 0) {
                // PUT 操作
                auto success = conn->call<void>("Put", key, value);
            } else if (op == 1) {
                // GET 操作
                auto result = conn->call<std::string>("Get", key);
            } else {
                // DELETE 操作
                auto success = conn->call<void>("Del", key);
            }
        } catch (const std::exception& e) {
            std::cerr << "Thread " << thread_id << " request failed: " << e.what() << std::endl;
            continue;
        }

        // 计算延迟
        auto end_time = std::chrono::high_resolution_clock::now();
        long long latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

        // 线程安全地记录延迟


        total_latency += latency_us;
        total_requests++;
    }

    // 连接自动关闭
}

// 计算分位数
double calculate_percentile(const std::vector<long long>& data, double percentile) {
    if (data.empty()) return 0.0;
    size_t index = static_cast<size_t>(data.size() * percentile / 100.0);
    if (index >= data.size()) index = data.size() - 1;
    return data[index];
}

// 主函数
int main(int argc, char* argv[]) {
    TestConfig config;

    // 解析命令行参数
    if (argc > 1) {
        std::string addr = argv[1];
        size_t colon_pos = addr.find(':');
        if (colon_pos != std::string::npos) {
            config.server_ip = addr.substr(0, colon_pos);
            config.server_port = std::stoi(addr.substr(colon_pos + 1));
        }
    }

    std::cout << "=== KV Store Performance Test ===" << std::endl;
    std::cout << "Server: " << config.server_ip << ":" << config.server_port << std::endl;
    std::cout << "Threads: " << config.num_threads << std::endl;
    std::cout << "Requests per thread: " << config.requests_per_thread << std::endl;
    std::cout << "Total requests: " << (config.num_threads * config.requests_per_thread) << std::endl;
    std::cout << "Key range: 1-" << config.key_range << std::endl;

    // 初始化客户端
    client& client_instance = client::get();
    client_instance.run();

    // 记录测试开始时间
    auto test_start = std::chrono::high_resolution_clock::now();

    // 启动工作线程
    std::vector<std::thread> threads;
    for (int i = 0; i < config.num_threads; ++i) {
        threads.emplace_back(worker_thread, i, std::ref(config));
    }

    // 等待所有线程完成
    for (auto& t : threads) {
        t.join();
    }

    // 记录测试结束时间
    auto test_end = std::chrono::high_resolution_clock::now();
    double total_time_sec = std::chrono::duration_cast<std::chrono::milliseconds>(test_end - test_start).count() / 1000.0;

    // 计算统计信息
    long long completed_requests = total_requests.load();
    double throughput = completed_requests / total_time_sec;
    double avg_latency = completed_requests > 0 ? total_latency.load() / static_cast<double>(completed_requests) : 0.0;

    // 排序延迟数据计算分位数
    std::sort(latencies.begin(), latencies.end());
    double p50 = calculate_percentile(latencies, 50.0);
    double p95 = calculate_percentile(latencies, 95.0);
    double p99 = calculate_percentile(latencies, 99.0);

    // 输出结果
    std::cout << "\n=== Test Results ===" << std::endl;
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Total time: " << total_time_sec << " seconds" << std::endl;
    std::cout << "Completed requests: " << completed_requests << std::endl;
    std::cout << "Throughput: " << throughput << " ops/sec" << std::endl;
    std::cout << "Average latency: " << avg_latency << " μs" << std::endl;
    std::cout << "50th percentile latency: " << p50 << " μs" << std::endl;
    std::cout << "95th percentile latency: " << p95 << " μs" << std::endl;
    std::cout << "99th percentile latency: " << p99 << " μs" << std::endl;

    // 清理客户端（先停止IO，再等待线程退出）
    client_instance.wait_shutdown();

    return 0;
}
