#ifndef MRPC_CLIENT_HPP
#define MRPC_CLIENT_HPP

#pragma once

#include <asio.hpp>
#include "router.hpp"
#include "connection.hpp"
#include "ZookeeperUtil.hpp"
namespace mrpc {
using namespace asio::ip;
class connection;

/**
 *  client for global
 */
class client final : private asio::noncopyable {
  public:
    /**
     * singleton for client
     */
    static client& get() {
        static client obj;
        return obj;
    }

    /**
     * export router object
     */
    mrpc::router& router() {
        return router_;
    }

    std::shared_ptr<connection> connect(const std::string& host, uint16_t port,
                                        std::time_t timeout = connection::DEFAULT_TIMEOUT) {
        auto conn = std::make_shared<connection>(asio::ip::tcp::socket(get_iocontext()), router_);
        if (conn == nullptr) return nullptr;
        return conn->connect(host, port, timeout) ? conn : nullptr;
    }

    std::shared_ptr<connection> async_connect(const std::string& host, uint16_t port) {
        auto conn = std::make_shared<connection>(asio::ip::tcp::socket(get_iocontext()), router_);
        if (conn == nullptr) return nullptr;
        conn->async_connect(host, port);
        return conn;
    }

    /**
     *  use one thread per iocontex, and auto runing
     *
     * @param io_count io_context pool size, default is double cpu count
     * @param thread_per_io thread count per io_context
     */
    void run(std::size_t io_count = 0,
             std::size_t thread_per_io = 1) {
        if (is_running_) return; // prevent call repeated.
        if (io_count < 1) {
            io_count = std::thread::hardware_concurrency() * 2;
        }
		iocs_.clear();
        //创建线程池
		for (std::size_t i = 0; i < io_count; ++i) {
			auto ioc = std::make_shared<asio::io_context>();
			iocs_.push_back(ioc);
            // assign a work, or io will stop
            workds_.emplace_back(std::make_shared<asio::io_context::work>(*ioc));
            for (std::size_t i = 0; i < thread_per_io; ++i) {
                thread_pool_.emplace_back([ioc]() {
                    ioc->run();
                });
            }
        }
        is_running_ = true;
        LOG_INFO("------------------------------client runing ----------------------------");

        zk_client_.start();
        zk_client_.setNodeChangeCallback(
            [this](const std::string& path, ZkNodeEventType type, const std::string& node) {
                this->onZkNodeChanged(path, type, node);
            }
        );

        // 3. 初始化拉取服务列表（首次加载）
        // refreshAllServiceList();
        zk_servers_ = zk_client_.getServiceList(root_path);
    }

    void onZkNodeChanged(const std::string& path, ZkNodeEventType type, const std::string& node) {
        if (type == ZkNodeEventType::SESSION_EXPIRED) {
            LOG_ERROR("ZK会话过期，重连中...");
            zk_client_.start();
            std::this_thread::sleep_for(std::chrono::seconds(1));
            refreshAllServiceList();
            return;
        }

        // 只要是 /mrpc 下的任何变化，直接全量刷新（最稳、最简单、最不容易漏）
        if (path.find("/mrpc") == 0) {
            LOG_INFO("触发服务列表全量刷新，变化路径:{}", path);
            refreshAllServiceList();
        }
    }

    void refreshAllServiceList() {
        std::lock_guard<std::mutex> lock(mutex_); // 线程安全锁

        // 第一步：刷新根路径 /mrpc 下的服务名列表
        zk_servers_ = zk_client_.getServiceList(root_path);
        if (!zk_servers_.has_value()) {
            LOG_ERROR("刷新服务列表失败！");
            return;
        }

        // 第二步：清空旧的函数列表，重新拉取每个服务下的函数
        func_list_map_.clear();
        LOG_INFO("=== 刷新后的服务列表 ===");
        for (const auto& service : zk_servers_.value()) {
            LOG_INFO("Service: {}", service);
            std::string func_path = root_path + "/" + service;
            
            // 拉取函数列表（自动注册监听）
            auto func_list = zk_client_.getServiceList(func_path);
            if (func_list.has_value()) {
                func_list_map_[service] = func_list.value(); // 保存到函数列表map
                for (const auto& func : func_list.value()) {
                    LOG_INFO("  └── Function: {}", func);
                }
            } else {
                LOG_WARN("服务 {} 下无函数列表", service);
            }
        }
    }

    std::optional<std::vector<std::string>> getLatestServiceList() {
        std::lock_guard<std::mutex> lock(mutex_);
        return zk_servers_;
    }

    std::optional<std::vector<std::string>> getLatestFuncList(const std::string& service) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = func_list_map_.find(service);
        if (it != func_list_map_.end()) {
            return it->second;
        }
        return std::nullopt;
    }


    std::optional<std::pair<std::string, uint16_t>> find_service_by_func(const std::string& func_name) {
        std::lock_guard<std::mutex> lock(mutex_);
        // LOG_DEBUG("查找函数 {} 所属服务", func_name);
        // 遍历所有服务，查找包含该函数的服务
        for (const auto& [service_name, func_list] : func_list_map_) {
            // 检查当前服务是否包含目标函数
            // LOG_DEBUG("检查服务 {} 是否包含函数 {}", service_name, func_name);
            if (std::find(func_list.begin(), func_list.end(), func_name) != func_list.end()) {
                // 从服务节点获取IP和Port（/mrpc/服务名 节点的数据是IP:Port）
                // LOG_DEBUG("服务 {} 包含函数 {}", service_name, func_name);
                std::string service_path = root_path + "/" + service_name;
                auto ip_port = zk_client_.getData(service_path);
                if (ip_port.has_value()) {
                    return std::make_pair(ip_port->first, static_cast<uint16_t>(ip_port->second));
                }
                // LOG_ERROR("服务 {} 下无IP:Port数据", service_name);
            }
            // LOG_DEBUG("服务 {} 不包含函数 {}", service_name, func_name);
        }
        // LOG_ERROR("未找到函数 {} 所属服务", func_name);
        return std::nullopt;
    }
    
    template<typename RET = void,   typename... Args>
    req_result<RET> call(const std::string& rpc_name, Args&& ... args) {
        auto ip_port = find_service_by_func(rpc_name);
        if (!ip_port.has_value()) {
            // LOG_ERROR("未找到服务 {} 的IP:Port", rpc_name);
            return req_result<RET>(status::not_found, "未找到服务 " + rpc_name + " 的IP:Port");
        }
        auto conn = connect(ip_port.value().first, ip_port.value().second);
        if (conn == nullptr){
            LOG_ERROR("连接服务 {} 失败", rpc_name);
            return req_result<RET>(status::not_found, "连接服务 " + rpc_name + " 失败");
        }
        // LOG_DEBUG("调用服务 {} 方法 {}", rpc_name, typeid(RET).name());
        return conn->call<RET>(rpc_name, std::forward<Args>(args)...);
    }


    template<typename... Args>
    void  async_call(callback_t::callback_func_type cb, const std::string& rpc_name, Args&&...args) {
        auto ip_port = find_service_by_func(rpc_name);
        if (!ip_port.has_value()) {
            LOG_ERROR("未找到服务 {} 的IP:Port", rpc_name);
            return;
        }
        auto conn = connect(ip_port.value().first, ip_port.value().second);
        if (conn == nullptr){
            LOG_ERROR("连接服务 {} 失败", rpc_name);
            return;
        }
        conn->async_call(cb, rpc_name, std::forward<Args>(args)...);
    }

#ifdef _USE_COROUTINE
    template<typename RET = void, typename ...Args>
    task_awaitable<RET> coro_call(const std::string& rpc_name, Args&&...args) {
        auto ip_port = find_service_by_func(rpc_name);
        if (!ip_port.has_value()) {
            LOG_ERROR("未找到服务 {} 的IP:Port", rpc_name);
            return task_awaitable<RET>();
        }
        auto conn = connect(ip_port.value().first, ip_port.value().second);
        if (conn == nullptr){
            LOG_ERROR("连接服务 {} 失败", rpc_name);
            return task_awaitable<RET>();
        }
        return conn->coro_call<RET>(rpc_name, std::forward<Args>(args)...);
}
#endif // _USE_COROUTINE
        /**
     *  shutdown all services and threads
     */
    void shutdown() {
        for (auto& ioc : iocs_) {
            ioc->stop();
        }
    }

    /**
     * wait all server and thread stoped
     */
    void wait_shutdown() {
        for (auto& thread : thread_pool_) {
            thread.join();
        }
    }

  private:
    client() {}
    ~client() {
        shutdown();
    }

    asio::io_context& get_iocontext() {
        // round-robin
        if (iocs_.size() < 2) {
            return *(iocs_.at(0));
        }
        ++next_ioc_index_;
        if (next_ioc_index_ >= iocs_.size()) {
            next_ioc_index_ = 1; // the first io_context only for accept
        }
        auto& ioc = iocs_[next_ioc_index_];
        return *ioc;
    }

  private:
	mrpc::router router_;
    Zookeeperutil zk_client_;
    std::atomic_bool is_running_ = false;                   // check is running, prevent multiple call run functions
    std::atomic_uint64_t next_ioc_index_ = 0;               // use atomic ensure thread safe
    std::vector<std::shared_ptr<asio::io_context>> iocs_;   // io pool
    std::vector<std::thread> thread_pool_;                  // thread pool
    std::vector<std::shared_ptr<asio::io_context::work>> workds_;

    std::string root_path = "/mrpc";         // ZK根路径
    std::optional<std::vector<std::string>> zk_servers_; // 服务名列表
    std::unordered_map<std::string, std::vector<std::string>> func_list_map_; // 服务->函数列表映射
    std::mutex mutex_;   
};
} // namespace mrpc

#endif // MRPC_CLIENT_HPP

