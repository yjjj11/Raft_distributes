#include <logger.hpp>
#include <mrpc/server.hpp>
#include "struct.hpp"
using namespace mrpc;
#include <iostream>
// 定义自定义结构体
struct TestStruct {
    int i = 0;
    std::string str = "";
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(TestStruct, i, str)

// 修改函数签名，添加返回值
int test_struct(const TestStruct& s) {
    spdlog::debug("Received: i={}, str={}", s.i, s.str);
    return 42;  // 返回测试值
}
int test_add(int i, int j) {
    return i + j;
}
int main(int argc, char* argv[]) {
    wlog::logger::get().init("logs/_other.log");
    auto& server = server::get();
    if (argc < 2) {
        std::cout << "Usage: " << argv[0] << " <port>" << std::endl;
        return 1;
    }
    server.set_ip_port("127.0.0.1", std::stoi(argv[1]));
    server.set_server_name("other_server");
    server.run();
    server.reg_func("test_struct", test_struct);
    server.reg_func("test_add", test_add);
    std::cout << "other_server is running on port " << argv[1] << std::endl;
    server.accept();
    server.wait_shutdown();
    wlog::logger::get().shutdown();
    return 0;
}