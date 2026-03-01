#include <logger.hpp>
#include <mrpc/client.hpp>
#include <iostream>
using namespace std::chrono_literals;
using namespace mrpc;

// 为自定义结构体提供序列化和反序列化函数
struct TestStruct {
    int i = 0;
    std::string str = "";
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(TestStruct, i, str)

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Usage: " << argv[0] << " <port>" << std::endl;
        return 1;
    }

    wlog::logger::get().init("logs/_other.log");
    auto& client = mrpc::client::get();
    client.run();
    auto conn = client.connect("127.0.0.1", std::stoi(argv[1]));
    if (!conn) {
        std::cerr << "Failed to connect to server" << std::endl;
        client.shutdown();
        return 1;
    }
    
    // 测试自定义结构体参数
    TestStruct test_struct{11, "12"};
    auto ret = conn->call<int>("test_struct", test_struct);
    if (ret.error_code() == mrpc::ok) {
        std::cout << "Success! Result: " << ret.value() << std::endl;
    } else {
        std::cerr << "Failed: " << ret.error_msg() << std::endl;
    }
    
    // 测试加法函数
    ret = conn->call<int>("test_add", 11, 12);
    if (ret.error_code() == mrpc::ok) {
        std::cout << "Success! Result: " << ret.value() << std::endl;
    } else {
        std::cerr << "Failed: " << ret.error_msg() << std::endl;
    }
    
    client.wait_shutdown();
    wlog::logger::get().shutdown();
    return 0;
}