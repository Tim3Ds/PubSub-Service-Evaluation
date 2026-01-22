#include <iostream>
#include <memory>
#include <string>
#include <fstream>
#include <chrono>
#include <vector>
#include <future>
#include <mutex>
#include <grpcpp/grpcpp.h>
#include "test_data.grpc.pb.h"
#include "../../include/json.hpp"
#include "../../include/stats_collector.hpp"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using test_data::TestDataItem;
using test_data::Ack;
using test_data::TestDataService;
using json = nlohmann::json;

struct TaskResult {
    bool success;
    long long duration;
    std::string message_id;
    std::string error;
};

class TestDataClient {
public:
    TestDataClient(std::shared_ptr<Channel> channel)
        : stub_(TestDataService::NewStub(channel)) {}

    TaskResult TransferData(const TestDataItem& item) {
        Ack ack;
        ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

        long long msg_start = get_current_time_ms();
        Status status = stub_->TransferData(&context, item, &ack);

        TaskResult res;
        res.message_id = item.message_id();
        res.success = false;
        res.duration = 0;

        if (status.ok()) {
            if (ack.status() == "ACK" && ack.message_id() == item.message_id()) {
                res.duration = get_current_time_ms() - msg_start;
                res.success = true;
            } else {
                res.error = "Unexpected ACK status: " + ack.status();
            }
        } else {
            res.error = status.error_message();
        }
        return res;
    }

private:
    std::unique_ptr<TestDataService::Stub> stub_;
};

int main() {
    std::ifstream f("../../test_data.json");
    if (!f.is_open()) {
        f.open("test_data.json");
    }
    if (!f.is_open()) {
        std::cerr << "Could not open test_data.json" << std::endl;
        return 1;
    }
    json test_data = json::parse(f);

    std::cout << " [x] Starting gRPC ASYNC Sender (C++)" << std::endl;
    std::cout << " [x] Starting async transfer of " << test_data.size() << " messages..." << std::endl;

    MessageStats stats;
    long long global_start = get_current_time_ms();

    // Cache of stubs per target
    std::map<int, std::shared_ptr<TestDataClient>> clients;
    for (int i = 0; i < 32; ++i) {
        int port = 50051 + i;
        auto channel = grpc::CreateChannel("localhost:" + std::to_string(port), grpc::InsecureChannelCredentials());
        clients[i] = std::make_shared<TestDataClient>(channel);
    }

    std::vector<std::future<TaskResult>> futures;
    for (auto& item : test_data) {
        int target = item.value("target", 0);
        TestDataItem request;
        request.set_message_id(item["message_id"]);
        request.set_message_name(item["message_name"]);
        // JSON "message_value" is likely a list/array
        if (item.contains("message_value") && item["message_value"].is_array()) {
            for (const auto& val : item["message_value"]) {
                request.add_message_value(val);
            }
        }
        request.set_target(target);

        auto client = clients[target];
        futures.push_back(std::async(std::launch::async, [client, request]() -> TaskResult {
            return client->TransferData(request);
        }));
    }

    for (auto& fut : futures) {
        TaskResult res = fut.get();
        if (res.success) {
            stats.record_message(true, res.duration);
            std::cout << " [OK] Message " << res.message_id << " acknowledged" << std::endl;
        } else {
            stats.record_message(false);
            std::cout << " [FAILED] Message " << res.message_id << ": " << res.error << std::endl;
        }
    }

    long long global_end = get_current_time_ms();
    stats.set_duration(global_start, global_end);
    
    json report = stats.get_stats();
    report["service"] = "gRPC";
    report["language"] = "C++";
    report["async"] = true;

    std::cout << "\nTest Results (ASYNC):" << std::endl;
    std::cout << "total_sent: " << stats.sent_count << std::endl;
    std::cout << "total_received: " << stats.received_count << std::endl;
    std::cout << "duration_ms: " << stats.get_duration_ms() << std::endl;

    std::ofstream rf("report.txt", std::ios::app);
    if (rf.good()) {
        rf << report.dump() << std::endl;
        rf.close();
    }

    return 0;
}
