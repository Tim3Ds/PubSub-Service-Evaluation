#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <memory>
#include <future>
#include <grpcpp/grpcpp.h>
#include "messaging.grpc.pb.h"
#include "../../utils/cpp/stats_collector.hpp"
#include "../../utils/cpp/test_data_loader.hpp"
#include "../../utils/cpp/message_helpers.hpp"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using messaging::MessageEnvelope;
using messaging::MessagingService;
using json = nlohmann::json;
using message_helpers::get_current_time_ms;

struct TaskResult {
    bool success;
    std::string message_id;
    long long duration;
    std::string error;
};

TaskResult send_message_task(const json& item, int receiver_count) {
    TaskResult res;
    res.success = false;
    res.message_id = message_helpers::extract_message_id(item);
    res.duration = 0;
    
    try {
        int target = item.value("target", 0);
        int port = 50051 + target;
        
        auto channel = grpc::CreateChannel("localhost:" + std::to_string(port), grpc::InsecureChannelCredentials());
        auto stub = MessagingService::NewStub(channel);
        
        long long msg_start = get_current_time_ms();
        
        // Create protobuf message with DataMessage payload
        MessageEnvelope request = message_helpers::create_data_envelope(item);
        
        MessageEnvelope reply;
        ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(100));
        
        Status status = stub->SendMessage(&context, request, &reply);
        
        if (status.ok()) {
            if (message_helpers::is_valid_ack(reply, res.message_id)) {
                res.duration = get_current_time_ms() - msg_start;
                res.success = true;
            } else {
                res.error = "Invalid ACK";
            }
        } else {
            res.error = status.error_message();
        }
    } catch (const std::exception& e) {
        res.error = e.what();
    }
    
    return res;
}

int main() {
    auto test_data = test_data_loader::loadTestData();
    
    // Find max target
    int max_target = 0;
    for (const auto& item : test_data) {
        if (item.contains("target")) {
            max_target = std::max(max_target, item["target"].get<int>());
        }
    }
    
    MessageStats stats;
    stats.set_metadata({
        {"service", "gRPC"},
        {"language", "C++"},
        {"async", true}
    });
    long long start_time = get_current_time_ms();
    
    std::cout << " [x] Starting ASYNC transfer of " << test_data.size() << " messages..." << std::endl;
    
    std::vector<std::future<TaskResult>> futures;
    for (auto& item : test_data) {
        futures.push_back(std::async(std::launch::async, send_message_task, item, max_target + 1));
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
    
    long long end_time = get_current_time_ms();
    stats.set_duration(start_time, end_time);
    
    json report = stats.get_stats();
    
    std::cout << "\nTest Results (ASYNC):" << std::endl;
    std::cout << "total_sent: " << stats.sent_count << std::endl;
    std::cout << "total_received: " << stats.received_count << std::endl;
    std::cout << "duration_ms: " << stats.get_duration_ms() << std::endl;
    
    std::ofstream rf("logs/report.txt", std::ios::app);
    if (rf.good()) {
        rf << report.dump() << std::endl;
        rf.close();
    }
    
    return 0;
}
