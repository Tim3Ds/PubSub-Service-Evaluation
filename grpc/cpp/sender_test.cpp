#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <memory>
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

class MessageClient {
private:
    std::vector<std::unique_ptr<MessagingService::Stub>> stubs_;
    
public:
    MessageClient(int num_receivers) {
        for (int i = 0; i < num_receivers; i++) {
            int port = 50051 + i;
            auto channel = grpc::CreateChannel("localhost:" + std::to_string(port), grpc::InsecureChannelCredentials());
            stubs_.push_back(MessagingService::NewStub(channel));
        }
    }
    
    bool SendMessage(const json& item) {
        std::string message_id = message_helpers::extract_message_id(item);
        int target = item.value("target", 0);
        
        // Create protobuf message with DataMessage payload
        MessageEnvelope request = message_helpers::create_data_envelope(item);

        MessageEnvelope reply;
        ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(40));

        Status status = stubs_[target]->SendMessage(&context, request, &reply);

        if (status.ok()) {
            return message_helpers::is_valid_ack(reply, message_id);
        } else {
            std::cout << " [FAILED] gRPC error: " << status.error_message() << std::endl;
            return false;
        }
    }
};

int main() {
    auto test_data = test_data_loader::loadTestData();
    
    // Find max target to create all necessary stubs
    int max_target = 0;
    for (const auto& item : test_data) {
        if (item.contains("target")) {
            max_target = std::max(max_target, item["target"].get<int>());
        }
    }
    
    MessageClient client(max_target + 1);
    MessageStats stats;
    stats.set_metadata({
        {"service", "gRPC"},
        {"language", "C++"},
        {"async", false}
    });
    long long start_time = get_current_time_ms();
    
    std::cout << " [x] Starting transfer of " << test_data.size() << " messages..." << std::endl;
    
    for (auto& item : test_data) {
        std::string message_id = message_helpers::extract_message_id(item);
        int target = item.value("target", 0);
        std::cout << " [x] Sending message " << message_id << " to target " << target << "..." << std::flush;
        
        long long msg_start = get_current_time_ms();
        if (client.SendMessage(item)) {
            long long msg_duration = get_current_time_ms() - msg_start;
            stats.record_message(true, msg_duration);
            std::cout << " [OK]" << std::endl;
        } else {
            stats.record_message(false);
            std::cout << " [FAILED]" << std::endl;
        }
    }
    
    long long end_time = get_current_time_ms();
    stats.set_duration(start_time, end_time);
    
    json report = stats.get_stats();
    
    std::cout << "\nTest Results:" << std::endl;
    std::cout << "service: gRPC" << std::endl;
    std::cout << "language: C++" << std::endl;
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
