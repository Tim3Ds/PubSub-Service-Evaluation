#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <memory>
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

class TestDataClient {
public:
    TestDataClient() {
        // Create channels for all 32 receivers
        for (int i = 0; i < 32; ++i) {
            std::string address = "localhost:" + std::to_string(50051 + i);
            stubs_.push_back(TestDataService::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials())));
        }
    }

    bool TransferData(const json& item) {
        TestDataItem request;
        request.set_message_id(item["message_id"]);
        request.set_message_name(item["message_name"]);
        for (const auto& val : item["message_value"]) {
            request.add_message_value(val);
        }
        
        int target = 0;
        if (item.contains("target")) {
            target = item["target"];
        }
        request.set_target(target);

        // Ensure target is within range
        if (target < 0 || target >= 32) {
             std::cout << " [FAILED] Invalid target: " << target << std::endl;
             return false;
        }

        Ack reply;
        ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

        Status status = stubs_[target]->TransferData(&context, request, &reply);

        if (status.ok()) {
            return reply.status() == "ACK" && reply.message_id() == item["message_id"];
        } else {
            std::cout << " [FAILED] gRPC error sending to target " << target << ": " << status.error_message() << std::endl;
            return false;
        }
    }

private:
    std::vector<std::unique_ptr<TestDataService::Stub>> stubs_;
};

int main() {
    try {
        std::string data_path = "test_data.json";
        std::ifstream f(data_path);
        if (!f.good()) {
            data_path = "../../test_data.json";
            f.close();
            f.open(data_path);
        }
        if (!f.good()) {
            data_path = "/home/tim/repos/test_data.json";
            f.close();
            f.open(data_path);
        }
        
        if (!f.good()) {
             throw std::runtime_error("Could not find test_data.json");
        }
        
        json test_data = json::parse(f);

        TestDataClient client;
        std::cout << " [x] Starting transfer of " << test_data.size() << " messages..." << std::endl;

        MessageStats stats;
        stats.set_duration(get_current_time_ms(), 0);
        long long start_time = get_current_time_ms();

        for (auto& item : test_data) {
            int target = 0;
            if (item.contains("target")) target = item["target"];
            
            std::cout << " [x] Sending message " << item["message_id"] << " to target " << target << "..." << std::flush;
            
            long long msg_start = get_current_time_ms();
            if (client.TransferData(item)) {
                std::cout << " [OK]" << std::endl;
                long long msg_duration = get_current_time_ms() - msg_start;
                stats.record_message(true, msg_duration);
            } else {
                stats.record_message(false);
            }
        }
        
        long long end_time = get_current_time_ms();
        stats.set_duration(start_time, end_time);

        // Generate complete report
        json report = stats.get_stats();
        report["service"] = "gRPC";
        report["language"] = "C++";

        std::cout << "\nTest Results:" << std::endl;
        std::cout << "service: gRPC" << std::endl;
        std::cout << "language: C++" << std::endl;
        std::cout << "total_sent: " << stats.sent_count << std::endl;
        std::cout << "total_received: " << stats.received_count << std::endl;
        std::cout << "duration_ms: " << stats.get_duration_ms() << std::endl;

        std::ofstream report_file("../../report.txt", std::ios_base::app);
        if (report_file.good()) {
            report_file << report.dump() << std::endl;
            report_file.close();
        } else {
            std::cerr << " [!] Warning: Could not write to report file" << std::endl;
        }

    } catch (const std::exception& e) {
        std::cerr << " [!] Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
