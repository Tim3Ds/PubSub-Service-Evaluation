#include <nats/nats.h>
#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <vector>
#include "../../include/json.hpp"
#include "../../include/stats_collector.hpp"

using json = nlohmann::json;

int main() {
    natsConnection *conn = NULL;
    natsStatus s = natsConnection_ConnectTo(&conn, "nats://localhost:4222");
    if (s != NATS_OK) return 1;

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
        std::cerr << " [!] Could not find test_data.json" << std::endl;
        return 1;
    }
    
    json test_data = json::parse(f);

    MessageStats stats;
    long long start_time = get_current_time_ms();
    stats.set_duration(start_time, 0);

    std::cout << " [x] Starting transfer of " << test_data.size() << " messages..." << std::endl;

    for (auto& item : test_data) {
        std::string message_id = item["message_id"].is_string() ? item["message_id"].get<std::string>() : std::to_string(item["message_id"].get<long long>());
        std::cout << " [x] Sending message " << message_id << " (" << item["message_name"] << ")..." << std::flush;
        
        int target = item.value("target", 0);
        std::string subject = "test.receiver." + std::to_string(target);
        
        long long msg_start = get_current_time_ms();
        natsMsg *reply = NULL;
        std::string body = item.dump();
        s = natsConnection_Request(&reply, conn, subject.c_str(), body.c_str(), (int)body.size(), 40);
        
        if (s == NATS_OK) {
            try {
                std::string reply_str(natsMsg_GetData(reply), natsMsg_GetDataLength(reply));
                json resp_data = json::parse(reply_str);
                
                if (resp_data["status"] == "ACK" && resp_data["message_id"] == item["message_id"]) {
                    long long msg_duration = get_current_time_ms() - msg_start;
                    stats.record_message(true, msg_duration);
                    std::cout << " [OK]" << std::endl;
                } else {
                    stats.record_message(false);
                    std::cout << " [FAILED] Unexpected response" << std::endl;
                }
            } catch (...) {
                stats.record_message(false);
                std::cout << " [FAILED] Parse error" << std::endl;
            }
            natsMsg_Destroy(reply);
        } else {
            stats.record_message(false);
            std::cout << " [FAILED] Timeout or error: " << natsStatus_GetText(s) << std::endl;
        }
    }

    long long end_time = get_current_time_ms();
    stats.set_duration(start_time, end_time);
    
    json report = stats.get_stats();
    report["service"] = "NATS";
    report["language"] = "C++";

    std::cout << "\nTest Results:" << std::endl;
    std::cout << "service: NATS" << std::endl;
    std::cout << "language: C++" << std::endl;
    std::cout << "total_sent: " << stats.sent_count << std::endl;
    std::cout << "total_received: " << stats.received_count << std::endl;
    std::cout << "duration_ms: " << stats.get_duration_ms() << std::endl;
    if (report.contains("message_timing_stats")) {
        std::cout << "message_timing_stats: " << report["message_timing_stats"].dump() << std::endl;
    }

    std::ofstream rf("report.txt", std::ios::app);
    if (rf.good()) {
        rf << report.dump() << std::endl;
        rf.close();
    } else {
        std::cerr << " [!] Warning: Could not write to report file" << std::endl;
    }

    natsConnection_Destroy(conn);
    return 0;
}
