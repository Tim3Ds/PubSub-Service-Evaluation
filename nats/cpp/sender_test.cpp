#include <nats/nats.h>
#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include "../../include/json.hpp"

using json = nlohmann::json;

int main() {
    natsConnection *conn = NULL;
    natsStatus s = natsConnection_ConnectTo(&conn, "nats://localhost:4222");
    if (s != NATS_OK) return 1;

    std::ifstream f("../../test_data.json");
    json test_data = json::parse(f);

    struct {
        int sent = 0;
        int received = 0;
        int processed = 0;
        int failed = 0;
        long long start_time;
        long long end_time;
    } stats;

    stats.start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    std::cout << " [x] Starting transfer of " << test_data.size() << " messages..." << std::endl;

    for (auto& item : test_data) {
        stats.sent++;
        std::cout << " [x] Sending message " << item["message_id"] << " (" << item["message_name"] << ")..." << std::flush;
        
        natsMsg *reply = NULL;
        std::string body = item.dump();
        s = natsConnection_Request(&reply, conn, "test_subject", body.c_str(), (int)body.size(), 2000);
        
        if (s == NATS_OK) {
            try {
                std::string reply_str(natsMsg_GetData(reply), natsMsg_GetDataLength(reply));
                json resp_data = json::parse(reply_str);
                
                if (resp_data["status"] == "ACK" && resp_data["message_id"] == item["message_id"]) {
                    stats.received++;
                    stats.processed++;
                    std::cout << " [OK]" << std::endl;
                } else {
                    stats.failed++;
                    std::cout << " [FAILED] Unexpected response" << std::endl;
                }
            } catch (...) {
                stats.failed++;
                std::cout << " [FAILED] Parse error" << std::endl;
            }
            natsMsg_Destroy(reply);
        } else {
            stats.failed++;
            std::cout << " [FAILED] Timeout or error: " << natsStatus_GetText(s) << std::endl;
        }
    }

    stats.end_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    double duration = stats.end_time - stats.start_time;
    
    json report;
    report["service"] = "NATS";
    report["language"] = "C++";
    report["total_sent"] = stats.sent;
    report["total_received"] = stats.received;
    report["total_processed"] = stats.processed;
    report["total_failed"] = stats.failed;
    report["duration_ms"] = duration;
    report["processed_per_ms"] = duration > 0 ? stats.processed / duration : 0;
    report["failed_per_ms"] = duration > 0 ? stats.failed / duration : 0;

    std::cout << "\nTest Results:" << std::endl;
    std::cout << report.dump(4) << std::endl;

    std::ofstream rf("../../report.txt", std::ios::app);
    rf << report.dump() << std::endl;

    natsConnection_Destroy(conn);
    return 0;
}
