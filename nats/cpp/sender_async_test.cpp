#include <nats/nats.h>
#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <vector>
#include <future>
#include <mutex>
#include "../../include/json.hpp"
#include "../../include/stats_collector.hpp"

using json = nlohmann::json;

struct TaskResult {
    bool success;
    long long duration;
    std::string message_id;
    std::string error;
};

TaskResult send_message_task(natsConnection *conn, json item) {
    int target = item.value("target", 0);
    std::string subject = "test.receiver." + std::to_string(target);
    std::string message_id = item["message_id"].is_string() ? item["message_id"].get<std::string>() : std::to_string(item["message_id"].get<long long>());
    
    TaskResult res;
    res.message_id = message_id;
    res.success = false;
    res.duration = 0;

    std::string body = item.dump();
    natsMsg *reply = NULL;
    long long msg_start = get_current_time_ms();
    
    natsStatus s = natsConnection_Request(&reply, conn, subject.c_str(), body.c_str(), (int)body.size(), 40);
    
    if (s == NATS_OK) {
        try {
            std::string reply_str(natsMsg_GetData(reply), natsMsg_GetDataLength(reply));
            json resp_data = json::parse(reply_str);
            // Handle message_id that could be either string or numeric
            auto resp_msg_id = resp_data["message_id"].is_string() ? resp_data["message_id"].get<std::string>() : std::to_string(resp_data["message_id"].get<long long>());
            
            if (resp_data["status"] == "ACK" && resp_msg_id == message_id) {
                res.duration = get_current_time_ms() - msg_start;
                res.success = true;
            } else {
                res.error = "Unexpected response: " + reply_str;
            }
        } catch (...) {
            res.error = "Parse error";
        }
        natsMsg_Destroy(reply);
    } else {
        res.error = natsStatus_GetText(s);
    }
    return res;
}

int main() {
    natsConnection *conn = NULL;
    natsStatus s = natsConnection_ConnectTo(&conn, "nats://localhost:4222");
    if (s != NATS_OK) {
        std::cerr << " [!] Could not connect to NATS server" << std::endl;
        return 1;
    }

    std::string data_path = "../../test_data.json";
    std::ifstream f(data_path);
    if (!f.good()) {
        data_path = "test_data.json";
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

    std::cout << " [x] Starting NATS ASYNC Sender (C++)" << std::endl;
    std::cout << " [x] Starting async transfer of " << test_data.size() << " messages..." << std::endl;

    std::vector<std::future<TaskResult>> futures;
    for (auto& item : test_data) {
        futures.push_back(std::async(std::launch::async, [conn, item]() {
            return send_message_task(conn, item);
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

    long long end_time = get_current_time_ms();
    stats.set_duration(start_time, end_time);
    
    json report = stats.get_stats();
    report["service"] = "NATS";
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

    natsConnection_Destroy(conn);
    return 0;
}
