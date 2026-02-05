#include <nats/nats.h>
#include <iostream>
#include <fstream>
#include <string>
#include <future>
#include <vector>
#include "../../utils/cpp/stats_collector.hpp"
#include "../../utils/cpp/test_data_loader.hpp"
#include "../../utils/cpp/message_helpers.hpp"

using json = nlohmann::json;
using messaging::MessageEnvelope;
using message_helpers::get_current_time_ms;

struct TaskResult {
    bool success;
    std::string message_id;
    long long duration;
    std::string error;
};

TaskResult send_message_task(natsConnection *conn, const json& item) {
    TaskResult res;
    res.success = false;
    res.message_id = message_helpers::extract_message_id(item);
    res.duration = 0;

    int target = item.value("target", 0);
    std::string subject = "test.subject." + std::to_string(target);

    long long msg_start = get_current_time_ms();

    // Create and send message
    MessageEnvelope envelope = message_helpers::create_data_envelope(item);
    std::string body = message_helpers::serialize_envelope(envelope);

    natsMsg *reply = NULL;
    natsStatus s = natsConnection_Request(&reply, conn, subject.c_str(), body.data(), (int)body.size(), 100);

    if (s == NATS_OK) {
        std::string reply_str(natsMsg_GetData(reply), natsMsg_GetDataLength(reply));
        
        MessageEnvelope resp_envelope;
        if (message_helpers::parse_envelope(reply_str, resp_envelope) && 
            message_helpers::is_valid_ack(resp_envelope, res.message_id)) {
            res.duration = get_current_time_ms() - msg_start;
            res.success = true;
        } else {
            res.error = "Invalid ACK";
        }
        natsMsg_Destroy(reply);
    } else {
        res.error = natsStatus_GetText(s);
    }

    return res;
}

int main() {
    auto test_data = test_data_loader::loadTestData();

    MessageStats stats;
    stats.set_metadata({
        {"service", "NATS"},
        {"language", "C++"},
        {"async", true}
    });
    long long start_time = get_current_time_ms();

    natsConnection *conn = NULL;
    natsStatus s = natsConnection_ConnectTo(&conn, "nats://localhost:4222");
    if (s != NATS_OK) {
        std::cerr << "Connection failed: " << natsStatus_GetText(s) << std::endl;
        return 1;
    }

    std::cout << " [x] Starting ASYNC transfer of " << test_data.size() << " messages..." << std::endl;

    std::vector<std::future<TaskResult>> futures;
    for (auto& item : test_data) {
        futures.push_back(std::async(std::launch::async, send_message_task, conn, item));
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

    natsConnection_Destroy(conn);
    return 0;
}
