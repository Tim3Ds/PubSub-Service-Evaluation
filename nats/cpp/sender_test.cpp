#include <nats/nats.h>
#include <iostream>
#include <fstream>
#include <string>
#include "../../utils/cpp/stats_collector.hpp"
#include "../../utils/cpp/test_data_loader.hpp"
#include "../../utils/cpp/message_helpers.hpp"

using json = nlohmann::json;
using messaging::MessageEnvelope;
using message_helpers::get_current_time_ms;

int main() {
    auto test_data = test_data_loader::loadTestData();

    MessageStats stats;
    stats.set_metadata({
        {"service", "NATS"},
        {"language", "C++"},
        {"async", false}
    });
    long long start_time = get_current_time_ms();

    std::cout << " [x] Starting transfer of " << test_data.size() << " messages..." << std::endl;

    natsConnection *conn = NULL;
    natsStatus s = natsConnection_ConnectTo(&conn, "nats://localhost:4222");
    if (s != NATS_OK) {
        std::cerr << "Connection failed: " << natsStatus_GetText(s) << std::endl;
        return 1;
    }

    for (auto& item : test_data) {
        std::string message_id = message_helpers::extract_message_id(item);
        int target = item.value("target", 0);
        std::cout << " [x] Sending message " << message_id << " to target " << target << "..." << std::flush;

        std::string subject = "test.subject." + std::to_string(target);
        long long msg_start = get_current_time_ms();

        // Create and send protobuf message
        MessageEnvelope envelope = message_helpers::create_data_envelope(item);
        std::string body = message_helpers::serialize_envelope(envelope);

        natsMsg *reply = NULL;
        s = natsConnection_Request(&reply, conn, subject.c_str(), body.data(), body.size(), 40);

        if (s == NATS_OK) {
            std::string reply_str(natsMsg_GetData(reply), natsMsg_GetDataLength(reply));
            
            MessageEnvelope resp_envelope;
            if (message_helpers::parse_envelope(reply_str, resp_envelope) && 
                message_helpers::is_valid_ack(resp_envelope, message_id)) {
                long long msg_duration = get_current_time_ms() - msg_start;
                stats.record_message(true, msg_duration);
                std::cout << " [OK]" << std::endl;
            } else {
                stats.record_message(false);
                std::cout << " [FAILED] Invalid ACK" << std::endl;
            }
            natsMsg_Destroy(reply);
        } else {
            stats.record_message(false);
            std::cout << " [FAILED] " << natsStatus_GetText(s) << std::endl;
        }
    }

    long long end_time = get_current_time_ms();
    stats.set_duration(start_time, end_time);
    
    json report = stats.get_stats();

    std::cout << "\nTest Results:" << std::endl;
    std::cout << "service: NATS" << std::endl;
    std::cout << "language: C++" << std::endl;
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
