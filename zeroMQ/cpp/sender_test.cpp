#include <zmq.hpp>
#include <string>
#include <iostream>
#include <fstream>
#include <chrono>
#include <thread>
#include <map>
#include "../../utils/cpp/stats_collector.hpp"
#include "../../utils/cpp/test_data_loader.hpp"
#include "../../utils/cpp/message_helpers.hpp"

using json = nlohmann::json;
using message_helpers::get_current_time_ms;

int main() {
    zmq::context_t context(1);
    std::map<int, zmq::socket_t*> sockets;

    auto test_data = test_data_loader::loadTestData();

    MessageStats stats;
    stats.set_metadata({
        {"service", "ZeroMQ"},
        {"language", "C++"},
        {"async", false}
    });
    long long start_time = get_current_time_ms();

    std::cout << " [x] Starting transfer of " << test_data.size() << " messages..." << std::endl;

    for (auto& item : test_data) {
        std::string message_id = message_helpers::extract_message_id(item);
        int target = item.value("target", 0);
        int port = 5556 + target;

        // Get or create socket for this target
        if (sockets.find(target) == sockets.end()) {
            zmq::socket_t* sock = new zmq::socket_t(context, ZMQ_REQ);
            sock->connect("tcp://localhost:" + std::to_string(port));
            sock->setsockopt(ZMQ_RCVTIMEO, 40);  // 40ms timeout
            sockets[target] = sock;
            
            // Small delay to allow ZeroMQ connection to establish
            // (connect() is async and returns immediately)
            // Async receivers need slightly more time
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        
        zmq::socket_t* socket = sockets[target];
        std::cout << " [x] Sending message " << message_id << " to port " << port << "..." << std::flush;
        
        long long msg_start = get_current_time_ms();
        try {
            // Create and send protobuf message
            MessageEnvelope envelope = message_helpers::create_data_envelope(item);
            std::string body = message_helpers::serialize_envelope(envelope);
            
            zmq::message_t request(body.size());
            memcpy(request.data(), body.c_str(), body.size());
            socket->send(request, zmq::send_flags::none);

            // Receive ACK
            zmq::message_t reply;
            auto recv_res = socket->recv(reply);
            
            if (recv_res.has_value()) {
                std::string reply_str(static_cast<char*>(reply.data()), reply.size());
                
                // Parse and validate ACK
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
            } else {
                stats.record_message(false);
                std::cout << " [FAILED] Timeout" << std::endl;
                // Close poisoned REQ socket
                socket->close();
                delete socket;
                sockets.erase(target);
            }

        } catch (const std::exception& e) {
            stats.record_message(false);
            std::cout << " [FAILED] Error: " << e.what() << std::endl;
            socket->close();
            delete socket;
            sockets.erase(target);
        }
    }

    long long end_time = get_current_time_ms();
    stats.set_duration(start_time, end_time);
    
    json report = stats.get_stats();

    std::cout << "\nTest Results:" << std::endl;
    std::cout << "service: ZeroMQ" << std::endl;
    std::cout << "language: C++" << std::endl;
    std::cout << "total_sent: " << stats.sent_count << std::endl;
    std::cout << "total_received: " << stats.received_count << std::endl;
    std::cout << "duration_ms: " << stats.get_duration_ms() << std::endl;

    std::ofstream rf("logs/report.txt", std::ios::app);
    if (rf.good()) {
        rf << report.dump() << std::endl;
        rf.close();
    }

    // Cleanup
    for (auto& pair : sockets) {
        pair.second->close();
        delete pair.second;
    }

    return 0;
}
