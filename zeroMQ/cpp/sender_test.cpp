#include <zmq.hpp>
#include <string>
#include <iostream>
#include <fstream>
#include <chrono>
#include "../../include/json.hpp"
#include "../../include/stats_collector.hpp"

using json = nlohmann::json;

int main() {
    zmq::context_t context(1);
    zmq::socket_t socket(context, ZMQ_DEALER);
    socket.setsockopt(ZMQ_IDENTITY, "sender", 6);
    socket.connect("tcp://localhost:5555");

    std::string data_path = "test_data.json";
    std::ifstream f(data_path);
    if (!f.good()) {
        data_path = "../../test_data.json";
        f.close();
        f.open(data_path);
    }
    if (!f.good()) {
        data_path = "../../../test_data.json";
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
        int target = item.value("target", 0);
        std::string receiver_id = "receiver_" + std::to_string(target);

        std::cout << " [x] Sending message " << item["message_id"] << " to target " << target << "..." << std::flush;
        
        long long msg_start = get_current_time_ms();
        try {
            std::string request_str = item.dump();
            
            // Send Multipart: [ReceiverID, Data]
            zmq::message_t id_msg(receiver_id.size());
            memcpy(id_msg.data(), receiver_id.c_str(), receiver_id.size());
            socket.send(id_msg, ZMQ_SNDMORE);
            
            zmq::message_t request(request_str.size());
            memcpy(request.data(), request_str.c_str(), request_str.size());
            socket.send(request);

            // Receive Multipart: [ReceiverID, Data]
            zmq::pollitem_t items[] = { { static_cast<void*>(socket), 0, ZMQ_POLLIN, 0 } };
            zmq::poll(&items[0], 1, 5000); // 5s timeout
            
            if (items[0].revents & ZMQ_POLLIN) {
                zmq::message_t reply;
                auto recv_res = socket.recv(reply);
                
                if (recv_res.has_value()) {
                     std::string reply_str(static_cast<char*>(reply.data()), reply.size());
                     
                     if (reply_str.empty()) {
                         stats.record_message(false);
                         std::cout << " [FAILED] Received empty reply" << std::endl;
                         continue;
                     }

                     try {
                         json resp_data = json::parse(reply_str);

                         if (resp_data["status"] == "ACK" && resp_data["message_id"] == item["message_id"]) {
                             long long msg_duration = get_current_time_ms() - msg_start;
                             stats.record_message(true, msg_duration);
                             std::cout << " [OK]" << std::endl;
                         } else {
                             stats.record_message(false);
                             std::cout << " [FAILED] Unexpected response: " << reply_str << std::endl;
                         }
                     } catch (const std::exception& e) {
                         stats.record_message(false);
                         std::cout << " [FAILED] JSON parse error: " << e.what() << " on data: '" << reply_str << "'" << std::endl;
                     }
                } else {
                    stats.record_message(false);
                    std::cout << " [FAILED] Failed to receive reply" << std::endl;
                }
            } else {
                stats.record_message(false);
                std::cout << " [FAILED] Timeout" << std::endl;
            }

        } catch (const std::exception& e) {
            stats.record_message(false);
            std::cout << " [FAILED] Error: " << e.what() << std::endl;
        }
    }

    long long end_time = get_current_time_ms();
    stats.set_duration(start_time, end_time);
    
    json report = stats.get_stats();
    report["service"] = "ZeroMQ";
    report["language"] = "C++";

    std::cout << "\nTest Results:" << std::endl;
    std::cout << "service: ZeroMQ" << std::endl;
    std::cout << "language: C++" << std::endl;
    std::cout << "total_sent: " << stats.sent_count << std::endl;
    std::cout << "total_received: " << stats.received_count << std::endl;
    std::cout << "duration_ms: " << stats.get_duration_ms() << std::endl;

    std::ofstream rf("../../report.txt", std::ios::app);
    if (rf.good()) {
        rf << report.dump() << std::endl;
        rf.close();
    } else {
        std::cerr << " [!] Warning: Could not write to report file" << std::endl;
    }

    return 0;
}
