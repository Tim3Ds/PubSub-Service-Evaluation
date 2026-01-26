#include <zmq.hpp>
#include <string>
#include <iostream>
#include <fstream>
#include <chrono>
#include <map>
#include "../../include/json.hpp"
#include "../../include/stats_collector.hpp"

using json = nlohmann::json;

int main() {
    zmq::context_t context(1);
    
    // Cache of sockets per target
    std::map<int, zmq::socket_t*> sockets;

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
        int port = 5556 + target;

        // Get or create socket for this target
        if (sockets.find(target) == sockets.end()) {
            zmq::socket_t* sock = new zmq::socket_t(context, ZMQ_REQ);
            sock->connect("tcp://localhost:" + std::to_string(port));
            sock->setsockopt(ZMQ_RCVTIMEO, 500);  // 500ms timeout (async receiver responsiveness)
            sockets[target] = sock;
        }
        
        zmq::socket_t* socket = sockets[target];
        std::string message_id = item["message_id"].is_string() ? item["message_id"].get<std::string>() : std::to_string(item["message_id"].get<long long>());

        std::cout << " [x] Sending message " << message_id << " to target " << target 
                  << " (port " << port << ")..." << std::flush;
        
        long long msg_start = get_current_time_ms();
        try {
            std::string request_str = item.dump();
            
            zmq::message_t request(request_str.size());
            memcpy(request.data(), request_str.c_str(), request_str.size());
            socket->send(request, zmq::send_flags::none);

            zmq::message_t reply;
            auto recv_res = socket->recv(reply);
            
            if (recv_res.has_value()) {
                std::string reply_str(static_cast<char*>(reply.data()), reply.size());
                
                if (reply_str.empty()) {
                    stats.record_message(false);
                    std::cout << " [FAILED] Received empty reply" << std::endl;
                    continue;
                }

                try {
                    json resp_data = json::parse(reply_str);
                    auto resp_msg_id = resp_data["message_id"].is_string() ? resp_data["message_id"].get<std::string>() : std::to_string(resp_data["message_id"].get<long long>());

                    if (resp_data["status"] == "ACK" && resp_msg_id == message_id) {
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
                std::cout << " [FAILED] Timeout" << std::endl;
                // Close poisoned REQ socket
                socket->close();
                delete socket;
                sockets.erase(target);
            }

        } catch (const std::exception& e) {
            stats.record_message(false);
            std::cout << " [FAILED] Error: " << e.what() << std::endl;
            // Close poisoned REQ socket on any error
            socket->close();
            delete socket;
            sockets.erase(target);
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

    // Cleanup
    for (auto& pair : sockets) {
        pair.second->close();
        delete pair.second;
    }

    return 0;
}
