#include <zmq.hpp>
#include <string>
#include <iostream>
#include <fstream>
#include <chrono>
#include <map>
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

std::mutex stats_mutex;

TaskResult send_message_task(zmq::context_t& context, json item) {
    int target = item.value("target", 0);
    int port = 5556 + target;
    std::string message_id = item["message_id"];
    
    TaskResult res;
    res.message_id = message_id;
    res.success = false;
    res.duration = 0;

    try {
        zmq::socket_t socket(context, ZMQ_REQ);
        socket.connect("tcp://localhost:" + std::to_string(port));
        socket.setsockopt(ZMQ_RCVTIMEO, 5000);  // 5s timeout

        long long msg_start = get_current_time_ms();
        std::string request_str = item.dump();
        
        zmq::message_t request(request_str.size());
        memcpy(request.data(), request_str.c_str(), request_str.size());
        socket.send(request, zmq::send_flags::none);

        zmq::message_t reply;
        auto recv_res = socket.recv(reply);
        
        if (recv_res.has_value()) {
            std::string reply_str(static_cast<char*>(reply.data()), reply.size());
            if (!reply_str.empty()) {
                json resp_data = json::parse(reply_str);
                if (resp_data["status"] == "ACK" && resp_data["message_id"] == message_id) {
                    res.duration = get_current_time_ms() - msg_start;
                    res.success = true;
                } else {
                    res.error = "Unexpected response: " + reply_str;
                }
            } else {
                res.error = "Received empty reply";
            }
        } else {
            res.error = "Timeout";
        }
    } catch (const std::exception& e) {
        res.error = std::string("Error: ") + e.what();
    }
    return res;
}

int main() {
    zmq::context_t context(1);
    
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

    std::cout << " [x] Starting ZeroMQ ASYNC Sender (C++)" << std::endl;
    std::cout << " [x] Starting async transfer of " << test_data.size() << " messages..." << std::endl;

    std::vector<std::future<TaskResult>> futures;
    for (auto& item : test_data) {
        futures.push_back(std::async(std::launch::async, [&context, item]() {
            return send_message_task(context, item);
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
    report["service"] = "ZeroMQ";
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

    return 0;
}
