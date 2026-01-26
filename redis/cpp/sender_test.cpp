/**
 * Redis Sender with targeted routing support for multi-receiver testing.
 * Routes each message to test_queue_{target} based on the target field.
 */
#include <hiredis/hiredis.h>
#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <vector>
#include "../../include/json.hpp"
#include "../../include/stats_collector.hpp"

using json = nlohmann::json;

std::string generate_uuid() {
    static int counter = 0;
    return "cpp-redis-" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + "-" + std::to_string(++counter);
}

int main() {
    redisContext *c = redisConnect("127.0.0.1", 6379);
    if (c == NULL || c->err) {
        if (c) {
            std::cerr << "Error: " << c->errstr << std::endl;
            redisFree(c);
        } else {
            std::cerr << "Can't allocate redis context" << std::endl;
        }
        return 1;
    }

    std::ifstream f("/home/tim/repos/test_data.json");
    if (!f.is_open()) {
        std::cerr << "Could not open test_data.json" << std::endl;
        return 1;
    }
    json test_data = json::parse(f);

    std::string callback_queue = "callback_" + generate_uuid();

    std::cout << " [x] Starting transfer of " << test_data.size() << " messages..." << std::endl;

    MessageStats stats;
    long long global_start = get_current_time_ms();
    stats.set_duration(global_start, 0);

    for (auto& item : test_data) {
        int target = item.value("target", 0);
        std::string queue_name = "test_queue_" + std::to_string(target);
        std::string message_id = item["message_id"].is_string() ? item["message_id"].get<std::string>() : std::to_string(item["message_id"].get<long long>());
        
        std::cout << " [x] Sending message " << message_id << " to target " << target << "..." << std::flush;
        
        long long msg_start = get_current_time_ms();
        item["reply_to"] = callback_queue;
        std::string body = item.dump();

        redisReply *push_reply = (redisReply *)redisCommand(c, "RPUSH %s %b", queue_name.c_str(), body.c_str(), (size_t)body.size());
        freeReplyObject(push_reply);

        redisReply *pop_reply = (redisReply *)redisCommand(c, "BLPOP %s 0.04", callback_queue.c_str());

            if (pop_reply && pop_reply->type == REDIS_REPLY_ARRAY && pop_reply->elements == 2) {
                std::string reply_str = pop_reply->element[1]->str;
                try {
                    json resp_data = json::parse(reply_str);
                
                    auto resp_msg_id = resp_data["message_id"].is_string() ? resp_data["message_id"].get<std::string>() : std::to_string(resp_data["message_id"].get<long long>());
                    if (resp_data["status"] == "ACK" && resp_msg_id == message_id) {
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
            } else {
            stats.record_message(false);
            std::cout << " [FAILED] Timeout" << std::endl;
        }
        if (pop_reply) freeReplyObject(pop_reply);
    }

    long long global_end = get_current_time_ms();
    stats.set_duration(global_start, global_end);
    
    json report = stats.get_stats();
    report["service"] = "Redis";
    report["language"] = "C++";

    std::cout << "\nTest Results:" << std::endl;
    std::cout << "service: Redis" << std::endl;
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
    }

    redisFree(c);
    return 0;
}
