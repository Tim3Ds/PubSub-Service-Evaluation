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
        int target = item.value("target", 0);
        std::string queue_name = "test_queue_" + std::to_string(target);
        
        std::cout << " [x] Sending message " << item["message_id"] << " to target " << target << "..." << std::flush;
        
        item["reply_to"] = callback_queue;
        std::string body = item.dump();

        redisReply *push_reply = (redisReply *)redisCommand(c, "RPUSH %s %b", queue_name.c_str(), body.c_str(), (size_t)body.size());
        freeReplyObject(push_reply);

        redisReply *pop_reply = (redisReply *)redisCommand(c, "BLPOP %s 5", callback_queue.c_str());
        
        if (pop_reply && pop_reply->type == REDIS_REPLY_ARRAY && pop_reply->elements == 2) {
            std::string reply_str = pop_reply->element[1]->str;
            try {
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
        } else {
            stats.failed++;
            std::cout << " [FAILED] Timeout" << std::endl;
        }
        if (pop_reply) freeReplyObject(pop_reply);
    }

    stats.end_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    double duration = stats.end_time - stats.start_time;
    
    json report;
    report["service"] = "Redis";
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

    std::ofstream rf("/home/tim/repos/report.txt", std::ios::app);
    rf << report.dump() << std::endl;

    redisFree(c);
    return 0;
}
