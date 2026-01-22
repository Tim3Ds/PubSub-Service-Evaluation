#include <hiredis/hiredis.h>
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

std::string generate_uuid() {
    static std::atomic<int> counter(0);
    return "cpp-redis-async-" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + "-" + std::to_string(++counter);
}

TaskResult send_message_task(json item, const std::string& callback_queue) {
    int target = item.value("target", 0);
    std::string queue_name = "test_queue_" + std::to_string(target);
    std::string message_id = item["message_id"];
    
    TaskResult res;
    res.message_id = message_id;
    res.success = false;
    res.duration = 0;

    redisContext *c = redisConnect("127.0.0.1", 6379);
    if (c == NULL || c->err) {
        res.error = (c ? c->errstr : "Can't allocate redis context");
        if (c) redisFree(c);
        return res;
    }

    try {
        long long msg_start = get_current_time_ms();
        item["reply_to"] = callback_queue;
        std::string body = item.dump();

        redisReply *push_reply = (redisReply *)redisCommand(c, "RPUSH %s %b", queue_name.c_str(), body.c_str(), (size_t)body.size());
        freeReplyObject(push_reply);

        redisReply *pop_reply = (redisReply *)redisCommand(c, "BLPOP %s 5", callback_queue.c_str());
        
        if (pop_reply && pop_reply->type == REDIS_REPLY_ARRAY && pop_reply->elements == 2) {
            std::string reply_str = pop_reply->element[1]->str;
            json resp_data = json::parse(reply_str);
            if (resp_data["status"] == "ACK" && resp_data["message_id"] == message_id) {
                res.duration = get_current_time_ms() - msg_start;
                res.success = true;
            } else {
                res.error = "Unexpected response: " + reply_str;
            }
        } else {
            res.error = "Timeout";
        }
        if (pop_reply) freeReplyObject(pop_reply);
    } catch (const std::exception& e) {
        res.error = e.what();
    }

    redisFree(c);
    return res;
}

int main() {
    std::ifstream f("../../test_data.json");
    if (!f.is_open()) {
        f.open("test_data.json");
    }
    if (!f.is_open()) {
        std::cerr << "Could not open test_data.json" << std::endl;
        return 1;
    }
    json test_data = json::parse(f);

    std::string callback_queue = "callback_" + generate_uuid();

    std::cout << " [x] Starting Redis ASYNC Sender (C++)" << std::endl;
    std::cout << " [x] Starting async transfer of " << test_data.size() << " messages..." << std::endl;

    MessageStats stats;
    long long global_start = get_current_time_ms();

    std::vector<std::future<TaskResult>> futures;
    for (auto& item : test_data) {
        futures.push_back(std::async(std::launch::async, send_message_task, item, callback_queue));
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

    long long global_end = get_current_time_ms();
    stats.set_duration(global_start, global_end);
    
    json report = stats.get_stats();
    report["service"] = "Redis";
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
