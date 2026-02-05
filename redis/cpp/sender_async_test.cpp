#include <hiredis/hiredis.h>
#include <iostream>
#include <fstream>
#include <string>
#include <future>
#include <vector>
#include <thread>
#include <cstring>
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

TaskResult send_message_task(const json& item) {
    TaskResult res;
    res.success = false;
    res.message_id = message_helpers::extract_message_id(item);
    res.duration = 0;
    
    redisContext *c_pub = redisConnect("127.0.0.1", 6379);
    redisContext *c_sub = redisConnect("127.0.0.1", 6379);
    
    if (!c_pub || c_pub->err || !c_sub || c_sub->err) {
        res.error = "Connection failed";
        if (c_pub) redisFree(c_pub);
        if (c_sub) redisFree(c_sub);
        return res;
    }
    
    int target = item.value("target", 0);
    std::string channel = "test_channel_" + std::to_string(target);
    std::string reply_channel = "reply_" + res.message_id;
    
    // Subscribe to reply channel
    redisReply *sub = (redisReply*)redisCommand(c_sub, "SUBSCRIBE %s", reply_channel.c_str());
    if (sub) freeReplyObject(sub);
    
    // Send message
    long long msg_start = get_current_time_ms();
    MessageEnvelope envelope = message_helpers::create_data_envelope(item);
    (*envelope.mutable_metadata())["reply_to"] = reply_channel;
    std::string body = message_helpers::serialize_envelope(envelope);
    
    // Publish with retry to handle race condition where subscriber isn't ready
    int published_to = 0;
    for (int retry = 0; retry < 5 && published_to == 0; ++retry) {
        redisReply *pub = (redisReply*)redisCommand(c_pub, "PUBLISH %s %b", channel.c_str(), body.data(), body.size());
        if (pub) {
            if (pub->type == REDIS_REPLY_INTEGER) {
                published_to = (int)pub->integer;
            }
            freeReplyObject(pub);
        }
        if (published_to == 0) std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    
    // Wait for ACK
    long long timeout_ms = 80;
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = timeout_ms * 1000;
    redisSetTimeout(c_sub, tv);
    
    while ((get_current_time_ms() - msg_start) < timeout_ms) {
        redisReply *reply = nullptr;
        int status = redisGetReply(c_sub, (void**)&reply);
        
        if (status == REDIS_OK && reply) {
            if (reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3) {
                if (reply->element[0]->type == REDIS_REPLY_STRING && 
                    strcmp(reply->element[0]->str, "message") == 0) {
                    std::string reply_str(reply->element[2]->str, reply->element[2]->len);
                    
                    MessageEnvelope resp_envelope;
                    if (message_helpers::parse_envelope(reply_str, resp_envelope) && 
                        message_helpers::is_valid_ack(resp_envelope, res.message_id)) {
                        res.duration = get_current_time_ms() - msg_start;
                        res.success = true;
                        freeReplyObject(reply);
                        break;
                    }
                }
            }
            freeReplyObject(reply);
        } else if (status == REDIS_ERR) {
            if (c_sub->err == REDIS_ERR_IO && (errno == EAGAIN || errno == EINTR || errno == 0)) {
                c_sub->err = 0;
                memset(c_sub->errstr, 0, sizeof(c_sub->errstr));
            }
            break;
        }
    }
    
    // Unsubscribe
    redisSetTimeout(c_sub, {1, 0});
    redisReply *unsub = (redisReply*)redisCommand(c_sub, "UNSUBSCRIBE %s", reply_channel.c_str());
    if (unsub) freeReplyObject(unsub);
    
    redisFree(c_pub);
    redisFree(c_sub);
    
    if (!res.success && res.error.empty()) {
        res.error = "Timeout";
    }
    
    return res;
}

int main() {
    auto test_data = test_data_loader::loadTestData();

    MessageStats stats;
    stats.set_metadata({
        {"service", "Redis"},
        {"language", "C++"},
        {"async", true}
    });
    long long start_time = get_current_time_ms();

    std::cout << " [x] Starting ASYNC transfer of " << test_data.size() << " messages..." << std::endl;

    std::vector<std::future<TaskResult>> futures;
    for (auto& item : test_data) {
        futures.push_back(std::async(std::launch::async, send_message_task, item));
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

    return 0;
}
