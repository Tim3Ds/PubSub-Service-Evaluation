#include <hiredis/hiredis.h>
#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <thread>
#include "../../utils/cpp/stats_collector.hpp"
#include "../../utils/cpp/test_data_loader.hpp"
#include "../../utils/cpp/message_helpers.hpp"

using json = nlohmann::json;
using message_helpers::get_current_time_ms;

int main() {
    auto test_data = test_data_loader::loadTestData();

    MessageStats stats;
    stats.set_metadata({
        {"service", "Redis"},
        {"language", "C++"},
        {"async", false}
    });
    long long start_time = get_current_time_ms();

    std::cout << " [x] Starting transfer of " << test_data.size() << " messages..." << std::endl;

    redisContext *c_pub = redisConnect("127.0.0.1", 6379);
    redisContext *c_sub = redisConnect("127.0.0.1", 6379);

    if (c_pub == NULL || c_pub->err || c_sub == NULL || c_sub->err) {
        std::cerr << "Redis connection failed" << std::endl;
        if (c_pub) redisFree(c_pub);
        if (c_sub) redisFree(c_sub);
        return 1;
    }

    // Set a moderate timeout for connecting and other ops
    struct timeval tv_default = {1, 0};
    redisSetTimeout(c_pub, tv_default);
    redisSetTimeout(c_sub, tv_default);

    for (auto& item : test_data) {
        std::string message_id = message_helpers::extract_message_id(item);
        int target = item.value("target", 0);
        std::cout << " [x] Sending message " << message_id << "... " << std::flush;
        
        std::string channel = "test_channel_" + std::to_string(target);
        std::string reply_channel = "reply_" + message_id;
        
        // Subscribe to reply channel
        redisReply *sub = (redisReply*)redisCommand(c_sub, "SUBSCRIBE %s", reply_channel.c_str());
        if (sub) freeReplyObject(sub);
        
        // Create and send message
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
        
        // Wait for ACK (with 40ms timeout)
        bool got_ack = false;
        long long timeout_ms = 80;
        
        // Set a timeout on the context for redisGetReply
        struct timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = timeout_ms * 1000;
        redisSetTimeout(c_sub, tv);
        
        while (!got_ack && (get_current_time_ms() - msg_start) < timeout_ms) {
            redisReply *reply = nullptr;
            int status = redisGetReply(c_sub, (void**)&reply);
            
            if (status == REDIS_OK && reply) {
                if (reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3) {
                    if (reply->element[0]->type == REDIS_REPLY_STRING && 
                        strcmp(reply->element[0]->str, "message") == 0) {
                        std::string reply_str(reply->element[2]->str, reply->element[2]->len);
                        
                        MessageEnvelope resp_envelope;
                        if (message_helpers::parse_envelope(reply_str, resp_envelope) && 
                            message_helpers::is_valid_ack(resp_envelope, message_id)) {
                            long long msg_duration = get_current_time_ms() - msg_start;
                            stats.record_message(true, msg_duration);
                            std::cout << " [OK]" << std::endl;
                            got_ack = true;
                        }
                    }
                }
                freeReplyObject(reply);
            } else if (status == REDIS_ERR) {
                // Check if it's a timeout
                if (c_sub->err == REDIS_ERR_IO && (errno == EAGAIN || errno == EINTR || errno == 0)) {
                    // Timeout occurred, clear error to allow further commands (like UNSUBSCRIBE)
                    c_sub->err = 0;
                    memset(c_sub->errstr, 0, sizeof(c_sub->errstr));
                }
                break; // Exit waiting loop on error/timeout
            }
        }
        
        if (!got_ack) {
            stats.record_message(false);
            std::cout << " [FAILED]" << std::endl;
        }
        
        // Unsubscribe from reply channel
        // Reset timeout to something reasonable for unsubscribe
        redisSetTimeout(c_sub, tv_default);
        redisReply *unsub = (redisReply*)redisCommand(c_sub, "UNSUBSCRIBE %s", reply_channel.c_str());
        if (unsub) freeReplyObject(unsub);
    }

    long long end_time = get_current_time_ms();
    stats.set_duration(start_time, end_time);
    
    json report = stats.get_stats();

    std::cout << "\nTest Results:" << std::endl;
    std::cout << "service: Redis" << std::endl;
    std::cout << "language: C++" << std::endl;
    std::cout << "total_sent: " << stats.sent_count << std::endl;
    std::cout << "total_received: " << stats.received_count << std::endl;
    std::cout << "duration_ms: " << stats.get_duration_ms() << std::endl;

    std::ofstream rf("logs/report.txt", std::ios::app);
    if (rf.good()) {
        rf << report.dump() << std::endl;
        rf.close();
    }

    redisFree(c_pub);
    redisFree(c_sub);
    return 0;
}
