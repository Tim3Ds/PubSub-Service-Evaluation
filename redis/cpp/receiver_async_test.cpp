#include <hiredis/hiredis.h>
#include <iostream>
#include <string>
#include <cstring>
#include <signal.h>
#include <thread>
#include <atomic>
#include "../../utils/cpp/message_helpers.hpp"

using messaging::MessageEnvelope;
using message_helpers::get_current_time_ms;

std::atomic<bool> running(true);

void signal_handler(int sig) {
    running = false;
}

int main(int argc, char* argv[]) {
    int receiver_id = 0;
    
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--id") == 0 && i + 1 < argc) {
            receiver_id = std::stoi(argv[i + 1]);
            break;
        }
    }
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    redisContext *c_sub = redisConnect("127.0.0.1", 6379);
    redisContext *c_pub = redisConnect("127.0.0.1", 6379);
    
    if (c_sub == NULL || c_sub->err || c_pub == NULL || c_pub->err) {
        std::cerr << "Redis connection failed" << std::endl;
        if (c_sub) redisFree(c_sub);
        if (c_pub) redisFree(c_pub);
        return 1;
    }
    std::cout << " [+] [ASYNC] Connected to Redis" << std::endl;

    std::string channel = "test_channel_" + std::to_string(receiver_id);
    std::cout << " [*] [ASYNC] Receiver " << receiver_id << " waiting for messages on " << channel << std::endl;

    // Subscribe to channel
    redisReply *sub = (redisReply*)redisCommand(c_sub, "SUBSCRIBE %s", channel.c_str());
    if (sub) freeReplyObject(sub);

    // Set timeout for graceful shutdown
    struct timeval tv = {1, 0};
    redisSetTimeout(c_sub, tv);

    while (running) {
        redisReply *reply = nullptr;
        int status = redisGetReply(c_sub, (void**)&reply);
        
        if (status == REDIS_OK && reply) {
            if (reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3) {
                if (reply->element[0]->type == REDIS_REPLY_STRING && 
                    strcmp(reply->element[0]->str, "message") == 0) {
                    
                    std::string message_str(reply->element[2]->str, reply->element[2]->len);
                    
                    // Parse message
                    MessageEnvelope msg_envelope;
                    if (message_helpers::parse_envelope(message_str, msg_envelope)) {
                        std::string message_id = msg_envelope.message_id();
                        std::cout << " [x] [ASYNC] Received message " << message_id << std::endl;
                        
                        // Create ACK
                        MessageEnvelope response = message_helpers::create_ack_from_envelope(
                            msg_envelope,
                            std::to_string(receiver_id)
                        );
                        response.set_async(true);
                        std::string response_str = message_helpers::serialize_envelope(response);
                        
                        // Send ACK to reply channel
                        std::string reply_channel = "reply_" + message_id;
                        if (msg_envelope.metadata().count("reply_to")) {
                            reply_channel = msg_envelope.metadata().at("reply_to");
                        }

                        redisReply *pub = (redisReply*)redisCommand(c_pub, "PUBLISH %s %b", 
                            reply_channel.c_str(), response_str.data(), response_str.size());
                        if (pub) freeReplyObject(pub);
                    }
                }
            }
            freeReplyObject(reply);
        } else if (status == REDIS_ERR) {
            // Handle error (including timeout)
            if (c_sub->err == REDIS_ERR_IO && (errno == EAGAIN || errno == EINTR || errno == 0)) {
                c_sub->err = 0;
                memset(c_sub->errstr, 0, sizeof(c_sub->errstr));
            } else if (running) {
                std::cerr << " [!] Redis error: " << c_sub->errstr << " (code: " << c_sub->err << ")" << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }

    std::cout << " [x] [ASYNC] Receiver " << receiver_id << " shutting down" << std::endl;
    redisFree(c_sub);
    redisFree(c_pub);
    return 0;
}
