#include <hiredis/hiredis.h>
#include <iostream>
#include <string>
#include <signal.h>
#include <thread>
#include <chrono>
#include <atomic>
#include "../../include/json.hpp"

using json = nlohmann::json;

std::atomic<bool> running(true);

void signal_handler(int sig) {
    running = false;
}

int main(int argc, char* argv[]) {
    int receiver_id = 0;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--id" && i + 1 < argc) {
            receiver_id = std::stoi(argv[i + 1]);
        }
    }

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    redisContext *c = redisConnect("127.0.0.1", 6379);
    if (c == NULL || c->err) return 1;

    std::string queue_name = "test_queue_" + std::to_string(receiver_id);
    int messages_received = 0;

    std::cout << " [*] [ASYNC] Receiver " << receiver_id << " awaiting messages on " << queue_name << std::endl;

    while (running) {
        redisReply *reply = (redisReply *)redisCommand(c, "BLPOP %s 1", queue_name.c_str());
        if (reply && reply->type == REDIS_REPLY_ARRAY && reply->elements == 2) {
            messages_received++;
            std::string body = reply->element[1]->str;
            try {
                json data = json::parse(body);
                std::string message_id = data.value("message_id", "unknown");
                std::string reply_to = data.value("reply_to", "");

                std::cout << " [Receiver " << receiver_id << "] [ASYNC] Received message " << message_id << std::endl;

                json resp;
                resp["status"] = "ACK";
                resp["message_id"] = message_id;
                resp["receiver_id"] = receiver_id;
                resp["async"] = true;

                if (!reply_to.empty()) {
                    std::string resp_str = resp.dump();
                    redisReply *ack_reply = (redisReply *)redisCommand(c, "RPUSH %s %b", reply_to.c_str(), resp_str.c_str(), (size_t)resp_str.size());
                    freeReplyObject(ack_reply);
                }
            } catch (const std::exception& e) {
                std::cerr << " [!] Error: " << e.what() << std::endl;
            }
        }
        if (reply) freeReplyObject(reply);
    }

    std::cout << " [x] [ASYNC] Receiver " << receiver_id << " shutting down (received " << messages_received << " messages)" << std::endl;

    redisFree(c);
    return 0;
}
