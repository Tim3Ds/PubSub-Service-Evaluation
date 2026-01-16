/**
 * Redis Receiver with targeted routing support for multi-receiver testing.
 * Each receiver listens on test_queue_{id} based on its assigned ID.
 * Usage: ./receiver_test --id <0-31>
 */
#include <hiredis/hiredis.h>
#include <iostream>
#include <string>
#include <cstring>
#include "../../include/json.hpp"

using json = nlohmann::json;

int main(int argc, char* argv[]) {
    int receiver_id = 0;
    
    // Parse --id argument
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--id") == 0 && i + 1 < argc) {
            receiver_id = std::stoi(argv[i + 1]);
            break;
        }
    }
    
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

    std::string queue_name = "test_queue_" + std::to_string(receiver_id);
    std::cout << " [*] Receiver " << receiver_id << " waiting for messages on " << queue_name << ". To exit press CTRL+C" << std::endl;

    while (true) {
        redisReply *reply = (redisReply *)redisCommand(c, "BLPOP %s 1", queue_name.c_str());
        
        if (reply == NULL) {
            continue;
        }
        
        if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 2) {
            std::string message = reply->element[1]->str;
            try {
                json data = json::parse(message);
                auto message_id = data["message_id"];
                std::string reply_to = data["reply_to"];

                json response;
                response["status"] = "ACK";
                response["message_id"] = message_id;
                response["receiver_id"] = receiver_id;

                std::string resp_str = response.dump();
                redisReply *push_reply = (redisReply *)redisCommand(c, "RPUSH %s %b", reply_to.c_str(), resp_str.c_str(), (size_t)resp_str.size());
                freeReplyObject(push_reply);
                
                redisReply *expire_reply = (redisReply *)redisCommand(c, "EXPIRE %s 60", reply_to.c_str());
                freeReplyObject(expire_reply);

            } catch (...) {
                std::cerr << "Error parsing message" << std::endl;
            }
        }
        freeReplyObject(reply);
    }

    redisFree(c);
    return 0;
}
