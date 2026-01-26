#include <nats/nats.h>
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

void onMsg(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure) {
    int *messages_received = (int*)closure;
    (*messages_received)++;

    try {
        std::string request_str(natsMsg_GetData(msg), natsMsg_GetDataLength(msg));
        json data = json::parse(request_str);
        // Handle message_id that could be either string or numeric
        std::string message_id;
        if (data["message_id"].is_string()) {
            message_id = data["message_id"].get<std::string>();
        } else {
            message_id = std::to_string(data["message_id"].get<int>());
        }

        std::cout << " [ASYNC] Received message " << message_id << std::endl;

        json resp;
        resp["status"] = "ACK";
        resp["message_id"] = data["message_id"];  // Keep original type
        resp["async"] = true;

        std::string resp_str = resp.dump();
        natsConnection_Publish(nc, natsMsg_GetReply(msg), resp_str.c_str(), (int)resp_str.size());
    } catch (const std::exception& e) {
        std::cerr << " [!] Error: " << e.what() << std::endl;
    }

    natsMsg_Destroy(msg);
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

    natsConnection *conn = NULL;
    natsStatus s = natsConnection_ConnectTo(&conn, "nats://localhost:4222");
    if (s != NATS_OK) return 1;

    std::string subject = "test.receiver." + std::to_string(receiver_id);
    int messages_received = 0;

    natsSubscription *sub = NULL;
    s = natsConnection_Subscribe(&sub, conn, subject.c_str(), onMsg, &messages_received);
    
    if (s == NATS_OK) {
        std::cout << " [*] [ASYNC] Receiver " << receiver_id << " subscribed to " << subject << std::endl;
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    std::cout << " [x] [ASYNC] Receiver " << receiver_id << " shutting down (received " << messages_received << " messages)" << std::endl;

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(conn);
    return 0;
}
