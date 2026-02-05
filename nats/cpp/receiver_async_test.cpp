#include <nats/nats.h>
#include <iostream>
#include <string>
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

void onMsg(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure) {
    int *receiver_id = (int*)closure;

    std::string request_str(natsMsg_GetData(msg), natsMsg_GetDataLength(msg));
    
    MessageEnvelope request_envelope;
    if (message_helpers::parse_envelope(request_str, request_envelope)) {
        std::string message_id = request_envelope.message_id();
        std::cout << " [x] [ASYNC] Received message " << message_id << std::endl;

        // Create ACK
        MessageEnvelope response = message_helpers::create_ack_from_envelope(
            request_envelope,
            std::to_string(*receiver_id)
        );
        response.set_async(true);
        std::string resp_str = message_helpers::serialize_envelope(response);
        
        // Send reply
        if (natsMsg_GetReply(msg)) {
            natsConnection_Publish(nc, natsMsg_GetReply(msg), resp_str.c_str(), (int)resp_str.size());
        }
    }

    natsMsg_Destroy(msg);
}

int main(int argc, char* argv[]) {
    int receiver_id = 0;
    
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--id" && i + 1 < argc) {
            receiver_id = std::stoi(argv[i+1]);
        }
    }

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    natsConnection *conn = NULL;
    natsStatus s = natsConnection_ConnectTo(&conn, "nats://localhost:4222");
    if (s != NATS_OK) {
        std::cerr << "Failed to connect: " << natsStatus_GetText(s) << std::endl;
        return 1;
    }

    std::string subject = "test.subject." + std::to_string(receiver_id);
    natsSubscription *sub = NULL;
    s = natsConnection_Subscribe(&sub, conn, subject.c_str(), onMsg, &receiver_id);
    if (s != NATS_OK) {
        std::cerr << "Failed to subscribe: " << natsStatus_GetText(s) << std::endl;
        natsConnection_Destroy(conn);
        return 1;
    }

    std::cout << " [*] [ASYNC] Receiver " << receiver_id << " subscribed to " << subject << std::endl;

    while (running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << " [x] [ASYNC] Receiver " << receiver_id << " shutting down" << std::endl;

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(conn);
    return 0;
}
