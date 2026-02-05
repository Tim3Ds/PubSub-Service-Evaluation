#include <nats/nats.h>
#include <iostream>
#include <string>
#include "../../utils/cpp/message_helpers.hpp"

using messaging::MessageEnvelope;
using message_helpers::get_current_time_ms;

void onMsg(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure) {
    int *receiver_id = (int*)closure;

    std::string request_str(natsMsg_GetData(msg), natsMsg_GetDataLength(msg));
    
    MessageEnvelope request_envelope;
    if (message_helpers::parse_envelope(request_str, request_envelope)) {
        std::string message_id = request_envelope.message_id();
        std::cout << " [x] Received message " << message_id << std::endl;

        // Create ACK
        MessageEnvelope response = message_helpers::create_ack_from_envelope(
            request_envelope,
            std::to_string(*receiver_id)
        );
        std::string resp_str = message_helpers::serialize_envelope(response);
        
        // Send reply
        if (natsMsg_GetReply(msg)) {
            natsConnection_Publish(nc, natsMsg_GetReply(msg), resp_str.c_str(), (int)resp_str.size());
        }
    }

    natsMsg_Destroy(msg);
}

int main(int argc, char** argv) {
    int id = 0;
    
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--id" && i + 1 < argc) {
            id = std::stoi(argv[i+1]);
        }
    }

    natsConnection *conn = NULL;
    natsStatus s = natsConnection_ConnectTo(&conn, "nats://localhost:4222");
    if (s != NATS_OK) {
        std::cerr << "Failed to connect: " << natsStatus_GetText(s) << std::endl;
        return 1;
    }

    std::string subject = "test.subject." + std::to_string(id);
    natsSubscription *sub = NULL;
    s = natsConnection_Subscribe(&sub, conn, subject.c_str(), onMsg, &id);
    if (s != NATS_OK) {
        std::cerr << "Failed to subscribe: " << natsStatus_GetText(s) << std::endl;
        natsConnection_Destroy(conn);
        return 1;
    }

    std::cout << " [*] Receiver " << id << " awaiting NATS requests on " << subject << std::endl;

    while (true) {
        nats_Sleep(100);
    }

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(conn);
    return 0;
}
