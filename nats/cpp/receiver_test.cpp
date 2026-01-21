#include <nats/nats.h>
#include <iostream>
#include <string>
#include "../../include/json.hpp"

using json = nlohmann::json;

void onMsg(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure) {
    std::string body(natsMsg_GetData(msg), natsMsg_GetDataLength(msg));
    try {
        json data = json::parse(body);
        int message_id = data["message_id"];
        std::cout << " [x] Received message " << message_id << std::endl;

        json response_json = {{"status", "ACK"}, {"message_id", message_id}};
        std::string response = response_json.dump();

        natsConnection_PublishString(nc, natsMsg_GetReply(msg), response.c_str());
    } catch (const std::exception& e) {
        std::cerr << " [!] Error processing message: " << e.what() << std::endl;
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

    natsConnection* conn = nullptr;
    natsStatus s = natsConnection_ConnectTo(&conn, "nats://localhost:4222");
    if (s != NATS_OK) {
        std::cerr << "Failed to connect: " << natsStatus_GetText(s) << std::endl;
        return 1;
    }

    std::string subject = "test.receiver." + std::to_string(id);
    natsSubscription* sub = nullptr;
    s = natsConnection_Subscribe(&sub, conn, subject.c_str(), onMsg, nullptr);
    if (s != NATS_OK) {
        std::cerr << "Failed to subscribe: " << natsStatus_GetText(s) << std::endl;
        natsConnection_Destroy(conn);
        return 1;
    }

    std::cout << " [x] Receiver " << id << " awaiting NATS requests on " << subject << std::endl;

    while (true) {
        nats_Sleep(100);
    }

    natsSubscription_Destroy(sub);
    natsConnection_Destroy(conn);

    return 0;
}
