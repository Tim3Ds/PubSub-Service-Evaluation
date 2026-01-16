// Simple NATS C++ Publisher Example
// Requires: https://github.com/nats-io/nats.c
#include <nats/nats.h>
#include <iostream>

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cout << "Usage: " << argv[0] << " <server> <subject> <message>" << std::endl;
        return 1;
    }
    const char* server = argv[1];
    const char* subject = argv[2];
    const char* message = argv[3];

    natsConnection* conn = nullptr;
    natsStatus s = natsConnection_ConnectTo(&conn, server);
    if (s != NATS_OK) {
        std::cerr << "Failed to connect: " << natsStatus_GetText(s) << std::endl;
        return 1;
    }
    s = natsConnection_PublishString(conn, subject, message);
    if (s != NATS_OK) {
        std::cerr << "Failed to publish: " << natsStatus_GetText(s) << std::endl;
        natsConnection_Destroy(conn);
        return 1;
    }
    std::cout << "Published to " << subject << ": " << message << std::endl;
    natsConnection_Destroy(conn);
    return 0;
}
