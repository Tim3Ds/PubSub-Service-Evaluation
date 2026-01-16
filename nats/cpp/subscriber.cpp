// Simple NATS C++ Subscriber Example
// Requires: https://github.com/nats-io/nats.c
#include <nats/nats.h>
#include <iostream>
#include <csignal>

volatile std::sig_atomic_t stop = 0;

void signal_handler(int) {
    stop = 1;
}

void onMsg(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure) {
    std::cout << "Received on [" << natsMsg_GetSubject(msg) << "]: " << natsMsg_GetData(msg) << std::endl;
    natsMsg_Destroy(msg);
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " <server> <subject>" << std::endl;
        return 1;
    }
    const char* server = argv[1];
    const char* subject = argv[2];

    std::signal(SIGINT, signal_handler);

    natsConnection* conn = nullptr;
    natsStatus s = natsConnection_ConnectTo(&conn, server);
    if (s != NATS_OK) {
        std::cerr << "Failed to connect: " << natsStatus_GetText(s) << std::endl;
        return 1;
    }
    natsSubscription* sub = nullptr;
    s = natsConnection_Subscribe(&sub, conn, subject, onMsg, nullptr);
    if (s != NATS_OK) {
        std::cerr << "Failed to subscribe: " << natsStatus_GetText(s) << std::endl;
        natsConnection_Destroy(conn);
        return 1;
    }
    std::cout << "Subscribed to " << subject << ". Press Ctrl+C to exit." << std::endl;
    while (!stop) {
        nats_Sleep(100);
    }
    natsSubscription_Destroy(sub);
    natsConnection_Destroy(conn);
    return 0;
}
