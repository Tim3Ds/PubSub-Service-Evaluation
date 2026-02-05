#include <iostream>
#include <string>
#include <memory>
#include <signal.h>
#include <atomic>
#include <thread>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include "messaging.grpc.pb.h"
#include "../../utils/cpp/message_helpers.hpp"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using messaging::MessageEnvelope;
using messaging::MessagingService;
using message_helpers::get_current_time_ms;

std::atomic<bool> running(true);

void signal_handler(int sig) {
    running = false;
}

class MessagingServiceImpl final : public MessagingService::Service {
private:
    int receiver_id;
    
public:
    MessagingServiceImpl(int id) : receiver_id(id) {}
    
    Status SendMessage(ServerContext* context, const MessageEnvelope* request, MessageEnvelope* reply) override {
        std::string message_id = request->message_id();
        std::cout << " [x] Received message " << message_id << std::endl;
        
        // Create ACK using helper
        *reply = message_helpers::create_ack_from_envelope(*request, std::to_string(receiver_id));
        
        return Status::OK;
    }
};

int main(int argc, char** argv) {
    int receiver_id = 0;
    
    for (int i = 1; i < argc; i++) {
        if (std::string(argv[i]) == "--id" && i + 1 < argc) {
            receiver_id = std::stoi(argv[i + 1]);
            break;
        }
    }
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    int port = 50051 + receiver_id;
    std::string server_address = "0.0.0.0:" + std::to_string(port);
    
    MessagingServiceImpl service(receiver_id);
    
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    
    std::cout << " [*] Receiver " << receiver_id << " listening on port " << port << std::endl;
    
    while (running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    std::cout << " [x] Receiver " << receiver_id << " shutting down" << std::endl;
    server->Shutdown();
    
    return 0;
}
