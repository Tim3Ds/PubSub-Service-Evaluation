#include <iostream>
#include <memory>
#include <string>
#include <signal.h>
#include <grpcpp/grpcpp.h>
#include "test_data.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using test_data::TestDataItem;
using test_data::Ack;
using test_data::TestDataService;

class TestDataServiceImpl final : public TestDataService::Service {
public:
    TestDataServiceImpl(int receiver_id) : receiver_id_(receiver_id), messages_received_(0) {}

    Status TransferData(ServerContext* context, const TestDataItem* request,
                        Ack* reply) override {
        messages_received_++;
        std::cout << " [Receiver " << receiver_id_ << "] [ASYNC] Received message " << request->message_id() << std::endl;
        reply->set_status("ACK");
        reply->set_message_id(request->message_id());
        reply->set_receiver_id(receiver_id_);
        return Status::OK;
    }

    int get_messages_received() const { return messages_received_; }

private:
    int receiver_id_;
    int messages_received_;
};

std::unique_ptr<Server> server_ptr;

void signal_handler(int sig) {
    if (server_ptr) {
        server_ptr->Shutdown();
    }
}

void RunServer(int receiver_id) {
    int port = 50051 + receiver_id;
    std::string server_address("0.0.0.0:" + std::to_string(port));
    TestDataServiceImpl service(receiver_id);

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    server_ptr = builder.BuildAndStart();
    std::cout << " [*] [ASYNC] Receiver " << receiver_id << " listening on " << server_address << std::endl;
    server_ptr->Wait();
    std::cout << " [x] [ASYNC] Receiver " << receiver_id << " shutting down (received " << service.get_messages_received() << " messages)" << std::endl;
}

int main(int argc, char** argv) {
    int receiver_id = 0;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--id" && i + 1 < argc) {
            receiver_id = std::stoi(argv[i + 1]);
        }
    }

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    RunServer(receiver_id);
    return 0;
}
