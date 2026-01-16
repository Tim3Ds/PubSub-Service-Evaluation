#include <iostream>
#include <string>
#include <grpcpp/grpcpp.h>
#include "test_data.grpc.pb.h"
#include <getopt.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using test_data::TestDataItem;
using test_data::Ack;
using test_data::TestDataService;

class TestDataServiceImpl final : public TestDataService::Service {
public:
    TestDataServiceImpl(int id) : receiver_id_(id), messages_received_(0) {}

    Status TransferData(ServerContext* context, const TestDataItem* request, Ack* reply) override {
        messages_received_++;
        std::cout << " [Receiver " << receiver_id_ << "] Received message " << request->message_id() << std::endl;
        
        reply->set_message_id(request->message_id());
        reply->set_status("ACK");
        reply->set_receiver_id(receiver_id_);
        return Status::OK;
    }
    
    int get_messages_received() const { return messages_received_; }
    
private:
    int receiver_id_;
    int messages_received_;
};

void RunServer(int id) {
    int port = 50051 + id;
    std::string server_address("0.0.0.0:" + std::to_string(port));
    TestDataServiceImpl service(id);

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << " [x] Receiver " << id << " listening on " << server_address << std::endl;
    server->Wait();
    std::cout << " [x] Receiver " << id << " shutting down (received " << service.get_messages_received() << " messages)" << std::endl;
}

int main(int argc, char** argv) {
    int receiver_id = 0;
    
    int opt;
    while ((opt = getopt(argc, argv, "i:-:")) != -1) {
        switch (opt) {
            case 'i':
                receiver_id = std::atoi(optarg);
                break;
            case '-':
                if (std::string(optarg) == "id") {
                     // Handle --id if passed as such, though getopt handles single char usually
                     // getopt_long would be better but simple logic:
                     // Skip for now, assume -i or handle args manually if needed
                }
                break;
        }
    }
    
    // Manual parsing for --id if getopt doesn't catch it
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--id" && i + 1 < argc) {
            receiver_id = std::atoi(argv[i+1]);
        }
    }

    RunServer(receiver_id);
    return 0;
}
