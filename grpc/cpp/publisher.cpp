#include <grpcpp/grpcpp.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include "pubsub.grpc.pb.h"
#include <iostream>
#include <thread>
#include <string>
#include <vector>

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using pubsub::PubSubService;
using pubsub::Message;
using pubsub::Value;

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <topic> <message>" << std::endl;
        return 1;
    }
    std::string topic = argv[1];
    std::string msg_str = argv[2];

    auto channel = grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials());
    std::unique_ptr<PubSubService::Stub> stub = PubSubService::NewStub(channel);

    ClientContext context;
    std::shared_ptr<ClientReaderWriter<Message, Message>> stream(stub->SubscribeAndPublish(&context));

    // Thread to handle incoming messages (optional for publisher)
    std::thread reader_thread([stream]() {
        Message response;
        while (stream->Read(&response)) {
            std::cout << "Received on " << response.topic() << ": ";
            for (const auto& value : response.values()) {
                if (value.has_string_value()) {
                    std::cout << value.string_value() << " ";
                } else if (value.has_int_value()) {
                    std::cout << value.int_value() << " ";
                } else if (value.has_double_value()) {
                    std::cout << value.double_value() << " ";
                }
            }
            std::cout << std::endl;
        }
    });

    // Send message
    Message msg;
    msg.set_topic(topic);
    Value* val = msg.add_values();
    val->set_string_value(msg_str);
    stream->Write(msg);

    // Keep stream open for a bit
    std::this_thread::sleep_for(std::chrono::seconds(1));

    stream->WritesDone();
    Status status = stream->Finish();
    if (!status.ok()) {
        std::cerr << "RPC failed: " << status.error_message() << std::endl;
        return 1;
    }

    reader_thread.join();
    return 0;
}
