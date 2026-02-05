#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <mutex>
#include <set>
#include <map>
#include <algorithm>

#include <grpcpp/grpcpp.h>
#include "messaging.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using messaging::MessageEnvelope;
using messaging::MessagingService;

struct Subscriber {
    ServerReaderWriter<MessageEnvelope, MessageEnvelope>* stream;
    std::mutex mu;
    bool active;
    
    Subscriber(ServerReaderWriter<MessageEnvelope, MessageEnvelope>* s) : stream(s), active(true) {}
};

class MessagingServiceImpl final : public MessagingService::Service {
    std::mutex global_mu_;
    std::map<std::string, std::set<std::shared_ptr<Subscriber>>> topic_subscribers_;

public:
    Status SubscribeAndPublish(ServerContext* context, ServerReaderWriter<MessageEnvelope, MessageEnvelope>* stream) override {
        // Create a subscriber state for this connection
        auto sub = std::make_shared<Subscriber>(stream);
        std::set<std::string> my_topics;

        MessageEnvelope msg;
        while (stream->Read(&msg)) {
            std::string topic = msg.topic();
            
            // Register subscription if new topic
            {
                std::lock_guard<std::mutex> lock(global_mu_);
                if (my_topics.find(topic) == my_topics.end()) {
                    my_topics.insert(topic);
                    topic_subscribers_[topic].insert(sub);
                    std::cout << "Client subscribed to: " << topic << std::endl;
                }
            }

            // If message has content, broadcast it
            if (msg.payload().size() > 0) {
                 Broadcast(msg, sub);
            }
        }

        // Cleanup on disconnect
        {
            std::lock_guard<std::mutex> lock(global_mu_);
            for (const auto& topic : my_topics) {
                topic_subscribers_[topic].erase(sub);
            }
        }

        // Mark as inactive to prevent further writes
        {
            std::lock_guard<std::mutex> lock(sub->mu);
            sub->active = false;
        }

        return Status::OK;
    }

private:
    void Broadcast(const MessageEnvelope& msg, std::shared_ptr<Subscriber> sender) {
        std::vector<std::shared_ptr<Subscriber>> targets;
        
        {
            std::lock_guard<std::mutex> lock(global_mu_);
            if (topic_subscribers_.count(msg.topic())) {
                for (const auto& s : topic_subscribers_[msg.topic()]) {
                     targets.push_back(s);
                }
            }
        }

        for (auto& s : targets) {
            std::lock_guard<std::mutex> lock(s->mu);
            if (s->active) {
                s->stream->Write(msg);
            }
        }
    }
};

void RunServer() {
    std::string server_address("0.0.0.0:50051");
    MessagingServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.SetMaxConcurrentStreams(1000);
    builder.SetMaxMessageSize(1024 * 1024 * 10); // 10MB
    builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::NUM_CQS, 4);
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    RunServer();
    return 0;
}
