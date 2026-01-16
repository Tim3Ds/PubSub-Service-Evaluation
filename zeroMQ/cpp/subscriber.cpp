#include <zmq.hpp>
#include <string>
#include <iostream>

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <connect-endpoint> <topic>\n";
        std::cerr << "Example: ./subscriber tcp://localhost:5555 test\n";
        return 1;
    }
    std::string endpoint = argv[1];
    std::string topic = argv[2];

    zmq::context_t ctx{1};
    zmq::socket_t sub(ctx, ZMQ_SUB);
    sub.connect(endpoint);
    sub.setsockopt(ZMQ_SUBSCRIBE, topic.data(), topic.size());

    while (true) {
        zmq::message_t msg;
        auto result = sub.recv(msg); // Using structured bindings
        if (result.has_value()) {  // Check if receive was successful
            std::string data(static_cast<const char*>(msg.data()), msg.size());
            std::cout << "Received: " << data << std::endl;
        }
    }
    return 0;
}
