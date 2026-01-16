#include <zmq.hpp>
#include <string>
#include <iostream>
#include <thread>
#include <chrono>

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <bind-endpoint> <topic> <message>\n";
        std::cerr << "Example: ./publisher tcp://*:5555 test \"hello\"\n";
        return 1;
    }
    std::string endpoint = argv[1];
    std::string topic = argv[2];
    std::string message = argv[3];

    zmq::context_t ctx{1};
    zmq::socket_t pub(ctx, ZMQ_PUB);
    pub.bind(endpoint);

    // allow subscribers to connect
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::string payload = topic + " " + message;
    zmq::message_t msg(payload.data(), payload.size());
    try {
        pub.send(msg);
    } catch (const zmq::error_t& e) {
        std::cerr << "Failed to send message: " << e.what() << "\n";
        return 1;
    }
    std::cout << "Published on topic '" << topic << "': " << message << "\n";
    return 0;
}
