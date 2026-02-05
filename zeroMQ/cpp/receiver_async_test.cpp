#include <zmq.hpp>
#include <iostream>
#include <string>
#include <signal.h>
#include <atomic>
#include <thread>
#include "../../utils/cpp/message_helpers.hpp"

using messaging::MessageEnvelope;
using message_helpers::get_current_time_ms;

std::atomic<bool> running(true);

void signal_handler(int sig) {
    running = false;
}

int main(int argc, char* argv[]) {
    int receiver_id = 0;
    
    for (int i = 1; i < argc; i++) {
        if (std::string(argv[i]) == "--id" && i + 1 < argc) {
            receiver_id = std::stoi(argv[i + 1]);
            break;
        }
    }
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    zmq::context_t context(1);
    zmq::socket_t socket(context, ZMQ_REP);
    
    int port = 5556 + receiver_id;
    socket.bind("tcp://*:" + std::to_string(port));
    socket.setsockopt(ZMQ_RCVTIMEO, 1000);  // 1s timeout for graceful shutdown

    std::cout << " [*] [ASYNC] Receiver " << receiver_id << " listening on port " << port << std::endl;

    while (running) {
        zmq::message_t request;
        auto recv_res = socket.recv(request);
        
        if (recv_res.has_value()) {
            std::string request_str(static_cast<char*>(request.data()), request.size());
            
            // Parse message
            MessageEnvelope msg_envelope;
            if (message_helpers::parse_envelope(request_str, msg_envelope)) {
                std::string message_id = msg_envelope.message_id();
                std::cout << " [x] [ASYNC] Received message " << message_id << std::endl;
                
                // Create ACK
                MessageEnvelope response = message_helpers::create_ack_from_envelope(
                    msg_envelope,
                    std::to_string(receiver_id)
                );
                response.set_async(true);
                std::string response_str = message_helpers::serialize_envelope(response);
                
                // Send ACK
                zmq::message_t reply(response_str.size());
                memcpy(reply.data(), response_str.c_str(), response_str.size());
                socket.send(reply, zmq::send_flags::none);
            }
        }
    }

    std::cout << " [x] [ASYNC] Receiver " << receiver_id << " shutting down" << std::endl;
    socket.close();
    return 0;
}
