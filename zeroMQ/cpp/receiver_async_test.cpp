#include <zmq.hpp>
#include <string>
#include <iostream>
#include <fstream>
#include <chrono>
#include <thread>
#include <atomic>
#include <signal.h>
#include "../../include/json.hpp"

using json = nlohmann::json;

std::atomic<bool> running(true);

void signal_handler(int sig) {
    running = false;
}

int main(int argc, char* argv[]) {
    int receiver_id = 0;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--id" && i + 1 < argc) {
            receiver_id = std::stoi(argv[i + 1]);
        }
    }

    int port = 5556 + receiver_id;
    zmq::context_t context(1);
    zmq::socket_t socket(context, ZMQ_REP);
    socket.bind("tcp://*:" + std::to_string(port));

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    std::cout << " [*] [ASYNC] Receiver " << receiver_id << " bound to port " << port << ", awaiting requests" << std::endl;

    int messages_received = 0;

    while (running) {
        // Use polling with very short timeout to remain responsive
        zmq::pollitem_t items[] = { { static_cast<void*>(socket), 0, ZMQ_POLLIN, 0 } };
        zmq::poll(&items[0], 1, 10);  // 10ms poll timeout for responsiveness
        
        if (items[0].revents & ZMQ_POLLIN) {
            zmq::message_t request;
            try {
                auto res = socket.recv(request, zmq::recv_flags::none);
                if (!res.has_value()) {
                    continue;
                }

                std::string request_str(static_cast<char*>(request.data()), request.size());
                json data = json::parse(request_str);
                
                // Handle message_id that could be either string or numeric
                std::string message_id;
                if (data["message_id"].is_string()) {
                    message_id = data["message_id"].get<std::string>();
                } else {
                    message_id = std::to_string(data["message_id"].get<int>());
                }
                
                messages_received++;
                std::cout << " [Receiver " << receiver_id << "] [ASYNC] Received message " << message_id << std::endl;

                json resp;
                resp["status"] = "ACK";
                resp["message_id"] = data["message_id"];  // Keep original type
                resp["receiver_id"] = receiver_id;
                resp["async"] = true;

                std::string resp_str = resp.dump();
                zmq::message_t reply(resp_str.size());
                memcpy(reply.data(), resp_str.c_str(), resp_str.size());
                socket.send(reply, zmq::send_flags::none);
            } catch (const zmq::error_t& e) {
                if (e.num() == ETERM) break;
            } catch (const std::exception& e) {
                std::cerr << " [!] [ASYNC] Error processing message: " << e.what() << std::endl;
            }
        }
    }

    std::cout << " [x] [ASYNC] Receiver " << receiver_id << " shutting down (received " << messages_received << " messages)" << std::endl;
    return 0;
}
