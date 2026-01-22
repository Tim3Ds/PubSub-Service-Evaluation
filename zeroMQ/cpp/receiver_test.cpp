#include <zmq.hpp>
#include <iostream>
#include <string>
#include <csignal>
#include <atomic>
#include "../../include/json.hpp"

using json = nlohmann::json;

static std::atomic<bool> should_exit(false);
static int receiver_id_global = 0;
static int messages_received = 0;

void signal_handler(int sig) {
    std::cout << " [x] Receiver " << receiver_id_global << " shutting down (received " << messages_received << " messages)" << std::endl;
    should_exit = true;
}

int main(int argc, char** argv) {
    int id = 0;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--id" && i + 1 < argc) {
            id = std::stoi(argv[i+1]);
        }
    }
    
    receiver_id_global = id;
    
    signal(SIGTERM, signal_handler);
    signal(SIGINT, signal_handler);

    int port = 5556 + id;
    zmq::context_t ctx{1};
    zmq::socket_t socket(ctx, ZMQ_REP);
    socket.bind("tcp://*:" + std::to_string(port));

    std::cout << " [*] Receiver " << id << " awaiting ZeroMQ requests on port " << port << std::endl;

    while (!should_exit.load()) {
        try {
            // Dealer receives: [Data] (Identity stripped by Router) or [Empty?, Data]?
            // Router -> Dealer: Router sends [Identity, Data]. Dealer receives [Data] (if Identity matches).
            // Actually, Router sends [Identity, Data] on the wire. Dealer consumes Identity? No.
            // Dealer receives just the message payload if it's addressed to it?
            // Let's assume standard behavior: Dealer just gets what falls out.
            // Sender: socket.send_multipart([receiver_id, item_json]) -> Router.
            // Router routes to Identity.
            // Message arriving at Dealer: [item_json].
            // But wait, Sender sent [id, item].
            // Router STRIPS the identity frame when sending to the Dealer.
            // So Dealer receives [item].
            
            zmq::pollitem_t items[] = { { static_cast<void*>(socket), 0, ZMQ_POLLIN, 0 } };
            zmq::poll(&items[0], 1, 100);
            
            if (items[0].revents & ZMQ_POLLIN) {
                zmq::message_t request;
                auto result = socket.recv(request, zmq::recv_flags::none);

                if (result.has_value()) {
                    std::string body(static_cast<const char*>(request.data()), request.size());
                    
                    // std::cout << "debug: " << body << std::endl;

                    json data = json::parse(body);
                    int message_id = data["message_id"];
                    messages_received++;
                    std::cout << " [Receiver " << id << "] Received message " << message_id << std::endl;

                    json response_json = {
                        {"status", "ACK"}, 
                        {"message_id", message_id},
                        {"receiver_id", id}
                    };
                    std::string response = response_json.dump();

                    zmq::message_t reply(response.data(), response.size());
                    socket.send(reply, zmq::send_flags::none);
                }
            }
        } catch (const std::exception& e) {
            if (!should_exit.load()) {
                std::cerr << " [!] Error processing message: " << e.what() << std::endl;
                // Send error
                std::string error_resp = "{\"status\":\"ERROR\"}";
                zmq::message_t reply(error_resp.data(), error_resp.size());
                socket.send(reply, zmq::send_flags::none);
            }
        }
    }

    return 0;
}
