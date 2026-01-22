#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>
#include <iostream>
#include <string>
#include <signal.h>
#include <thread>
#include <chrono>
#include <atomic>
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

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    if (amqp_socket_open(socket, "localhost", 5672)) return 1;

    amqp_login(conn, "/", 0, AMQP_DEFAULT_FRAME_SIZE, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
    amqp_channel_open(conn, 1);

    std::string queue_name = "test_queue_" + std::to_string(receiver_id);
    amqp_queue_declare(conn, 1, amqp_cstring_bytes(queue_name.c_str()), 0, 0, 0, 0, amqp_empty_table);
    amqp_basic_consume(conn, 1, amqp_cstring_bytes(queue_name.c_str()), amqp_empty_bytes, 0, 1, 0, amqp_empty_table);

    int messages_received = 0;
    std::cout << " [*] [ASYNC] Receiver " << receiver_id << " awaiting messages on " << queue_name << std::endl;

    while (running) {
        amqp_rpc_reply_t res;
        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(conn);

        struct timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 100000; // 100ms

        res = amqp_consume_message(conn, &envelope, &timeout, 0);

        if (res.reply_type == AMQP_RESPONSE_NORMAL) {
            messages_received++;
            std::string body((char *)envelope.message.body.bytes, envelope.message.body.len);
            try {
                json data = json::parse(body);
                std::string message_id = data.value("message_id", "unknown");

                std::cout << " [Receiver " << receiver_id << "] [ASYNC] Received message " << message_id << std::endl;

                json resp;
                resp["status"] = "ACK";
                resp["message_id"] = message_id;
                resp["receiver_id"] = receiver_id;
                resp["async"] = true;

                amqp_basic_properties_t props;
                props._flags = AMQP_BASIC_CORRELATION_ID_FLAG;
                props.correlation_id = envelope.message.properties.correlation_id;

                amqp_basic_publish(conn, 1, amqp_empty_bytes, envelope.message.properties.reply_to, 0, 0, &props, amqp_cstring_bytes(resp.dump().c_str()));
            } catch (const std::exception& e) {
                std::cerr << " [!] Error: " << e.what() << std::endl;
            }
            amqp_destroy_envelope(&envelope);
        }
    }

    std::cout << " [x] [ASYNC] Receiver " << receiver_id << " shutting down (received " << messages_received << " messages)" << std::endl;

    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
    return 0;
}
