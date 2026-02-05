#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>
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

    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    amqp_socket_open(socket, "localhost", 5672);
    amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
    amqp_channel_open(conn, 1);

    std::string queue_name = "test_queue_" + std::to_string(receiver_id);
    amqp_queue_declare(conn, 1, amqp_cstring_bytes(queue_name.c_str()), 0, 0, 0, 0, amqp_empty_table);
    amqp_basic_consume(conn, 1, amqp_cstring_bytes(queue_name.c_str()), amqp_empty_bytes, 0, 1, 0, amqp_empty_table);

    std::cout << " [*] [ASYNC] Receiver " << receiver_id << " waiting for messages on " << queue_name << std::endl;

    while (running) {
        struct timeval timeout = {1, 0};
        amqp_envelope_t envelope;
        amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, &timeout, 0);

        if (res.reply_type == AMQP_RESPONSE_NORMAL) {
            std::string message_str((char*)envelope.message.body.bytes, envelope.message.body.len);
            
            MessageEnvelope msg_envelope;
            if (message_helpers::parse_envelope(message_str, msg_envelope)) {
                std::string message_id = msg_envelope.message_id();
                std::cout << " [x] [ASYNC] Received message " << message_id << std::endl;

                // Create ACK
                MessageEnvelope response = message_helpers::create_ack_from_envelope(
                    msg_envelope,
                    std::to_string(receiver_id)
                );
                response.set_async(true);
                std::string response_str = message_helpers::serialize_envelope(response);

                // Send ACK with proper binary handling
                amqp_basic_properties_t props;
                props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_CORRELATION_ID_FLAG;
                props.content_type = amqp_cstring_bytes("application/octet-stream");
                props.correlation_id = envelope.message.properties.correlation_id;

                // Use amqp_bytes_t to properly handle binary data (not null-terminated)
                amqp_bytes_t body;
                body.len = response_str.size();
                body.bytes = (void*)response_str.data();

                amqp_basic_publish(conn, 1, amqp_empty_bytes, envelope.message.properties.reply_to,
                                  0, 0, &props, body);
            }
            amqp_destroy_envelope(&envelope);
        }
    }

    std::cout << " [x] [ASYNC] Receiver " << receiver_id << " shutting down" << std::endl;
    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
    return 0;
}
