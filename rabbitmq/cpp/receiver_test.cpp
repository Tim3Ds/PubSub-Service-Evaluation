#include <iostream>
#include <string>
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>
#include "../../include/json.hpp"

using json = nlohmann::json;

int main(int argc, char** argv) {
    int id = 0;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--id" && i + 1 < argc) {
            id = std::stoi(argv[i+1]);
        }
    }

    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    if (!socket) {
        std::cerr << "Error creating TCP socket" << std::endl;
        return 1;
    }

    int status = amqp_socket_open(socket, "localhost", 5672);
    if (status) {
        std::cerr << "Error opening TCP socket" << std::endl;
        return 1;
    }

    amqp_rpc_reply_t login_reply = amqp_login(conn, "/", 0, AMQP_DEFAULT_FRAME_SIZE, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
    if (login_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << "Error logging in" << std::endl;
        return 1;
    }

    amqp_channel_open(conn, 1);
    amqp_get_rpc_reply(conn);

    std::string queue_name = "test_queue_" + std::to_string(id);

    amqp_queue_declare(conn, 1, amqp_cstring_bytes(queue_name.c_str()), 0, 0, 0, 0, amqp_empty_table);
    amqp_get_rpc_reply(conn);

    amqp_basic_consume(conn, 1, amqp_cstring_bytes(queue_name.c_str()), amqp_empty_bytes, 0, 0, 0, amqp_empty_table);
    amqp_get_rpc_reply(conn);

    std::cout << " [x] Receiver " << id << " awaiting RPC requests on " << queue_name << std::endl;

    while (1) {
        amqp_rpc_reply_t res;
        amqp_envelope_t envelope;

        amqp_maybe_release_buffers(conn);

        res = amqp_consume_message(conn, &envelope, NULL, 0);

        if (AMQP_RESPONSE_NORMAL != res.reply_type) {
            break;
        }

        std::string body((char *)envelope.message.body.bytes, envelope.message.body.len);
        try {
            json data = json::parse(body);
            int message_id = data["message_id"];
            std::cout << " [x] Received message " << message_id << std::endl;

            json response_json = {{"status", "ACK"}, {"message_id", message_id}};
            std::string response = response_json.dump();

            amqp_basic_properties_t props;
            props._flags = AMQP_BASIC_CORRELATION_ID_FLAG;
            props.correlation_id = amqp_bytes_malloc_dup(envelope.message.properties.correlation_id);

            amqp_basic_publish(conn, 1, amqp_empty_bytes, envelope.message.properties.reply_to, 0, 0, &props, amqp_cstring_bytes(response.c_str()));
            
            amqp_basic_ack(conn, 1, envelope.delivery_tag, 0);
            
            amqp_bytes_free(props.correlation_id);
        } catch (const std::exception& e) {
            std::cerr << " [!] Error processing message: " << e.what() << std::endl;
        }

        amqp_destroy_envelope(&envelope);
    }

    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);

    return 0;
}
