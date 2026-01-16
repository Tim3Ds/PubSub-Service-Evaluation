#include <iostream>
#include <string>
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

int main(int argc, char const *const *argv) {
    std::string hostname = "localhost";
    int port = 5672;
    std::string exchange = "test_exchange";
    std::string routing_key = "test_key";

    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    if (!socket) {
        std::cerr << "Error creating TCP socket" << std::endl;
        return 1;
    }

    int status = amqp_socket_open(socket, hostname.c_str(), port);
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

    amqp_exchange_declare(conn, 1, amqp_cstring_bytes(exchange.c_str()), amqp_cstring_bytes("fanout"),
                          0, 1, 0, 0, amqp_empty_table);
    amqp_get_rpc_reply(conn);

    amqp_queue_declare_ok_t *r = amqp_queue_declare(conn, 1, amqp_empty_bytes, 0, 0, 0, 1, amqp_empty_table);
    amqp_get_rpc_reply(conn);
    amqp_bytes_t queuename = amqp_bytes_malloc_dup(r->queue);

    amqp_queue_bind(conn, 1, queuename, amqp_cstring_bytes(exchange.c_str()), amqp_cstring_bytes(routing_key.c_str()), amqp_empty_table);
    amqp_get_rpc_reply(conn);

    amqp_basic_consume(conn, 1, queuename, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
    amqp_get_rpc_reply(conn);

    std::cout << " [*] Waiting for messages. To exit press CTRL+C" << std::endl;

    while (1) {
        amqp_rpc_reply_t res;
        amqp_envelope_t envelope;

        amqp_maybe_release_buffers(conn);

        res = amqp_consume_message(conn, &envelope, NULL, 0);

        if (AMQP_RESPONSE_NORMAL != res.reply_type) {
            break;
        }

        std::cout << " [x] Received " << (char *)envelope.message.body.bytes << std::endl;

        amqp_destroy_envelope(&envelope);
    }

    amqp_bytes_free(queuename);
    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);

    return 0;
}
