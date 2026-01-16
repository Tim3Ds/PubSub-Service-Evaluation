#include <iostream>
#include <string>
#include <amqp.h>
#include <amqp_tcp_socket.h>

int main(int argc, char const *const *argv) {
    std::string hostname = "localhost";
    int port = 5672;
    std::string exchange = "test_exchange";
    std::string routing_key = "test_key";
    std::string messagebody = "Hello from RabbitMQ C++ Publisher!";

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

    amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes("text/plain");
    props.delivery_mode = 2; // persistent

    status = amqp_basic_publish(conn, 1, amqp_cstring_bytes(exchange.c_str()),
                                amqp_cstring_bytes(routing_key.c_str()), 0, 0,
                                &props, amqp_cstring_bytes(messagebody.c_str()));

    if (status < 0) {
        std::cerr << "Error publishing message" << std::endl;
        return 1;
    }

    std::cout << " [x] Sent '" << messagebody << "'" << std::endl;

    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);

    return 0;
}
