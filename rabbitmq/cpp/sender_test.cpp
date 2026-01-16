#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>
#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <vector>
#include "../../include/json.hpp"

using json = nlohmann::json;

int main() {
    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    amqp_socket_open(socket, "localhost", 5672);
    amqp_login(conn, "/", 0, AMQP_DEFAULT_FRAME_SIZE, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
    amqp_channel_open(conn, 1);
    
    amqp_queue_declare_ok_t *r = amqp_queue_declare(conn, 1, amqp_empty_bytes, 0, 0, 0, 1, amqp_empty_table);
    amqp_bytes_t reply_queue = amqp_bytes_malloc_dup(r->queue);
    amqp_basic_consume(conn, 1, reply_queue, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);

    std::ifstream f("../../test_data.json");
    json test_data = json::parse(f);

    struct {
        int sent = 0;
        int received = 0;
        int processed = 0;
        int failed = 0;
        long long start_time;
        long long end_time;
    } stats;

    stats.start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    std::cout << " [x] Starting transfer of " << test_data.size() << " messages..." << std::endl;

    for (auto& item : test_data) {
        stats.sent++;
        std::cout << " [x] Sending message " << item["message_id"] << " (" << item["message_name"] << ")..." << std::flush;
        
        std::string body = item.dump();
        std::string corr_id = "corr-" + std::to_string(stats.sent);

        amqp_basic_properties_t props;
        props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG | AMQP_BASIC_REPLY_TO_FLAG | AMQP_BASIC_CORRELATION_ID_FLAG;
        props.content_type = amqp_cstring_bytes("application/json");
        props.delivery_mode = 2;
        props.reply_to = reply_queue;
        props.correlation_id = amqp_cstring_bytes(corr_id.c_str());

        amqp_basic_publish(conn, 1, amqp_empty_bytes, amqp_cstring_bytes("test_queue"), 0, 0, &props, amqp_cstring_bytes(body.c_str()));

        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(conn);
        amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, NULL, 0);

        if (res.reply_type == AMQP_RESPONSE_NORMAL) {
            try {
                std::string reply_str((char *)envelope.message.body.bytes, envelope.message.body.len);
                json resp_data = json::parse(reply_str);
                
                if (resp_data["status"] == "ACK" && resp_data["message_id"] == item["message_id"]) {
                    stats.received++;
                    stats.processed++;
                    std::cout << " [OK]" << std::endl;
                } else {
                    stats.failed++;
                    std::cout << " [FAILED] Unexpected response" << std::endl;
                }
            } catch (...) {
                stats.failed++;
                std::cout << " [FAILED] Parse error" << std::endl;
            }
            amqp_destroy_envelope(&envelope);
        } else {
            stats.failed++;
            std::cout << " [FAILED] Error consuming" << std::endl;
        }
    }

    stats.end_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    double duration = stats.end_time - stats.start_time;
    
    json report;
    report["service"] = "RabbitMQ";
    report["language"] = "C++";
    report["total_sent"] = stats.sent;
    report["total_received"] = stats.received;
    report["total_processed"] = stats.processed;
    report["total_failed"] = stats.failed;
    report["duration_ms"] = duration;
    report["processed_per_ms"] = duration > 0 ? stats.processed / duration : 0;
    report["failed_per_ms"] = duration > 0 ? stats.failed / duration : 0;

    std::cout << "\nTest Results:" << std::endl;
    std::cout << report.dump(4) << std::endl;

    std::ofstream rf("../../report.txt", std::ios::app);
    rf << report.dump() << std::endl;

    amqp_bytes_free(reply_queue);
    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);

    return 0;
}
