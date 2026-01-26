#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>
#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <vector>
#include "../../include/json.hpp"
#include "../../include/stats_collector.hpp"

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

    std::string data_path = "test_data.json";
    std::ifstream f(data_path);
    if (!f.good()) {
        data_path = "../../test_data.json";
        f.close();
        f.open(data_path);
    }
    if (!f.good()) {
        data_path = "/home/tim/repos/test_data.json";
        f.close();
        f.open(data_path);
    }
    
    if (!f.good()) {
        std::cerr << " [!] Could not find test_data.json" << std::endl;
        return 1;
    }
    
    json test_data = json::parse(f);

    MessageStats stats;
    long long start_time = get_current_time_ms();
    stats.set_duration(start_time, 0);

    std::cout << " [x] Starting transfer of " << test_data.size() << " messages..." << std::endl;

    for (auto& item : test_data) {
        std::string message_id = item["message_id"].is_string() ? item["message_id"].get<std::string>() : std::to_string(item["message_id"].get<long long>());
        std::cout << " [x] Sending message " << message_id << " (" << item["message_name"] << ")..." << std::flush;
        
        long long msg_start = get_current_time_ms();
        std::string body = item.dump();
        std::string corr_id = "corr-" + std::to_string(stats.sent_count + 1);

        amqp_basic_properties_t props;
        props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG | AMQP_BASIC_REPLY_TO_FLAG | AMQP_BASIC_CORRELATION_ID_FLAG;
        props.content_type = amqp_cstring_bytes("application/json");
        props.delivery_mode = 2;
        props.reply_to = reply_queue;
        props.correlation_id = amqp_cstring_bytes(corr_id.c_str());

        int target = item.value("target", 0);
        std::string queue_name = "test_queue_" + std::to_string(target);

        amqp_basic_publish(conn, 1, amqp_empty_bytes, amqp_cstring_bytes(queue_name.c_str()), 0, 0, &props, amqp_cstring_bytes(body.c_str()));

        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(conn);
        struct timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 40000;
        amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, &timeout, 0);

        if (res.reply_type == AMQP_RESPONSE_NORMAL) {
            try {
                std::string reply_str((char *)envelope.message.body.bytes, envelope.message.body.len);
                json resp_data = json::parse(reply_str);
                
                if (resp_data["status"] == "ACK" && resp_data["message_id"] == item["message_id"]) {
                    long long msg_duration = get_current_time_ms() - msg_start;
                    stats.record_message(true, msg_duration);
                    std::cout << " [OK]" << std::endl;
                } else {
                    stats.record_message(false);
                    std::cout << " [FAILED] Unexpected response" << std::endl;
                }
            } catch (...) {
                stats.record_message(false);
                std::cout << " [FAILED] Parse error" << std::endl;
            }
            amqp_destroy_envelope(&envelope);
        } else {
            stats.record_message(false);
            std::cout << " [FAILED] Error consuming" << std::endl;
        }
    }

    long long end_time = get_current_time_ms();
    stats.set_duration(start_time, end_time);
    
    json report = stats.get_stats();
    report["service"] = "RabbitMQ";
    report["language"] = "C++";

    std::cout << "\nTest Results:" << std::endl;
    std::cout << "service: RabbitMQ" << std::endl;
    std::cout << "language: C++" << std::endl;
    std::cout << "total_sent: " << stats.sent_count << std::endl;
    std::cout << "total_received: " << stats.received_count << std::endl;
    std::cout << "duration_ms: " << stats.get_duration_ms() << std::endl;
    if (report.contains("message_timing_stats")) {
        std::cout << "message_timing_stats: " << report["message_timing_stats"].dump() << std::endl;
    }

    std::ofstream rf("report.txt", std::ios::app);
    if (rf.good()) {
        rf << report.dump() << std::endl;
        rf.close();
    } else {
        std::cerr << " [!] Warning: Could not write to report file" << std::endl;
    }

    amqp_bytes_free(reply_queue);
    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);

    return 0;
}
