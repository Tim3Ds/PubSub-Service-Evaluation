#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>
#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <vector>
#include <future>
#include "../../include/json.hpp"
#include "../../include/stats_collector.hpp"

using json = nlohmann::json;

struct TaskResult {
    bool success;
    long long duration;
    std::string message_id;
    std::string error;
};

TaskResult send_message_task(json item) {
    TaskResult res;
    res.message_id = item["message_id"];
    res.success = false;
    res.duration = 0;

    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    if (amqp_socket_open(socket, "localhost", 5672)) {
        res.error = "Could not open socket";
        amqp_destroy_connection(conn);
        return res;
    }
    
    amqp_login(conn, "/", 0, AMQP_DEFAULT_FRAME_SIZE, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
    amqp_channel_open(conn, 1);
    
    amqp_queue_declare_ok_t *ok = amqp_queue_declare(conn, 1, amqp_empty_bytes, 0, 0, 0, 1, amqp_empty_table);
    amqp_bytes_t reply_queue = amqp_bytes_malloc_dup(ok->queue);
    amqp_basic_consume(conn, 1, reply_queue, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);

    long long msg_start = get_current_time_ms();
    std::string body = item.dump();
    std::string corr_id = "cpp-async-" + res.message_id;

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
    struct timeval timeout;
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;
    
    amqp_rpc_reply_t r = amqp_consume_message(conn, &envelope, &timeout, 0);

    if (r.reply_type == AMQP_RESPONSE_NORMAL) {
        try {
            std::string reply_str((char *)envelope.message.body.bytes, envelope.message.body.len);
            json resp_data = json::parse(reply_str);
            if (resp_data["status"] == "ACK" && resp_data["message_id"] == res.message_id) {
                res.duration = get_current_time_ms() - msg_start;
                res.success = true;
            } else {
                res.error = "Unexpected response";
            }
        } catch (...) {
            res.error = "Parse error";
        }
        amqp_destroy_envelope(&envelope);
    } else {
        res.error = "Timeout/Error";
    }

    amqp_bytes_free(reply_queue);
    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);

    return res;
}

int main() {
    std::ifstream f("../../test_data.json");
    if (!f.is_open()) f.open("test_data.json");
    if (!f.is_open()) f.open("/home/tim/repos/test_data.json");
    
    if (!f.is_open()) {
        std::cerr << " [!] Could not find test_data.json" << std::endl;
        return 1;
    }
    
    json test_data = json::parse(f);

    std::cout << " [x] Starting RabbitMQ ASYNC Sender (C++)" << std::endl;
    std::cout << " [x] Starting async transfer of " << test_data.size() << " messages..." << std::endl;

    MessageStats stats;
    long long global_start = get_current_time_ms();

    std::vector<std::future<TaskResult>> futures;
    for (auto& item : test_data) {
        futures.push_back(std::async(std::launch::async, send_message_task, item));
    }

    for (auto& fut : futures) {
        TaskResult res = fut.get();
        if (res.success) {
            stats.record_message(true, res.duration);
            std::cout << " [OK] Message " << res.message_id << " acknowledged" << std::endl;
        } else {
            stats.record_message(false);
            std::cout << " [FAILED] Message " << res.message_id << ": " << res.error << std::endl;
        }
    }

    long long global_end = get_current_time_ms();
    stats.set_duration(global_start, global_end);
    
    json report = stats.get_stats();
    report["service"] = "RabbitMQ";
    report["language"] = "C++";
    report["async"] = true;

    std::cout << "\nTest Results (ASYNC):" << std::endl;
    std::cout << "total_sent: " << stats.sent_count << std::endl;
    std::cout << "total_received: " << stats.received_count << std::endl;
    std::cout << "duration_ms: " << stats.get_duration_ms() << std::endl;

    std::ofstream rf("report.txt", std::ios::app);
    if (rf.good()) {
        rf << report.dump() << std::endl;
        rf.close();
    }

    return 0;
}
