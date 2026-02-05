#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>
#include <iostream>
#include <fstream>
#include <string>
#include <future>
#include <vector>
#include "../../utils/cpp/stats_collector.hpp"
#include "../../utils/cpp/test_data_loader.hpp"
#include "../../utils/cpp/message_helpers.hpp"

using json = nlohmann::json;
using messaging::MessageEnvelope;
using message_helpers::get_current_time_ms;

struct TaskResult {
    bool success;
    std::string message_id;
    long long duration;
    std::string error;
};

TaskResult send_message_task(const json& item) {
    TaskResult res;
    res.success = false;
    res.message_id = message_helpers::extract_message_id(item);
    res.duration = 0;

    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    
    if (amqp_socket_open(socket, "localhost", 5672) != 0) {
        res.error = "Connection failed";
        amqp_destroy_connection(conn);
        return res;
    }

    amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
    amqp_channel_open(conn, 1);
    
    // Subscribe to direct reply queue
    amqp_basic_consume(conn, 1, amqp_cstring_bytes("amq.rabbitmq.reply-to"), amqp_empty_bytes, 0, 1, 0, amqp_empty_table);

    int target = item.value("target", 0);
    std::string queue_name = "test_queue_" + std::to_string(target);
    std::string reply_queue = "amq.rabbitmq.reply-to";

    long long msg_start = get_current_time_ms();

    // Create and send message
    MessageEnvelope envelope = message_helpers::create_data_envelope(item);
    std::string body = message_helpers::serialize_envelope(envelope);

    amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_REPLY_TO_FLAG | AMQP_BASIC_CORRELATION_ID_FLAG;
    props.content_type = amqp_cstring_bytes("application/octet-stream");
    props.reply_to = amqp_cstring_bytes(reply_queue.c_str());
    props.correlation_id = amqp_cstring_bytes(res.message_id.c_str());

    amqp_basic_publish(conn, 1, amqp_empty_bytes, amqp_cstring_bytes(queue_name.c_str()),
                      0, 0, &props, amqp_cstring_bytes(body.c_str()));

    // Wait for reply
    struct timeval timeout = {0, 100000};  // 100ms
    amqp_envelope_t reply_envelope;
    amqp_rpc_reply_t rpc_res = amqp_consume_message(conn, &reply_envelope, &timeout, 0);

    if (rpc_res.reply_type == AMQP_RESPONSE_NORMAL) {
        std::string reply_str((char*)reply_envelope.message.body.bytes, reply_envelope.message.body.len);
        
        MessageEnvelope resp_envelope;
        if (message_helpers::parse_envelope(reply_str, resp_envelope) && 
            message_helpers::is_valid_ack(resp_envelope, res.message_id)) {
            res.duration = get_current_time_ms() - msg_start;
            res.success = true;
        } else {
            res.error = "Invalid ACK";
        }
        amqp_destroy_envelope(&reply_envelope);
    } else {
        res.error = "Timeout";
    }

    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);

    return res;
}

int main() {
    auto test_data = test_data_loader::loadTestData();

    MessageStats stats;
    stats.set_metadata({
        {"service", "RabbitMQ"},
        {"language", "C++"},
        {"async", true}
    });
    long long start_time = get_current_time_ms();

    std::cout << " [x] Starting ASYNC transfer of " << test_data.size() << " messages..." << std::endl;

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

    long long end_time = get_current_time_ms();
    stats.set_duration(start_time, end_time);
    
    json report = stats.get_stats();

    std::cout << "\nTest Results (ASYNC):" << std::endl;
    std::cout << "total_sent: " << stats.sent_count << std::endl;
    std::cout << "total_received: " << stats.received_count << std::endl;
    std::cout << "duration_ms: " << stats.get_duration_ms() << std::endl;

    std::ofstream rf("logs/report.txt", std::ios::app);
    if (rf.good()) {
        rf << report.dump() << std::endl;
        rf.close();
    }

    return 0;
}
