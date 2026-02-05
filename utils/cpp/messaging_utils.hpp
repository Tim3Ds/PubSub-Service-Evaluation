#ifndef MESSAGING_UTILS_H
#define MESSAGING_UTILS_H

#include <string>
#include <vector>
#include <map>
#include <chrono>
#include <memory>
#include <sstream>
#include <iomanip>
#include <random>
#include <thread>
#include <iostream>
#include <algorithm>
#include <numeric>
#include <google/protobuf/util/json_util.h>
#include "messaging.pb.h"

using namespace google::protobuf;

namespace messaging {
namespace utils {

// Message types
enum class MessageType {
    DATA_MESSAGE,
    RPC_REQUEST,
    RPC_RESPONSE,
    ACK,
    CONTROL,
    EVENT
};

// Routing modes
enum class RoutingMode {
    POINT_TO_POINT,
    PUBLISH_SUBSCRIBE,
    REQUEST_REPLY,
    FANOUT
};

// Quality of Service levels
enum class QoSLevel {
    AT_MOST_ONCE,
    AT_LEAST_ONCE,
    EXACTLY_ONCE
};

// Convert our enum to protobuf enum
inline messaging::MessageType to_proto_message_type(MessageType type) {
    switch (type) {
        case MessageType::DATA_MESSAGE: return messaging::MessageType::DATA_MESSAGE;
        case MessageType::RPC_REQUEST: return messaging::MessageType::RPC_REQUEST;
        case MessageType::RPC_RESPONSE: return messaging::MessageType::RPC_RESPONSE;
        case MessageType::ACK: return messaging::MessageType::ACK;
        case MessageType::CONTROL: return messaging::MessageType::CONTROL;
        case MessageType::EVENT: return messaging::MessageType::EVENT;
        default: return messaging::MessageType::DATA_MESSAGE;
    }
}

inline MessageType from_proto_message_type(messaging::MessageType type) {
    switch (type) {
        case messaging::MessageType::DATA_MESSAGE: return MessageType::DATA_MESSAGE;
        case messaging::MessageType::RPC_REQUEST: return MessageType::RPC_REQUEST;
        case messaging::MessageType::RPC_RESPONSE: return MessageType::RPC_RESPONSE;
        case messaging::MessageType::ACK: return MessageType::ACK;
        case messaging::MessageType::CONTROL: return MessageType::CONTROL;
        case messaging::MessageType::EVENT: return MessageType::EVENT;
        default: return MessageType::DATA_MESSAGE;
    }
}

inline messaging::RoutingMode to_proto_routing_mode(RoutingMode mode) {
    switch (mode) {
        case RoutingMode::POINT_TO_POINT: return messaging::RoutingMode::POINT_TO_POINT;
        case RoutingMode::PUBLISH_SUBSCRIBE: return messaging::RoutingMode::PUBLISH_SUBSCRIBE;
        case RoutingMode::REQUEST_REPLY: return messaging::RoutingMode::REQUEST_REPLY;
        case RoutingMode::FANOUT: return messaging::RoutingMode::FANOUT;
        default: return messaging::RoutingMode::POINT_TO_POINT;
    }
}

inline RoutingMode from_proto_routing_mode(messaging::RoutingMode mode) {
    switch (mode) {
        case messaging::RoutingMode::POINT_TO_POINT: return RoutingMode::POINT_TO_POINT;
        case messaging::RoutingMode::PUBLISH_SUBSCRIBE: return RoutingMode::PUBLISH_SUBSCRIBE;
        case messaging::RoutingMode::REQUEST_REPLY: return RoutingMode::REQUEST_REPLY;
        case messaging::RoutingMode::FANOUT: return RoutingMode::FANOUT;
        default: return RoutingMode::POINT_TO_POINT;
    }
}

inline messaging::QoSLevel to_proto_qos_level(QoSLevel level) {
    switch (level) {
        case QoSLevel::AT_MOST_ONCE: return messaging::QoSLevel::AT_MOST_ONCE;
        case QoSLevel::AT_LEAST_ONCE: return messaging::QoSLevel::AT_LEAST_ONCE;
        case QoSLevel::EXACTLY_ONCE: return messaging::QoSLevel::EXACTLY_ONCE;
        default: return messaging::QoSLevel::AT_MOST_ONCE;
    }
}

inline QoSLevel from_proto_qos_level(messaging::QoSLevel level) {
    switch (level) {
        case messaging::QoSLevel::AT_MOST_ONCE: return QoSLevel::AT_MOST_ONCE;
        case messaging::QoSLevel::AT_LEAST_ONCE: return QoSLevel::AT_LEAST_ONCE;
        case messaging::QoSLevel::EXACTLY_ONCE: return QoSLevel::EXACTLY_ONCE;
        default: return QoSLevel::AT_MOST_ONCE;
    }
}

// Acknowledgment wrapper (forward declared before)
class Acknowledgment {
public:
    std::string original_message_id;
    bool received = false;
    double latency_ms = 0.0;
    std::string receiver_id;
    std::string status = "OK";

    ::messaging::Acknowledgment to_proto() const {
        ::messaging::Acknowledgment ack;
        ack.set_original_message_id(original_message_id);
        ack.set_received(received);
        ack.set_latency_ms(latency_ms);
        ack.set_receiver_id(receiver_id);
        ack.set_status(status);
        return ack;
    }

    static Acknowledgment from_proto(const ::messaging::Acknowledgment& ack) {
        Acknowledgment a;
        a.original_message_id = ack.original_message_id();
        a.received = ack.received();
        a.latency_ms = ack.latency_ms();
        a.receiver_id = ack.receiver_id();
        a.status = ack.status();
        return a;
    }

    std::vector<uint8_t> serialize() const {
        ::messaging::Acknowledgment ack = to_proto();
        std::string data;
        ack.SerializeToString(&data);
        return std::vector<uint8_t>(data.begin(), data.end());
    }

    static Acknowledgment deserialize(const std::vector<uint8_t>& data) {
        ::messaging::Acknowledgment ack;
        std::string str(data.begin(), data.end());
        ack.ParseFromString(str);
        return from_proto(ack);
    }
};

// Message envelope wrapper for C++
class MessageEnvelope {
public:
    std::string message_id;
    int target = 0;
    std::string topic;
    MessageType type = MessageType::DATA_MESSAGE;
    std::vector<uint8_t> payload;
    bool async = false;
    int64_t timestamp = 0;
    RoutingMode routing = RoutingMode::POINT_TO_POINT;
    QoSLevel qos = QoSLevel::AT_MOST_ONCE;
    std::map<std::string, std::string> metadata;
    
    // Optional ack field for ACK messages
    std::unique_ptr<Acknowledgment> ack;

    MessageEnvelope() {
        timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
        message_id = generate_message_id();
    }

    // Copy constructor required for deep copy of unique_ptr
    MessageEnvelope(const MessageEnvelope& other) 
        : message_id(other.message_id),
          target(other.target),
          topic(other.topic),
          type(other.type),
          payload(other.payload),
          async(other.async),
          timestamp(other.timestamp),
          routing(other.routing),
          qos(other.qos),
          metadata(other.metadata) {
        
        if (other.ack) {
            ack = std::unique_ptr<Acknowledgment>(new Acknowledgment(*other.ack));
        }
    }

    // Assignment operator
    MessageEnvelope& operator=(const MessageEnvelope& other) {
        if (this != &other) {
            message_id = other.message_id;
            target = other.target;
            topic = other.topic;
            type = other.type;
            payload = other.payload;
            async = other.async;
            timestamp = other.timestamp;
            routing = other.routing;
            qos = other.qos;
            metadata = other.metadata;
            
            if (other.ack) {
                ack = std::unique_ptr<Acknowledgment>(new Acknowledgment(*other.ack));
            } else {
                ack.reset();
            }
        }
        return *this;
    }

    // Move constructor (default)
    MessageEnvelope(MessageEnvelope&&) = default;
    
    // Move assignment (default)
    MessageEnvelope& operator=(MessageEnvelope&&) = default;

    static std::string generate_message_id() {
        auto now = std::chrono::high_resolution_clock::now();
        auto nanos = std::chrono::time_point_cast<std::chrono::nanoseconds>(now).time_since_epoch().count();
        std::stringstream ss;
        ss << std::hex << nanos << std::hex << std::rand();
        return ss.str();
    }

    // Convert to protobuf
    ::messaging::MessageEnvelope to_proto() const {
        ::messaging::MessageEnvelope env;
        env.set_message_id(message_id);
        env.set_target(target);
        env.set_topic(topic);
        env.set_type(to_proto_message_type(type));
        env.set_payload(payload.data(), payload.size());
        env.set_async(async);
        env.set_timestamp(timestamp);
        env.set_routing(to_proto_routing_mode(routing));
        env.set_qos(to_proto_qos_level(qos));
        auto* proto_metadata = env.mutable_metadata();
        for (const auto& pair : metadata) {
            (*proto_metadata)[pair.first] = pair.second;
        }
        
        // Populate ack field if present
        if (ack) {
            auto* proto_ack = env.mutable_ack();
            proto_ack->set_original_message_id(ack->original_message_id);
            proto_ack->set_received(ack->received);
            proto_ack->set_latency_ms(ack->latency_ms);
            proto_ack->set_receiver_id(ack->receiver_id);
            proto_ack->set_status(ack->status);
        }
        
        return env;
    }

    // Create from protobuf
    static MessageEnvelope from_proto(const ::messaging::MessageEnvelope& env) {
        MessageEnvelope envelope;
        envelope.message_id = env.message_id();
        envelope.target = env.target();
        envelope.topic = env.topic();
        envelope.type = from_proto_message_type(env.type());
        envelope.payload = std::vector<uint8_t>(env.payload().begin(), env.payload().end());
        envelope.async = env.async();
        envelope.timestamp = env.timestamp();
        envelope.routing = from_proto_routing_mode(env.routing());
        envelope.qos = from_proto_qos_level(env.qos());
        for (const auto& kv : env.metadata()) {
            envelope.metadata[kv.first] = kv.second;
        }
        
        // Parse ack field if present
        if (env.has_ack()) {
            envelope.ack = std::unique_ptr<Acknowledgment>(new Acknowledgment());
            envelope.ack->original_message_id = env.ack().original_message_id();
            envelope.ack->received = env.ack().received();
            envelope.ack->latency_ms = env.ack().latency_ms();
            envelope.ack->receiver_id = env.ack().receiver_id();
            envelope.ack->status = env.ack().status();
        }
        
        return envelope;
    }

    // Serialize to binary (protobuf)
    std::vector<uint8_t> serialize() const {
        ::messaging::MessageEnvelope env = to_proto();
        std::string data;
        env.SerializeToString(&data);
        return std::vector<uint8_t>(data.begin(), data.end());
    }

    // Deserialize from binary (protobuf)
    static MessageEnvelope deserialize(const std::vector<uint8_t>& data) {
        ::messaging::MessageEnvelope env;
        std::string str(data.begin(), data.end());
        env.ParseFromString(str);
        return from_proto(env);
    }

    // JSON serialization (for debugging)
    std::string to_json() const {
        ::messaging::MessageEnvelope env = to_proto();
        std::string json;
        util::MessageToJsonString(env, &json);
        return json;
    }

    // Create from JSON
    static MessageEnvelope from_json(const std::string& json_str) {
        ::messaging::MessageEnvelope env;
        util::JsonStringToMessage(json_str, &env);
        return from_proto(env);
    }
};

// Message builder
class MessageBuilder {
private:
    MessageEnvelope envelope_;

public:
    MessageBuilder() {
        envelope_.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
        envelope_.message_id = MessageEnvelope::generate_message_id();
    }

    MessageBuilder& set_target(int target) {
        envelope_.target = target;
        return *this;
    }

    MessageBuilder& set_topic(const std::string& topic) {
        envelope_.topic = topic;
        return *this;
    }

    MessageBuilder& set_type(MessageType type) {
        envelope_.type = type;
        return *this;
    }

    MessageBuilder& set_payload(const std::vector<uint8_t>& payload) {
        envelope_.payload = payload;
        return *this;
    }

    MessageBuilder& set_payload(const std::string& str) {
        envelope_.payload = std::vector<uint8_t>(str.begin(), str.end());
        return *this;
    }

    MessageBuilder& set_async(bool async_flag) {
        envelope_.async = async_flag;
        return *this;
    }

    MessageBuilder& set_routing(RoutingMode mode) {
        envelope_.routing = mode;
        return *this;
    }

    MessageBuilder& set_qos(QoSLevel level) {
        envelope_.qos = level;
        return *this;
    }

    MessageBuilder& add_metadata(const std::string& key, const std::string& value) {
        envelope_.metadata[key] = value;
        return *this;
    }

    MessageEnvelope build() {
        return envelope_;
    }
};

// Statistics
struct MessagingStats {
    int64_t sent_count = 0;
    int64_t received_count = 0;
    int64_t failed_count = 0;
    std::vector<double> message_timings;
    int64_t start_time = 0;
    int64_t end_time = 0;

    void record_send(bool success, double timing_ms = 0.0) {
        sent_count++;
        if (success) {
            received_count++;
            if (timing_ms > 0) {
                message_timings.push_back(timing_ms);
            }
        } else {
            failed_count++;
        }
    }

    void set_duration(int64_t start, int64_t end) {
        start_time = start;
        end_time = end;
    }

    double get_duration_ms() const {
        if (start_time && end_time) {
            return static_cast<double>(end_time - start_time);
        }
        return 0.0;
    }

    std::map<std::string, double> get_stats() const {
        std::map<std::string, double> stats;
        double duration = get_duration_ms();
        
        stats["total_sent"] = static_cast<double>(sent_count);
        stats["total_received"] = static_cast<double>(received_count);
        stats["total_failed"] = static_cast<double>(failed_count);
        stats["duration_ms"] = duration;
        stats["messages_per_sec"] = (duration > 0) ? (received_count / duration * 1000.0) : 0;

        if (!message_timings.empty()) {
            double min_t = *std::min_element(message_timings.begin(), message_timings.end());
            double max_t = *std::max_element(message_timings.begin(), message_timings.end());
            double sum = std::accumulate(message_timings.begin(), message_timings.end(), 0.0);
            double mean = sum / message_timings.size();
            
            stats["min_ms"] = min_t;
            stats["max_ms"] = max_t;
            stats["mean_ms"] = mean;
        }

        return stats;
    }
};

// Utility functions
inline std::string generate_unique_id() {
    return MessageEnvelope::generate_message_id();
}

inline int64_t get_timestamp_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

inline void sleep_ms(int ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

} // namespace utils
} // namespace messaging

#endif // MESSAGING_UTILS_H
