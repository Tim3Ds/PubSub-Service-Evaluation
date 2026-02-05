// messaging_utils.cpp
// Implementation of unified protobuf-based serialization utilities

#include "messaging_utils.hpp"
#include <random>
#include <sstream>
#include <iomanip>
#include <thread>
#include <chrono>

namespace messaging {
namespace utils {

// MessageEnvelope implementation
std::vector<uint8_t> MessageEnvelope::serialize() const {
    JsonSerializer serializer;
    return serializer.serialize(*this);
}

MessageEnvelope MessageEnvelope::deserialize(const std::vector<uint8_t>& data) {
    JsonSerializer serializer;
    return serializer.deserialize(data);
}

json MessageEnvelope::to_json() const {
    json j;
    j["message_id"] = message_id;
    j["target"] = target;
    j["topic"] = topic;
    j["type"] = static_cast<int>(type);
    
    // Convert payload bytes to array of integers
    if (!payload.empty()) {
        j["payload"] = json::array();
        for (uint8_t b : payload) {
            j["payload"].push_back(b);
        }
    }
    
    j["async"] = async;
    j["timestamp"] = timestamp;
    j["routing"] = static_cast<int>(routing);
    j["qos"] = static_cast<int>(qos);
    j["metadata"] = metadata;
    return j;
}

MessageEnvelope MessageEnvelope::from_json(const json& j) {
    MessageEnvelope env;
    env.message_id = j.value("message_id", "");
    env.target = j.value("target", 0);
    env.topic = j.value("topic", "");
    env.type = static_cast<MessageType>(j.value("type", 0));
    
    if (j.contains("payload")) {
        if (j["payload"].is_array()) {
            for (const auto& v : j["payload"]) {
                env.payload.push_back(static_cast<uint8_t>(v.get<int>()));
            }
        } else if (j["payload"].is_string()) {
            std::string str = j["payload"].get<std::string>();
            env.payload = std::vector<uint8_t>(str.begin(), str.end());
        }
    }
    
    env.async = j.value("async", false);
    env.timestamp = j.value("timestamp", 0);
    env.routing = static_cast<RoutingMode>(j.value("routing", 0));
    env.qos = static_cast<QoSLevel>(j.value("qos", 0));
    
    if (j.contains("metadata") && j["metadata"].is_object()) {
        for (const auto& [key, value] : j["metadata"].items()) {
            env.metadata[key] = value.get<std::string>();
        }
    }
    
    return env;
}

// DataMessage implementation
json DataMessage::to_json() const {
    json j = json::object();
    j["message_name"] = message_name;
    j["message_value"] = json::array();
    for (const auto& v : message_value) {
        j["message_value"].push_back(v);
    }
    return j;
}

DataMessage DataMessage::from_json(const json& j) {
    DataMessage msg;
    msg.message_name = j.value("message_name", "");
    if (j.contains("message_value") && j["message_value"].is_array()) {
        for (const auto& v : j["message_value"]) {
            msg.message_value.push_back(v.get<std::string>());
        }
    }
    return msg;
}

// RPCRequest implementation
json RPCRequest::to_json() const {
    json j = json::object();
    j["method"] = method;
    j["timeout_ms"] = timeout_ms;
    j["arguments"] = json::array();
    for (uint8_t b : arguments) {
        j["arguments"].push_back(b);
    }
    return j;
}

RPCRequest RPCRequest::from_json(const json& j) {
    RPCRequest req;
    req.method = j.value("method", "");
    req.timeout_ms = j.value("timeout_ms", 5000);
    
    if (j.contains("arguments") && j["arguments"].is_array()) {
        for (const auto& v : j["arguments"]) {
            req.arguments.push_back(static_cast<uint8_t>(v.get<int>()));
        }
    }
    
    return req;
}

// RPCResponse implementation
json RPCResponse::to_json() const {
    json j = json::object();
    j["success"] = success;
    j["error_message"] = error_message;
    j["result"] = json::array();
    for (uint8_t b : result) {
        j["result"].push_back(b);
    }
    return j;
}

RPCResponse RPCResponse::from_json(const json& j) {
    RPCResponse resp;
    resp.success = j.value("success", false);
    resp.error_message = j.value("error_message", "");
    
    if (j.contains("result") && j["result"].is_array()) {
        for (const auto& v : j["result"]) {
            resp.result.push_back(static_cast<uint8_t>(v.get<int>()));
        }
    }
    
    return resp;
}

// Acknowledgment implementation
json Acknowledgment::to_json() const {
    json j = json::object();
    j["original_message_id"] = original_message_id;
    j["received"] = received;
    j["latency_ms"] = latency_ms;
    j["receiver_id"] = receiver_id;
    j["status"] = status;
    return j;
}

Acknowledgment Acknowledgment::from_json(const json& j) {
    Acknowledgment ack;
    ack.original_message_id = j.value("original_message_id", "");
    ack.received = j.value("received", false);
    ack.latency_ms = j.value("latency_ms", 0);
    ack.receiver_id = j.value("receiver_id", "");
    ack.status = j.value("status", "");
    return ack;
}

// ControlMessage implementation
json ControlMessage::to_json() const {
    json j = json::object();
    j["type"] = static_cast<int>(type);
    j["source"] = source;
    j["destination"] = destination;
    j["data"] = json::array();
    for (uint8_t b : data) {
        j["data"].push_back(b);
    }
    return j;
}

ControlMessage ControlMessage::from_json(const json& j) {
    ControlMessage ctrl;
    ctrl.type = static_cast<ControlType>(j.value("type", 0));
    ctrl.source = j.value("source", "");
    ctrl.destination = j.value("destination", "");
    
    if (j.contains("data") && j["data"].is_array()) {
        for (const auto& v : j["data"]) {
            ctrl.data.push_back(static_cast<uint8_t>(v.get<int>()));
        }
    }
    
    return ctrl;
}

// MessageBuilder implementation
std::string MessageBuilder::generate_message_id() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<int> dis(0, 15);
    static std::uniform_int_distribution<int> dis2(8, 11);
    
    std::stringstream ss;
    ss << std::hex;
    for (int i = 0; i < 8; ++i) {
        ss << dis(gen);
    }
    ss << "-";
    for (int i = 0; i < 4; ++i) {
        ss << dis(gen);
    }
    ss << "-4";
    for (int i = 0; i < 3; ++i) {
        ss << dis(gen);
    }
    ss << "-";
    ss << dis2(gen);
    for (int i = 0; i < 3; ++i) {
        ss << dis(gen);
    }
    ss << "-";
    for (int i = 0; i < 12; ++i) {
        ss << dis(gen);
    }
    return ss.str();
}

// Protocol factory
std::unique_ptr<ProtocolSerializer> create_serializer(bool use_binary) {
    if (use_binary) {
        return std::make_unique<BinarySerializer>();
    }
    return std::make_unique<JsonSerializer>();
}

// Utility functions
std::string generate_unique_id() {
    return MessageBuilder::generate_message_id();
}

int64_t get_timestamp_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

void sleep_ms(int ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

} // namespace utils
} // namespace messaging
