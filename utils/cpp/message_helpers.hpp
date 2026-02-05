#ifndef MESSAGE_HELPERS_HPP
#define MESSAGE_HELPERS_HPP

#include <string>
#include <chrono>
#include "json.hpp"
#include "messaging.pb.h"

using json = nlohmann::json;
using messaging::MessageEnvelope;
using messaging::Acknowledgment;
using messaging::DataMessage;
using messaging::MessageType;
using messaging::RoutingMode;

namespace message_helpers {

// Get current time in milliseconds
inline long long get_current_time_ms() {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    return ms.time_since_epoch().count();
}

// Create a MessageEnvelope from JSON test data
inline MessageEnvelope create_data_envelope(const json& item, RoutingMode routing = RoutingMode::REQUEST_REPLY) {
    MessageEnvelope envelope;
    
    // Set message ID (handle both string and numeric)
    std::string message_id;
    if (item["message_id"].is_string()) {
        message_id = item["message_id"].get<std::string>();
    } else {
        message_id = std::to_string(item["message_id"].get<long long>());
    }
    envelope.set_message_id(message_id);
    
    // Set target
    envelope.set_target(item.value("target", 0));
    
    // Set type and timestamp
    envelope.set_type(MessageType::DATA_MESSAGE);
    envelope.set_timestamp(get_current_time_ms());
    envelope.set_routing(routing);
    
    // Set metadata if present in item or for specific needs
    if (item.contains("metadata") && item["metadata"].is_object()) {
        auto& meta = *envelope.mutable_metadata();
        for (auto it = item["metadata"].begin(); it != item["metadata"].end(); ++it) {
            meta[it.key()] = it.value().get<std::string>();
        }
    }
    
    // Create DataMessage payload
    DataMessage data_msg;
    data_msg.set_message_name(item.value("message_name", ""));
    
    if (item.contains("message_value") && item["message_value"].is_array()) {
        for (const auto& val : item["message_value"]) {
            data_msg.add_message_value(val.is_string() ? val.get<std::string>() : val.dump());
        }
    }
    
    // Serialize DataMessage into the envelope payload
    data_msg.SerializeToString(envelope.mutable_payload());
    
    return envelope;
}

// Create an ACK envelope in response to a received message
inline MessageEnvelope create_ack_envelope(
    const std::string& original_message_id,
    int target,
    const std::string& receiver_id,
    const std::string& status = "OK",
    double latency_ms = 0.5
) {
    MessageEnvelope envelope;
    envelope.set_message_id("ack_" + original_message_id);
    envelope.set_target(target);
    envelope.set_type(MessageType::ACK);
    envelope.set_timestamp(get_current_time_ms());
    
    // Create and populate Acknowledgment
    Acknowledgment* ack = envelope.mutable_ack();
    ack->set_original_message_id(original_message_id);
    ack->set_received(true);
    ack->set_latency_ms(latency_ms);
    ack->set_receiver_id(receiver_id);
    ack->set_status(status);
    
    return envelope;
}

// Create an ACK envelope from a received MessageEnvelope
inline MessageEnvelope create_ack_from_envelope(
    const MessageEnvelope& received_envelope,
    const std::string& receiver_id,
    const std::string& status = "OK",
    double latency_ms = 0.5
) {
    return create_ack_envelope(
        received_envelope.message_id(),
        received_envelope.target(),
        receiver_id,
        status,
        latency_ms
    );
}

// Parse a MessageEnvelope from binary string
inline bool parse_envelope(const std::string& data, MessageEnvelope& envelope) {
    return envelope.ParseFromString(data);
}

// Serialize a MessageEnvelope to binary string
inline std::string serialize_envelope(const MessageEnvelope& envelope) {
    std::string output;
    envelope.SerializeToString(&output);
    return output;
}

// Check if an envelope is a valid ACK for the given message_id
inline bool is_valid_ack(const MessageEnvelope& envelope, const std::string& expected_message_id) {
    if (envelope.type() != MessageType::ACK || !envelope.has_ack()) {
        return false;
    }
    
    const Acknowledgment& ack = envelope.ack();
    return ack.received() && 
           ack.original_message_id() == expected_message_id &&
           ack.status() == "OK";
}

// Extract message_id from JSON (handles both string and numeric types)
inline std::string extract_message_id(const json& item) {
    if (item["message_id"].is_string()) {
        return item["message_id"].get<std::string>();
    } else {
        return std::to_string(item["message_id"].get<long long>());
    }
}

} // namespace message_helpers

#endif // MESSAGE_HELPERS_HPP
