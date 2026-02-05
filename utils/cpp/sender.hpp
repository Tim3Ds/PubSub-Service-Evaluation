#ifndef UNIFIED_SENDER_HPP
#define UNIFIED_SENDER_HPP

#include <string>
#include <vector>
#include <map>
#include <optional>
#include <chrono>
#include <iostream>
#include "json.hpp"
#include "messaging_utils.hpp"

namespace messaging {
namespace utils {

using json = nlohmann::json;

/**
 * Result of a send operation.
 */
struct SendResult {
    bool success = false;
    std::string message_id;
    double latency_ms = 0.0;
    std::string receiver_id;
    std::string error;
};

/**
 * Abstract base class for all C++ senders.
 * Mirrors the Python UnifiedSender architecture.
 */
class UnifiedSender {
public:
    std::string service_name;
    std::string language;
    MessagingStats stats;

public:
    UnifiedSender(const std::string& service, const std::string& lang = "C++")
        : service_name(service), language(lang) {}

    virtual ~UnifiedSender() = default;

    // Establish connection to the messaging service
    virtual bool connect() = 0;

    // Close connection to the messaging service
    virtual void disconnect() = 0;

    // Send raw message envelope without waiting for response
    virtual bool _send_raw(const MessageEnvelope& envelope) = 0;

    // Send message and wait for acknowledgment
    virtual std::optional<MessageEnvelope> _send_with_ack(
        const MessageEnvelope& envelope, int timeout_ms) = 0;

    /**
     * Send a message to a target receiver.
     */
    SendResult send(
        int target,
        const std::string& payload,
        const std::string& topic = "",
        bool wait_for_ack = true,
        int timeout_ms = 5000,
        const std::map<std::string, std::string>& metadata = {}
    ) {
        SendResult result;
        
        // Build message envelope
        MessageEnvelope envelope;
        envelope.message_id = MessageEnvelope::generate_message_id();
        envelope.target = target;
        envelope.topic = topic;
        envelope.type = MessageType::DATA_MESSAGE;
        envelope.routing = RoutingMode::REQUEST_REPLY;
        envelope.timestamp = get_timestamp_ms();
        envelope.payload = std::vector<uint8_t>(payload.begin(), payload.end());
        envelope.metadata = metadata;
        
        result.message_id = envelope.message_id;
        int64_t start_time = get_timestamp_ms();

        try {
            if (wait_for_ack) {
                auto ack = _send_with_ack(envelope, timeout_ms);
                if (ack) {
                    result.success = true;
                    result.latency_ms = static_cast<double>(get_timestamp_ms() - start_time);
                    
                    // Read ACK from proto ack field (or fallback to JSON payload)
                    if (ack->ack) {
                        // Use proto ack field
                        result.receiver_id = ack->ack->receiver_id;
                        result.success = ack->ack->received;
                        if (!result.success) {
                            result.error = ack->ack->status;
                        }
                    } else if (!ack->payload.empty()) {
                        // Fallback: parse JSON payload for backward compatibility
                        try {
                            std::string ack_str(ack->payload.begin(), ack->payload.end());
                            json ack_data = json::parse(ack_str);
                            result.receiver_id = ack_data.value("receiver_id", "");
                            result.success = ack_data.value("received", true);
                        } catch (...) {}
                    }
                    
                    stats.record_send(result.success, result.latency_ms);
                } else {
                    result.success = false;
                    result.error = "Timeout or no response";
                    stats.record_send(false);
                }
            } else {
                result.success = _send_raw(envelope);
                result.latency_ms = static_cast<double>(get_timestamp_ms() - start_time);
                stats.record_send(result.success, result.latency_ms);
            }
        } catch (const std::exception& e) {
            result.success = false;
            result.error = e.what();
            stats.record_send(false);
        }

        return result;
    }

    /**
     * Run a performance test and return statistics.
     */
    std::map<std::string, double> run_performance_test(
        const json& test_data,
        bool wait_for_ack = true,
        int timeout_ms = 40
    ) {
        stats = MessagingStats();  // Reset stats
        stats.start_time = get_timestamp_ms();

        for (const auto& item : test_data) {
            int target = item.value("target", 0);
            std::string payload = item.dump();
            
            send(target, payload, "", wait_for_ack, timeout_ms);
        }

        stats.end_time = get_timestamp_ms();
        return stats.get_stats();
    }

    /**
     * Parse an ACK response from various formats.
     * Handles both MessageEnvelope ACK (type=4) and legacy JSON ACKs.
     */
    static bool parse_ack_response(
        const std::string& response_str,
        std::string& receiver_id,
        bool& received
    ) {
        try {
            json resp = json::parse(response_str);
            
            // Check for MessageEnvelope ACK format (type == 4)
            int resp_type = resp.value("type", 0);
            if (resp_type == 4) {
                // Parse payload which is JSON string
                std::string payload_str = resp.value("payload", "");
                if (!payload_str.empty()) {
                    json ack_payload = json::parse(payload_str);
                    received = ack_payload.value("received", false);
                    receiver_id = ack_payload.value("receiver_id", "");
                    return received;
                }
            }
            
            // Legacy format: check for status field
            std::string status = resp.value("status", "");
            if (status == "ACK" || status == "OK") {
                received = true;
                if (resp.contains("receiver_id")) {
                    if (resp["receiver_id"].is_string()) {
                        receiver_id = resp["receiver_id"].get<std::string>();
                    } else {
                        receiver_id = std::to_string(resp["receiver_id"].get<int>());
                    }
                }
                return true;
            }
            
            // Check for received field directly
            if (resp.contains("received")) {
                received = resp["received"].get<bool>();
                receiver_id = resp.value("receiver_id", "");
                return received;
            }
            
            return false;
        } catch (...) {
            return false;
        }
    }
};

// ============================================================================
// ZeroMQ Sender Implementation
// ============================================================================

class ZeroMQSender : public UnifiedSender {
private:
    void* _context = nullptr;
    std::map<int, void*> _sockets;  // target -> socket

public:
    ZeroMQSender() : UnifiedSender("ZeroMQ") {}

    bool connect() override { return false; }
    void disconnect() override {}
    bool _send_raw(const MessageEnvelope& envelope) override { return false; }
    std::optional<MessageEnvelope> _send_with_ack(const MessageEnvelope& envelope, int timeout_ms) override {
        return std::nullopt;
    }

    int get_port(int target) const { return 5556 + target; }
};

// ============================================================================
// Redis Sender Implementation
// ============================================================================

class RedisSender : public UnifiedSender {
private:
    std::string _host;
    int _port;
    void* _redis = nullptr;

public:
    RedisSender(const std::string& host = "127.0.0.1", int port = 6379)
        : UnifiedSender("Redis"), _host(host), _port(port) {}

    bool connect() override { return false; }
    void disconnect() override {}
    bool _send_raw(const MessageEnvelope& envelope) override { return false; }
    std::optional<MessageEnvelope> _send_with_ack(const MessageEnvelope& envelope, int timeout_ms) override {
        return std::nullopt;
    }

    std::string get_channel_name(int target) const { 
        return "test_channel_" + std::to_string(target); 
    }
};

// ============================================================================
// NATS Sender Implementation
// ============================================================================

class NatsSender : public UnifiedSender {
private:
    std::string _host;
    int _port;

public:
    NatsSender(const std::string& host = "localhost", int port = 4222)
        : UnifiedSender("NATS"), _host(host), _port(port) {}

    bool connect() override { return false; }
    void disconnect() override {}
    bool _send_raw(const MessageEnvelope& envelope) override { return false; }
    std::optional<MessageEnvelope> _send_with_ack(const MessageEnvelope& envelope, int timeout_ms) override {
        return std::nullopt;
    }

    std::string get_subject(int target) const { 
        return "test.subject." + std::to_string(target); 
    }
};

// ============================================================================
// RabbitMQ Sender Implementation
// ============================================================================

class RabbitMQSender : public UnifiedSender {
private:
    std::string _host;
    int _port;

public:
    RabbitMQSender(const std::string& host = "localhost", int port = 5672)
        : UnifiedSender("RabbitMQ"), _host(host), _port(port) {}

    bool connect() override { return false; }
    void disconnect() override {}
    bool _send_raw(const MessageEnvelope& envelope) override { return false; }
    std::optional<MessageEnvelope> _send_with_ack(const MessageEnvelope& envelope, int timeout_ms) override {
        return std::nullopt;
    }

    std::string get_queue_name(int target) const { 
        return "test_queue_" + std::to_string(target); 
    }
};

// ============================================================================
// ActiveMQ Sender Implementation
// ============================================================================

class ActiveMQSender : public UnifiedSender {
private:
    std::string _host;
    int _port;

public:
    ActiveMQSender(const std::string& host = "localhost", int port = 61616)
        : UnifiedSender("ActiveMQ"), _host(host), _port(port) {}

    bool connect() override { return false; }
    void disconnect() override {}
    bool _send_raw(const MessageEnvelope& envelope) override { return false; }
    std::optional<MessageEnvelope> _send_with_ack(const MessageEnvelope& envelope, int timeout_ms) override {
        return std::nullopt;
    }

    std::string get_queue_name(int target) const { 
        return "test_queue_" + std::to_string(target); 
    }
};

} // namespace utils
} // namespace messaging

#endif // UNIFIED_SENDER_HPP
