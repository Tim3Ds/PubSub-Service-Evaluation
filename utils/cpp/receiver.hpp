#ifndef UNIFIED_RECEIVER_HPP
#define UNIFIED_RECEIVER_HPP

#include <string>
#include <vector>
#include <map>
#include <optional>
#include <atomic>
#include <chrono>
#include <iostream>
#include <functional>
#include "json.hpp"
#include "messaging_utils.hpp"

namespace messaging {
namespace utils {

using json = nlohmann::json;

/**
 * Abstract base class for all C++ receivers.
 * Mirrors the Python UnifiedReceiver architecture.
 */
class UnifiedReceiver {
public:
    int receiver_id;
    std::string service_name;
    std::string language;
    MessagingStats stats;

protected:
    std::atomic<bool> _running{false};

public:
    UnifiedReceiver(int id, const std::string& service, const std::string& lang = "C++")
        : receiver_id(id), service_name(service), language(lang) {}

    virtual ~UnifiedReceiver() = default;

    // Establish connection to the messaging service
    virtual bool connect() = 0;

    // Close connection to the messaging service
    virtual void disconnect() = 0;

    // Receive raw message bytes (returns nullopt on timeout)
    virtual std::optional<std::vector<uint8_t>> _receive_raw(int timeout_ms) = 0;

    // Send raw message bytes (for ACKs)
    virtual bool _send_raw(const std::vector<uint8_t>& data) = 0;

    /**
     * Create a proper MessageEnvelope ACK response.
     * This is the unified ACK format using protobuf ack field.
     */
    MessageEnvelope _create_ack(const MessageEnvelope& original) {
        MessageEnvelope ack_envelope;
        ack_envelope.message_id = "ack_" + original.message_id;
        ack_envelope.target = original.target;
        ack_envelope.type = MessageType::ACK;
        ack_envelope.routing = RoutingMode::REQUEST_REPLY;
        ack_envelope.timestamp = get_timestamp_ms();
        
        // Populate ack field directly (no JSON)
        ack_envelope.ack = std::make_unique<Acknowledgment>();
        ack_envelope.ack->original_message_id = original.message_id;
        ack_envelope.ack->received = true;
        ack_envelope.ack->latency_ms = static_cast<double>(ack_envelope.timestamp - original.timestamp);
        ack_envelope.ack->receiver_id = std::to_string(receiver_id);
        ack_envelope.ack->status = "OK";
        
        // Copy reply_to metadata if present
        if (original.metadata.count("reply_to")) {
            ack_envelope.metadata["reply_to"] = original.metadata.at("reply_to");
        }
        
        return ack_envelope;
    }

    /**
     * Receive a message and send acknowledgment.
     * Returns the received envelope, or nullopt on timeout.
     */
    std::optional<MessageEnvelope> receive_and_ack(int timeout_ms = 1000) {
        auto raw_data = _receive_raw(timeout_ms);
        if (!raw_data) {
            return std::nullopt;
        }

        try {
            // Try to parse as MessageEnvelope JSON
            std::string json_str(raw_data->begin(), raw_data->end());
            MessageEnvelope envelope = MessageEnvelope::from_json(json_str);
            
            stats.received_count++;
            
            // Create and send ACK
            MessageEnvelope ack = _create_ack(envelope);
            std::string ack_json = ack.to_json();
            std::vector<uint8_t> ack_bytes(ack_json.begin(), ack_json.end());
            _send_raw(ack_bytes);
            
            return envelope;
        } catch (const std::exception& e) {
            std::cerr << " [!] Error processing message: " << e.what() << std::endl;
            stats.failed_count++;
            return std::nullopt;
        }
    }

    /**
     * Run the receiver loop.
     */
    void run(bool verbose = true) {
        if (!connect()) {
            std::cerr << " [!] Failed to connect" << std::endl;
            return;
        }

        if (verbose) {
            std::cout << " [*] " << service_name << " Receiver " << receiver_id 
                      << " ready and waiting for messages" << std::endl;
        }

        _running = true;
        stats.start_time = get_timestamp_ms();

        while (_running) {
            auto envelope = receive_and_ack(1000);
            if (envelope && verbose) {
                std::cout << " [Receiver " << receiver_id << "] Received message " 
                          << envelope->message_id << std::endl;
            }
        }

        stats.end_time = get_timestamp_ms();
        
        if (verbose) {
            std::cout << " [x] Receiver " << receiver_id << " shutting down (received " 
                      << stats.received_count << " messages)" << std::endl;
        }

        disconnect();
    }

    /**
     * Stop the receiver loop.
     */
    void stop() {
        _running = false;
    }

    bool is_running() const {
        return _running;
    }
};

// ============================================================================
// ZeroMQ Receiver Implementation
// ============================================================================

class ZeroMQReceiver : public UnifiedReceiver {
private:
    void* _context = nullptr;
    void* _socket = nullptr;
    int _port;
    std::vector<uint8_t> _pending_reply;
    bool _has_pending_reply = false;

public:
    ZeroMQReceiver(int id) : UnifiedReceiver(id, "ZeroMQ"), _port(5556 + id) {}

    bool connect() override {
        // Note: ZMQ headers must be included by the user
        // This is a placeholder - actual implementation requires zmq.hpp
        return false; // Override in actual implementation
    }

    void disconnect() override {
        // Cleanup ZMQ resources
    }

    std::optional<std::vector<uint8_t>> _receive_raw(int timeout_ms) override {
        return std::nullopt; // Override in actual implementation
    }

    bool _send_raw(const std::vector<uint8_t>& data) override {
        return false; // Override in actual implementation
    }

    int get_port() const { return _port; }
};

// ============================================================================
// Redis Receiver Implementation  
// ============================================================================

class RedisReceiver : public UnifiedReceiver {
private:
    std::string _host;
    int _port;
    std::string _channel_name;
    void* _redis_sub = nullptr;  // redisContext*
    void* _redis_pub = nullptr;  // redisContext*
    std::string _pending_reply_to;

public:
    RedisReceiver(int id, const std::string& host = "127.0.0.1", int port = 6379)
        : UnifiedReceiver(id, "Redis"), _host(host), _port(port) {
        _channel_name = "test_channel_" + std::to_string(id);
    }

    bool connect() override {
        return false; // Override in actual implementation
    }

    void disconnect() override {}

    std::optional<std::vector<uint8_t>> _receive_raw(int timeout_ms) override {
        return std::nullopt;
    }

    bool _send_raw(const std::vector<uint8_t>& data) override {
        return false;
    }

    const std::string& get_channel_name() const { return _channel_name; }
};

// ============================================================================
// NATS Receiver Implementation
// ============================================================================

class NatsReceiver : public UnifiedReceiver {
private:
    std::string _host;
    int _port;
    std::string _subject;

public:
    NatsReceiver(int id, const std::string& host = "localhost", int port = 4222)
        : UnifiedReceiver(id, "NATS"), _host(host), _port(port) {
        _subject = "test.subject." + std::to_string(id);
    }

    bool connect() override { return false; }
    void disconnect() override {}
    std::optional<std::vector<uint8_t>> _receive_raw(int timeout_ms) override { return std::nullopt; }
    bool _send_raw(const std::vector<uint8_t>& data) override { return false; }

    const std::string& get_subject() const { return _subject; }
};

// ============================================================================
// RabbitMQ Receiver Implementation
// ============================================================================

class RabbitMQReceiver : public UnifiedReceiver {
private:
    std::string _host;
    int _port;
    std::string _queue_name;

public:
    RabbitMQReceiver(int id, const std::string& host = "localhost", int port = 5672)
        : UnifiedReceiver(id, "RabbitMQ"), _host(host), _port(port) {
        _queue_name = "test_queue_" + std::to_string(id);
    }

    bool connect() override { return false; }
    void disconnect() override {}
    std::optional<std::vector<uint8_t>> _receive_raw(int timeout_ms) override { return std::nullopt; }
    bool _send_raw(const std::vector<uint8_t>& data) override { return false; }

    const std::string& get_queue_name() const { return _queue_name; }
};

// ============================================================================
// ActiveMQ Receiver Implementation
// ============================================================================

class ActiveMQReceiver : public UnifiedReceiver {
private:
    std::string _host;
    int _port;
    std::string _queue_name;

public:
    ActiveMQReceiver(int id, const std::string& host = "localhost", int port = 61616)
        : UnifiedReceiver(id, "ActiveMQ"), _host(host), _port(port) {
        _queue_name = "test_queue_" + std::to_string(id);
    }

    bool connect() override { return false; }
    void disconnect() override {}
    std::optional<std::vector<uint8_t>> _receive_raw(int timeout_ms) override { return std::nullopt; }
    bool _send_raw(const std::vector<uint8_t>& data) override { return false; }

    const std::string& get_queue_name() const { return _queue_name; }
};

} // namespace utils
} // namespace messaging

#endif // UNIFIED_RECEIVER_HPP
