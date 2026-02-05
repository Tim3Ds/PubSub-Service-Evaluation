#include <activemq/library/ActiveMQCPP.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/BytesMessage.h>
#include <iostream>
#include <fstream>
#include <future>
#include <vector>
#include "../../utils/cpp/stats_collector.hpp"
#include "../../utils/cpp/test_data_loader.hpp"
#include "../../utils/cpp/message_helpers.hpp"

using namespace activemq::core;
using namespace cms;
using namespace std;
using json = nlohmann::json;
using messaging::MessageEnvelope;
using message_helpers::get_current_time_ms;

struct TaskResult {
    bool success;
    string message_id;
    long long duration;
    string error;
};

TaskResult send_message_task(Connection* connection, const json& item) {
    TaskResult res;
    res.success = false;
    res.message_id = message_helpers::extract_message_id(item);
    res.duration = 0;
    
    try {
        auto_ptr<Session> session(connection->createSession(Session::AUTO_ACKNOWLEDGE));
        auto_ptr<Destination> replyDest(session->createTemporaryQueue());
        auto_ptr<MessageProducer> producer(session->createProducer(NULL));
        auto_ptr<MessageConsumer> consumer(session->createConsumer(replyDest.get()));

        int target = item.value("target", 0);
        long long msg_start = get_current_time_ms();
        
        // Create and send message
        MessageEnvelope envelope = message_helpers::create_data_envelope(item);
        string body = message_helpers::serialize_envelope(envelope);
        
        auto_ptr<BytesMessage> message(session->createBytesMessage((unsigned char*)body.data(), body.size()));
        message->setCMSReplyTo(replyDest.get());
        message->setCMSCorrelationID("corr-cpp-async-" + res.message_id);
        
        string destName = "test_queue_" + to_string(target);
        auto_ptr<Destination> destination(session->createQueue(destName));
        producer->send(destination.get(), message.get());
        
        // Wait for reply
        auto_ptr<Message> reply(consumer->receive(100));  // 100ms timeout
        if (reply.get()) {
            const BytesMessage* bytesReply = dynamic_cast<const BytesMessage*>(reply.get());
            if (bytesReply && bytesReply->getCMSCorrelationID() == message->getCMSCorrelationID()) {
                unsigned char* buffer = new unsigned char[bytesReply->getBodyLength()];
                bytesReply->readBytes(buffer, bytesReply->getBodyLength());
                string response((char*)buffer, bytesReply->getBodyLength());
                delete[] buffer;
                
                MessageEnvelope resp_envelope;
                if (message_helpers::parse_envelope(response, resp_envelope) && 
                    message_helpers::is_valid_ack(resp_envelope, res.message_id)) {
                    res.duration = get_current_time_ms() - msg_start;
                    res.success = true;
                } else {
                    res.error = "Invalid ACK";
                }
            }
        } else {
            res.error = "Timeout";
        }
    } catch (CMSException& e) {
        res.error = e.getMessage();
    }
    
    return res;
}

int main() {
    activemq::library::ActiveMQCPP::initializeLibrary();

    try {
        auto test_data = test_data_loader::loadTestData();

        MessageStats stats;
        stats.set_metadata({
            {"service", "ActiveMQ"},
            {"language", "C++"},
            {"async", true}
        });
        long long start_time = get_current_time_ms();

        cout << " [x] Starting ASYNC transfer of " << test_data.size() << " messages..." << endl;

        auto_ptr<ConnectionFactory> factory(ConnectionFactory::createCMSConnectionFactory("tcp://localhost:61616"));
        auto_ptr<Connection> connection(factory->createConnection());
        connection->start();

        vector<future<TaskResult>> futures;
        for (auto& item : test_data) {
            futures.push_back(async(launch::async, send_message_task, connection.get(), item));
        }

        for (auto& fut : futures) {
            TaskResult res = fut.get();
            if (res.success) {
                stats.record_message(true, res.duration);
                cout << " [OK] Message " << res.message_id << " acknowledged" << endl;
            } else {
                stats.record_message(false);
                cout << " [FAILED] Message " << res.message_id << ": " << res.error << endl;
            }
        }

        connection->close();

        long long end_time = get_current_time_ms();
        stats.set_duration(start_time, end_time);
        
        json report = stats.get_stats();

        cout << "\nTest Results (ASYNC):" << endl;
        cout << "total_sent: " << stats.sent_count << endl;
        cout << "total_received: " << stats.received_count << endl;
        cout << "duration_ms: " << stats.get_duration_ms() << endl;

        ofstream rf("logs/report.txt", ios::app);
        if (rf.good()) {
            rf << report.dump() << endl;
            rf.close();
        }

    } catch (CMSException& e) {
        e.printStackTrace();
    }

    activemq::library::ActiveMQCPP::shutdownLibrary();
    return 0;
}
