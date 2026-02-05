#include <activemq/library/ActiveMQCPP.h>
#include <decaf/lang/Thread.h>
#include <decaf/util/concurrent/CountDownLatch.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/TextMessage.h>
#include <cms/BytesMessage.h>
#include <cms/MessageListener.h>
#include <iostream>
#include <fstream>
#include <string>
#include "../../utils/cpp/stats_collector.hpp"
#include "../../utils/cpp/test_data_loader.hpp"
#include "../../utils/cpp/message_helpers.hpp"

using namespace activemq::core;
using namespace decaf::util::concurrent;
using namespace cms;
using namespace std;
using json = nlohmann::json;
using messaging::MessageEnvelope;
using message_helpers::get_current_time_ms;

class ReplyListener : public MessageListener {
private:
    string response;
    string currentCorrId;
    CountDownLatch* latch;
public:
    ReplyListener() : latch(NULL) {}
    
    void setLatch(CountDownLatch* l, const string& corrId) {
        latch = l;
        currentCorrId = corrId;
        response = "";
    }
    
    string getResponse() { return response; }
    
    virtual void onMessage(const Message* message) {
        const BytesMessage* bytesMsg = dynamic_cast<const BytesMessage*>(message);
        if (bytesMsg && message->getCMSCorrelationID() == currentCorrId) {
            unsigned char* buffer = new unsigned char[bytesMsg->getBodyLength()];
            bytesMsg->readBytes(buffer, bytesMsg->getBodyLength());
            response = string((char*)buffer, bytesMsg->getBodyLength());
            delete[] buffer;
            if (latch) latch->countDown();
        }
    }
};

int main() {
    activemq::library::ActiveMQCPP::initializeLibrary();

    try {
        auto test_data = test_data_loader::loadTestData();
        
        MessageStats stats;
        stats.set_metadata({
            {"service", "ActiveMQ"},
            {"language", "C++"},
            {"async", false}
        });
        long long start_time = get_current_time_ms();

        auto_ptr<ConnectionFactory> factory(ConnectionFactory::createCMSConnectionFactory("tcp://localhost:61616"));
        auto_ptr<Connection> connection(factory->createConnection());
        connection->start();

        auto_ptr<Session> session(connection->createSession(Session::AUTO_ACKNOWLEDGE));
        auto_ptr<Destination> replyDest(session->createTemporaryQueue());
        auto_ptr<MessageProducer> producer(session->createProducer(NULL));
        producer->setDeliveryMode(DeliveryMode::NON_PERSISTENT);

        ReplyListener listener;
        auto_ptr<MessageConsumer> consumer(session->createConsumer(replyDest.get()));
        consumer->setMessageListener(&listener);

        cout << " [x] Starting transfer of " << test_data.size() << " messages..." << endl;

        int corrCounter = 0;
        for (auto& item : test_data) {
            string message_id = message_helpers::extract_message_id(item);
            int target = item.value("target", 0);
            cout << " [x] Sending message " << message_id << " to target " << target << "..." << flush;
            
            long long msg_start = get_current_time_ms();
            
            // Create protobuf envelope
            MessageEnvelope envelope = message_helpers::create_data_envelope(item);
            string body = message_helpers::serialize_envelope(envelope);
            
            // Send as BytesMessage
            auto_ptr<BytesMessage> message(session->createBytesMessage((unsigned char*)body.data(), body.size()));
            message->setCMSReplyTo(replyDest.get());
            message->setCMSCorrelationID("corr-cpp-" + to_string(++corrCounter));
            
            string destName = "test_queue_" + to_string(target);
            auto_ptr<Destination> destination(session->createQueue(destName));
            
            CountDownLatch latch(1);
            listener.setLatch(&latch, message->getCMSCorrelationID());
            
            producer->send(destination.get(), message.get());
            
            if (latch.await(40)) {  // 40ms timeout
                string response = listener.getResponse();
                MessageEnvelope resp_envelope;
                if (message_helpers::parse_envelope(response, resp_envelope) && 
                    message_helpers::is_valid_ack(resp_envelope, message_id)) {
                    long long msg_duration = get_current_time_ms() - msg_start;
                    stats.record_message(true, msg_duration);
                    cout << " [OK]" << endl;
                } else {
                    stats.record_message(false);
                    cout << " [FAILED] Invalid ACK" << endl;
                }
            } else {
                stats.record_message(false);
                cout << " [FAILED] Timeout" << endl;
            }
        }

        long long end_time = get_current_time_ms();
        stats.set_duration(start_time, end_time);
        
        json report = stats.get_stats();

        cout << "\nTest Results:" << endl;
        cout << "service: ActiveMQ" << endl;
        cout << "language: C++" << endl;
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
