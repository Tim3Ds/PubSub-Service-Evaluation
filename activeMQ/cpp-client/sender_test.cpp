#include <activemq/library/ActiveMQCPP.h>
#include <decaf/lang/Thread.h>
#include <decaf/lang/Runnable.h>
#include <decaf/util/concurrent/CountDownLatch.h>
#include <decaf/lang/Integer.h>
#include <decaf/lang/Long.h>
#include <decaf/lang/System.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/TextMessage.h>
#include <cms/BytesMessage.h>
#include <cms/MapMessage.h>
#include <cms/ExceptionListener.h>
#include <cms/MessageListener.h>
#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include "../../include/json.hpp"

using namespace activemq::core;
using namespace decaf::util::concurrent;
using namespace decaf::lang;
using namespace cms;
using namespace std;
using json = nlohmann::json;

class TestDataSender : public MessageListener {
private:
    Connection* connection;
    Session* session;
    Destination* destination;
    Destination* replyDestination;
    MessageProducer* producer;
    MessageConsumer* consumer;
    string response;
    string currentCorrId;
    CountDownLatch* latch;
    int corrCounter;

public:
    TestDataSender(const string& brokerURI, const string& destName) {
        connection = NULL;
        session = NULL;
        destination = NULL;
        replyDestination = NULL;
        producer = NULL;
        consumer = NULL;
        latch = NULL;
        corrCounter = 0;

        auto_ptr<ConnectionFactory> connectionFactory(ConnectionFactory::createCMSConnectionFactory(brokerURI));
        connection = connectionFactory->createConnection();
        connection->start();

        session = connection->createSession(Session::AUTO_ACKNOWLEDGE);
        destination = session->createQueue(destName);
        replyDestination = session->createTemporaryQueue();

        producer = session->createProducer(destination);
        producer->setDeliveryMode(DeliveryMode::NON_PERSISTENT);

        consumer = session->createConsumer(replyDestination);
        consumer->setMessageListener(this);
    }

    virtual ~TestDataSender() {
        cleanup();
    }

    void cleanup() {
        try {
            if (connection != NULL) connection->close();
        } catch (CMSException& e) {}
        delete destination;
        delete replyDestination;
        delete producer;
        delete consumer;
        delete connection;
    }

    virtual void onMessage(const Message* message) {
        const TextMessage* textMessage = dynamic_cast<const TextMessage*>(message);
        if (textMessage != NULL) {
            if (message->getCMSCorrelationID() == currentCorrId) {
                response = textMessage->getText();
                if (latch != NULL) latch->countDown();
            }
        }
    }

    string sendAndWait(const string& msgBody) {
        response = "";
        currentCorrId = "corr-cpp-" + to_string(++corrCounter);
        latch = new CountDownLatch(1);

        auto_ptr<TextMessage> message(session->createTextMessage(msgBody));
        message->setCMSReplyTo(replyDestination);
        message->setCMSCorrelationID(currentCorrId);

        producer->send(message.get());

        if (latch->await(5000)) {
            delete latch;
            latch = NULL;
            return response;
        } else {
            delete latch;
            latch = NULL;
            return "";
        }
    }
};

int main() {
    activemq::library::ActiveMQCPP::initializeLibrary();

    try {
        string brokerURI = "tcp://localhost:61616";
        string destName = "test_request_cpp";

        TestDataSender client(brokerURI, destName);

        ifstream f("../../test_data.json");
        json test_data = json::parse(f);

        struct {
            int sent = 0;
            int received = 0;
            int processed = 0;
            int failed = 0;
            long long start_time;
            long long end_time;
        } stats;

        stats.start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        cout << " [x] Starting transfer of " << test_data.size() << " messages..." << endl;

        int count = 0;
        for (auto& item : test_data) {
            if (++count > 50) break;
            stats.sent++;

            cout << " [x] Sending message " << item["message_id"] << " (" << item["message_name"] << ")..." << flush;
            
            string response = client.sendAndWait(item.dump());
            if (response != "") {
                try {
                    json resp_data = json::parse(response);
                    if (resp_data["status"] == "ACK" && resp_data["message_id"] == item["message_id"]) {
                        stats.received++;
                        stats.processed++;
                        cout << " [OK]" << endl;
                    } else {
                        stats.failed++;
                        cout << " [FAILED] Unexpected response" << endl;
                    }
                } catch (...) {
                    stats.failed++;
                    cout << " [FAILED] Parse error" << endl;
                }
            } else {
                stats.failed++;
                cout << " [FAILED] Timeout" << endl;
            }
        }

        stats.end_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        
        double duration = (double)(stats.end_time - stats.start_time);
        
        json report;
        report["service"] = "ActiveMQ";
        report["language"] = "C++";
        report["total_sent"] = stats.sent;
        report["total_received"] = stats.received;
        report["total_processed"] = stats.processed;
        report["total_failed"] = stats.failed;
        report["duration_ms"] = duration;
        report["processed_per_ms"] = duration > 0 ? stats.processed / duration : 0;
        report["failed_per_ms"] = duration > 0 ? stats.failed / duration : 0;

        cout << "\nTest Results:" << endl;
        cout << report.dump(4) << endl;

        ofstream rf("../../report.txt", ios::app);
        rf << report.dump() << endl;

    } catch (CMSException& e) {
        e.printStackTrace();
    }

    activemq::library::ActiveMQCPP::shutdownLibrary();
    return 0;
}
