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
#include "../../include/stats_collector.hpp"

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
    Destination* replyDestination;
    MessageProducer* producer;
    MessageConsumer* consumer;
    string response;
    string currentCorrId;
    CountDownLatch* latch;
    int corrCounter;

public:
    TestDataSender(const string& brokerURI) {
        connection = NULL;
        session = NULL;
        replyDestination = NULL;
        producer = NULL;
        consumer = NULL;
        latch = NULL;
        corrCounter = 0;

        auto_ptr<ConnectionFactory> connectionFactory(ConnectionFactory::createCMSConnectionFactory(brokerURI));
        connection = connectionFactory->createConnection();
        connection->start();

        session = connection->createSession(Session::AUTO_ACKNOWLEDGE);
        replyDestination = session->createTemporaryQueue();

        producer = session->createProducer(NULL);
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
        delete replyDestination;
        delete producer;
        delete consumer;
        delete session;
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

    string sendAndWait(int target_id, const string& msgBody) {
        response = "";
        currentCorrId = "corr-cpp-" + to_string(++corrCounter);
        latch = new CountDownLatch(1);

        auto_ptr<TextMessage> message(session->createTextMessage(msgBody));
        message->setCMSReplyTo(replyDestination);
        message->setCMSCorrelationID(currentCorrId);

        string destName = "test_queue_" + to_string(target_id);
        auto_ptr<Destination> destination(session->createQueue(destName));
        producer->send(destination.get(), message.get());

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
        TestDataSender client(brokerURI);

        string data_path = "test_data.json";
        ifstream f(data_path);
        if (!f.good()) {
            f.close();
            data_path = "../../test_data.json";
            f.open(data_path);
        }
        if (!f.good()) {
            f.close();
            data_path = "/home/tim/repos/test_data.json";
            f.open(data_path);
        }
        
        if (!f.good()) {
            cerr << " [!] Could not find test_data.json" << endl;
            activemq::library::ActiveMQCPP::shutdownLibrary();
            return 1;
        }
        
        json test_data = json::parse(f);

        MessageStats stats;
        long long start_time = get_current_time_ms();
        stats.set_duration(start_time, 0);

        cout << " [x] Starting transfer of " << test_data.size() << " messages..." << endl;

        for (auto& item : test_data) {
            int target = item.value("target", 0);
            cout << " [x] Sending message " << item["message_id"] << " to target " << target << "..." << flush;
            
            long long msg_start = get_current_time_ms();
            string response = client.sendAndWait(target, item.dump());
            if (response != "") {
                try {
                    json resp_data = json::parse(response);
                    if (resp_data["status"] == "ACK" && resp_data["message_id"] == item["message_id"]) {
                        long long msg_duration = get_current_time_ms() - msg_start;
                        stats.record_message(true, msg_duration);
                        cout << " [OK]" << endl;
                    } else {
                        stats.record_message(false);
                        cout << " [FAILED] Unexpected response" << endl;
                    }
                } catch (...) {
                    stats.record_message(false);
                    cout << " [FAILED] Parse error" << endl;
                }
            } else {
                stats.record_message(false);
                cout << " [FAILED] Timeout" << endl;
            }
        }

        long long end_time = get_current_time_ms();
        stats.set_duration(start_time, end_time);
        
        json report = stats.get_stats();
        report["service"] = "ActiveMQ";
        report["language"] = "C++";

        cout << "\nTest Results:" << endl;
        cout << "service: ActiveMQ" << endl;
        cout << "language: C++" << endl;
        cout << "total_sent: " << stats.sent_count << endl;
        cout << "total_received: " << stats.received_count << endl;
        cout << "duration_ms: " << stats.get_duration_ms() << endl;
        if (report.contains("message_timing_stats")) {
            cout << "message_timing_stats: " << report["message_timing_stats"].dump() << endl;
        }

        ofstream rf("report.txt", ios::app);
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
