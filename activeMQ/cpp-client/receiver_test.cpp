#include <activemq/library/ActiveMQCPP.h>
#include <decaf/lang/Thread.h>
#include <decaf/lang/Runnable.h>
#include <decaf/util/concurrent/CountDownLatch.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/TextMessage.h>
#include <cms/MessageListener.h>
#include <iostream>
#include <string>
#include "../../include/json.hpp"

using namespace activemq::core;
using namespace decaf::util::concurrent;
using namespace decaf::lang;
using namespace cms;
using namespace std;
using json = nlohmann::json;

class TestDataReceiver : public MessageListener {
private:
    Connection* connection;
    Session* session;
    Destination* destination;
    MessageConsumer* consumer;
    MessageProducer* producer;

public:
    TestDataReceiver(const string& brokerURI, const string& destName) {
        connection = NULL;
        session = NULL;
        destination = NULL;
        consumer = NULL;
        producer = NULL;

        auto_ptr<ConnectionFactory> connectionFactory(ConnectionFactory::createCMSConnectionFactory(brokerURI));
        connection = connectionFactory->createConnection();
        connection->start();

        session = connection->createSession(Session::AUTO_ACKNOWLEDGE);
        destination = session->createQueue(destName);
        consumer = session->createConsumer(destination);
        consumer->setMessageListener(this);
        
        producer = session->createProducer(NULL);
    }

    virtual ~TestDataReceiver() {
        cleanup();
    }

    void cleanup() {
        try {
            if (connection != NULL) connection->close();
        } catch (CMSException& e) {}
        delete destination;
        delete consumer;
        delete producer;
        delete session;
        delete connection;
    }

    virtual void onMessage(const Message* message) {
        const TextMessage* textMessage = dynamic_cast<const TextMessage*>(message);
        if (textMessage != NULL) {
            try {
                json data = json::parse(textMessage->getText());
                int message_id = data["message_id"];
                cout << " [x] Received message " << message_id << endl;


                json resp;
                resp["status"] = "ACK";
                resp["message_id"] = message_id;

                auto_ptr<TextMessage> reply(session->createTextMessage(resp.dump()));
                reply->setCMSCorrelationID(message->getCMSCorrelationID());
                
                const Destination* replyTo = message->getCMSReplyTo();
                if (replyTo != NULL) {
                    producer->send(replyTo, reply.get());
                }
            } catch (...) {
                cout << " [!] Error processing message" << endl;
            }
        }
    }
};

int main() {
    activemq::library::ActiveMQCPP::initializeLibrary();

    try {
        string brokerURI = "tcp://localhost:61616";
        string destName = "test_request_cpp";

        TestDataReceiver receiver(brokerURI, destName);
        cout << " [x] Awaiting ActiveMQ requests (C++)" << endl;

        CountDownLatch latch(1);
        latch.await();

    } catch (CMSException& e) {
        e.printStackTrace();
    }

    activemq::library::ActiveMQCPP::shutdownLibrary();
    return 0;
}
