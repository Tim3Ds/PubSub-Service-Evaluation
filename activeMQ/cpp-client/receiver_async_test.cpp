#include <activemq/library/ActiveMQCPP.h>
#include <decaf/lang/Thread.h>
#include <decaf/lang/Runnable.h>
#include <decaf/util/concurrent/CountDownLatch.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <activemq/util/Config.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/TextMessage.h>
#include <cms/ExceptionListener.h>
#include <cms/MessageListener.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <memory>
#include <string>
#include <signal.h>
#include "../../include/json.hpp"

using namespace activemq::core;
using namespace decaf::util::concurrent;
using namespace decaf::lang;
using namespace cms;
using namespace std;
using json = nlohmann::json;

bool running = true;

void signal_handler(int sig) {
    running = false;
}

class AsyncReceiver : public MessageListener {
private:
    Session* session;
    int receiver_id;
    int messages_received;

public:
    AsyncReceiver(Session* session, int id) : session(session), receiver_id(id), messages_received(0) {}

    virtual void onMessage(const Message* message) {
        messages_received++;
        const TextMessage* textMessage = dynamic_cast<const TextMessage*>(message);
        if (textMessage != NULL) {
            try {
                string body = textMessage->getText();
                json data = json::parse(body);
                // Handle message_id that could be either string or numeric
                string message_id;
                if (data["message_id"].is_string()) {
                    message_id = data["message_id"].get<std::string>();
                } else {
                    message_id = std::to_string(data["message_id"].get<int>());
                }

                cout << " [Receiver " << receiver_id << "] [ASYNC] Received message " << message_id << endl;

                const Destination* replyTo = message->getCMSReplyTo();
                if (replyTo != NULL) {
                    json resp;
                    resp["status"] = "ACK";
                    resp["message_id"] = data["message_id"];  // Keep original type
                    resp["receiver_id"] = receiver_id;
                    resp["async"] = true;

                    unique_ptr<MessageProducer> producer(session->createProducer(replyTo));
                    unique_ptr<TextMessage> ack(session->createTextMessage(resp.dump()));
                    ack->setCMSCorrelationID(message->getCMSCorrelationID());
                    producer->send(ack.get());
                }
            } catch (exception& e) {
                cerr << " [!] Error: " << e.what() << endl;
            }
        }
    }

    int get_messages_received() const { return messages_received; }
};

int main(int argc, char** argv) {
    int receiver_id = 0;
    for (int i = 1; i < argc; ++i) {
        if (string(argv[i]) == "--id" && i + 1 < argc) {
            receiver_id = stoi(argv[i + 1]);
        }
    }

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    activemq::library::ActiveMQCPP::initializeLibrary();

    string brokerURI = "failover:(tcp://localhost:61613)";
    unique_ptr<ActiveMQConnectionFactory> factory(new ActiveMQConnectionFactory(brokerURI));

    try {
        unique_ptr<Connection> connection(factory->createConnection());
        connection->start();

        unique_ptr<Session> session(connection->createSession(Session::AUTO_ACKNOWLEDGE));
        string queue_name = "test_queue_" + to_string(receiver_id);
        unique_ptr<Queue> dest(session->createQueue(queue_name));

        AsyncReceiver receiver(session.get(), receiver_id);
        unique_ptr<MessageConsumer> consumer(session->createConsumer(dest.get()));
        consumer->setMessageListener(&receiver);

        cout << " [*] [ASYNC] Receiver " << receiver_id << " awaiting messages on " << queue_name << endl;

        while (running) {
            Thread::sleep(100);
        }

        cout << " [x] [ASYNC] Receiver " << receiver_id << " shutting down (received " << receiver.get_messages_received() << " messages)" << endl;
        
        connection->stop();
    } catch (CMSException& e) {
        e.printStackTrace();
    }

    activemq::library::ActiveMQCPP::shutdownLibrary();
    return 0;
}
