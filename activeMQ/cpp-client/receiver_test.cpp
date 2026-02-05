#include <activemq/library/ActiveMQCPP.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/BytesMessage.h>
#include <cms/MessageListener.h>
#include <iostream>
#include <string>
#include <signal.h>
#include <atomic>
#include <thread>
#include <chrono>
#include "../../utils/cpp/message_helpers.hpp"

using namespace activemq::core;
using namespace cms;
using namespace std;
using messaging::MessageEnvelope;
using message_helpers::get_current_time_ms;

atomic<bool> running(true);

void signal_handler(int sig) {
    running = false;
}

class RequestListener : public MessageListener {
private:
    Session* session;
    MessageProducer* producer;
    int receiver_id;
public:
    RequestListener(Session* s, MessageProducer* p, int id) 
        : session(s), producer(p), receiver_id(id) {}
    
    virtual void onMessage(const Message* message) {
        try {
            const BytesMessage* bytesMsg = dynamic_cast<const BytesMessage*>(message);
            if (bytesMsg) {
                // Read message
                unsigned char* buffer = new unsigned char[bytesMsg->getBodyLength()];
                bytesMsg->readBytes(buffer, bytesMsg->getBodyLength());
                string request_str((char*)buffer, bytesMsg->getBodyLength());
                delete[] buffer;
                
                // Parse message
                MessageEnvelope msg_envelope;
                if (message_helpers::parse_envelope(request_str, msg_envelope)) {
                    string message_id = msg_envelope.message_id();
                    cout << " [x] Received message " << message_id << endl;
                    
                    // Create ACK
                    MessageEnvelope response = message_helpers::create_ack_from_envelope(
                        msg_envelope,
                        to_string(receiver_id)
                    );
                    string response_str = message_helpers::serialize_envelope(response);
                    
                    // Send ACK
                    auto_ptr<BytesMessage> reply(session->createBytesMessage(
                        (unsigned char*)response_str.data(), response_str.size()));
                    reply->setCMSCorrelationID(message->getCMSCorrelationID());
                    
                    producer->send(message->getCMSReplyTo(), reply.get());
                }
            }
        } catch (CMSException& e) {
            cerr << " [!] Error: " << e.getMessage() << endl;
        }
    }
};

int main(int argc, char* argv[]) {
    int receiver_id = 0;
    
    for (int i = 1; i < argc; i++) {
        if (string(argv[i]) == "--id" && i + 1 < argc) {
            receiver_id = stoi(argv[i + 1]);
            break;
        }
    }
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    activemq::library::ActiveMQCPP::initializeLibrary();

    try {
        auto_ptr<ConnectionFactory> factory(ConnectionFactory::createCMSConnectionFactory("tcp://localhost:61616"));
        auto_ptr<Connection> connection(factory->createConnection());
        connection->start();

        auto_ptr<Session> session(connection->createSession(Session::AUTO_ACKNOWLEDGE));
        
        string queue_name = "test_queue_" + to_string(receiver_id);
        auto_ptr<Destination> destination(session->createQueue(queue_name));
        auto_ptr<MessageProducer> producer(session->createProducer(NULL));
        producer->setDeliveryMode(DeliveryMode::NON_PERSISTENT);

        RequestListener listener(session.get(), producer.get(), receiver_id);
        auto_ptr<MessageConsumer> consumer(session->createConsumer(destination.get()));
        consumer->setMessageListener(&listener);

        cout << " [*] Receiver " << receiver_id << " waiting for messages on " << queue_name << endl;

        while (running) {
            this_thread::sleep_for(chrono::milliseconds(100));
        }

        cout << " [x] Receiver " << receiver_id << " shutting down" << endl;

    } catch (CMSException& e) {
        e.printStackTrace();
    }

    activemq::library::ActiveMQCPP::shutdownLibrary();
    return 0;
}
