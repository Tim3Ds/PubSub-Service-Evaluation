// Simple listener adapted from ActiveMQ examples
#include <activemq/library/ActiveMQCPP.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <decaf/lang/Integer.h>
#include <decaf/lang/System.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/Destination.h>
#include <cms/MessageConsumer.h>
#include <cms/TextMessage.h>
#include <iostream>

using namespace activemq;
using namespace activemq::core;
using namespace decaf::lang;
using namespace cms;

std::string getEnv(const std::string& key, const std::string& defaultValue) {
    try{ return System::getenv(key); } catch(...) {}
    return defaultValue;
}

int main(int argc, char* argv[]) {
    activemq::library::ActiveMQCPP::initializeLibrary();

    std::string user = getEnv("ACTIVEMQ_USER", "admin");
    std::string password = getEnv("ACTIVEMQ_PASSWORD", "password");
    std::string host = getEnv("ACTIVEMQ_HOST", "localhost");
    int port = Integer::parseInt(getEnv("ACTIVEMQ_PORT", "61616"));
    // Use a topic so all subscribers get the same message
    std::string destination = argc > 1 ? argv[1] : "/topic/test";

    ActiveMQConnectionFactory factory;
    factory.setBrokerURI(std::string("tcp://") + host + ":" + Integer::toString(port));

    std::auto_ptr<Connection> connection(factory.createConnection(user, password));
    std::auto_ptr<Session> session(connection->createSession());
    std::auto_ptr<Destination> dest(session->createTopic(destination));
    std::auto_ptr<MessageConsumer> consumer(session->createConsumer(dest.get()));

    connection->start();

    std::cout << "Waiting for messages on topic: " << destination << std::endl;
    while(true) {
        std::auto_ptr<Message> msg(consumer->receive());
        const TextMessage* txt = dynamic_cast<const TextMessage*>(msg.get());
        if(txt != NULL) {
            std::string body = txt->getText();
            std::cout << "Received: " << body << std::endl;
            if(body == "SHUTDOWN") break;
        }
    }

    connection->close();
    activemq::library::ActiveMQCPP::shutdownLibrary();
    return 0;
}
