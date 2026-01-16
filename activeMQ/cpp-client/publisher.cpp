// Simple publisher adapted from ActiveMQ examples
#include <activemq/util/Config.h>

#include <decaf/lang/System.h>
#include <decaf/lang/Integer.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <activemq/library/ActiveMQCPP.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/Destination.h>
#include <cms/MessageProducer.h>
#include <cms/TextMessage.h>
#include <iostream>

using namespace cms;
using namespace activemq;
using namespace activemq::core;
using namespace decaf::lang;

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
    std::string destination = argc > 1 ? argv[1] : "test";

    ActiveMQConnectionFactory factory;
    factory.setBrokerURI(std::string("tcp://") + host + ":" + Integer::toString(port));

    std::auto_ptr<Connection> connection(factory.createConnection(user, password));
    connection->start();

    std::auto_ptr<Session> session(connection->createSession());
    std::auto_ptr<Destination> dest(session->createTopic(destination));
    std::auto_ptr<MessageProducer> producer(session->createProducer(dest.get()));

    producer->setDeliveryMode(DeliveryMode::NON_PERSISTENT);

    std::auto_ptr<TextMessage> message;
    message.reset(session->createTextMessage("Hello from C++ publisher"));
    producer->send(message.get());

    std::cout << "Sent message to topic: " << destination << std::endl;

    connection->close();
    activemq::library::ActiveMQCPP::shutdownLibrary();
    return 0;
}
