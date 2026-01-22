/**
 * ActiveMQ Async Sender with targeted routing.
 * Uses std::async to send messages concurrently.
 */
#include <activemq/library/ActiveMQCPP.h>
#include <decaf/lang/Thread.h>
#include <decaf/lang/Runnable.h>
#include <decaf/util/concurrent/CountDownLatch.h>
#include <decaf/lang/Integer.h>
#include <decaf/lang/Long.h>
#include <decaf/lang/System.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <activemq/util/Config.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/TextMessage.h>
#include <cms/BytesMessage.h>
#include <cms/MapMessage.h>
#include <cms/ExceptionListener.h>
#include <cms/MessageListener.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <memory>
#include <string>
#include <fstream>
#include <vector>
#include <future>
#include "../../include/json.hpp"
#include "../../include/stats_collector.hpp"

using namespace activemq::core;
using namespace decaf::util::concurrent;
using namespace decaf::lang;
using namespace cms;
using namespace std;
using json = nlohmann::json;

struct TaskResult {
    bool success;
    long long duration;
    string message_id;
    string error;
};

class AsyncSimpleResponseListener : public MessageListener {
public:
    string response;
    string corr_id;
    CountDownLatch latch;

    AsyncSimpleResponseListener() : latch(1) {}

    virtual void onMessage(const Message* message) {
        if (message->getCMSCorrelationID() == corr_id) {
            const TextMessage* textMessage = dynamic_cast<const TextMessage*>(message);
            if (textMessage != NULL) {
                response = textMessage->getText();
            }
            latch.countDown();
        }
    }
};

TaskResult send_message_task(ConnectionFactory* factory, json item) {
    TaskResult res;
    res.message_id = item["message_id"];
    res.success = false;
    res.duration = 0;

    int target = item.value("target", 0);
    string queue_name = "test_queue_" + to_string(target);
    string body = item.dump();
    string corr_id = "cpp-async-" + to_string(get_current_time_ms()) + "-" + res.message_id;

    try {
        unique_ptr<Connection> connection(factory->createConnection());
        connection->start();

        unique_ptr<Session> session(connection->createSession(Session::AUTO_ACKNOWLEDGE));
        
        string reply_queue_name = "reply_" + corr_id;
        unique_ptr<Queue> reply_dest(session->createQueue(reply_queue_name));
        
        AsyncSimpleResponseListener listener;
        listener.corr_id = corr_id;
        unique_ptr<MessageConsumer> consumer(session->createConsumer(reply_dest.get()));
        consumer->setMessageListener(&listener);

        unique_ptr<Queue> dest(session->createQueue(queue_name));
        unique_ptr<MessageProducer> producer(session->createProducer(dest.get()));

        long long msg_start = get_current_time_ms();
        unique_ptr<TextMessage> message(session->createTextMessage(body));
        message->setCMSReplyTo(reply_dest.get());
        message->setCMSCorrelationID(corr_id);
        
        producer->send(message.get());

        if (listener.latch.await(5000)) {
            json resp_data = json::parse(listener.response);
            if (resp_data["status"] == "ACK" && resp_data["message_id"] == res.message_id) {
                res.duration = get_current_time_ms() - msg_start;
                res.success = true;
            } else {
                res.error = "Unexpected response: " + listener.response;
            }
        } else {
            res.error = "Timeout";
        }

        connection->stop();
    } catch (CMSException& e) {
        res.error = e.getMessage();
    } catch (exception& e) {
        res.error = e.what();
    }
    return res;
}

int main() {
    activemq::library::ActiveMQCPP::initializeLibrary();

    string brokerURI = "failover:(tcp://localhost:61613)";
    unique_ptr<ActiveMQConnectionFactory> factory(new ActiveMQConnectionFactory(brokerURI));

    ifstream f("../../test_data.json");
    if (!f.is_open()) {
        f.open("test_data.json");
    }
    if (!f.is_open()) {
        cerr << "Could not open test_data.json" << endl;
        return 1;
    }
    json test_data = json::parse(f);

    cout << " [x] Starting ActiveMQ ASYNC Sender (C++)" << endl;
    cout << " [x] Starting async transfer of " << test_data.size() << " messages..." << endl;

    MessageStats stats;
    long long global_start = get_current_time_ms();

    vector<future<TaskResult>> futures;
    for (auto& item : test_data) {
        futures.push_back(async(launch::async, send_message_task, factory.get(), item));
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

    long long global_end = get_current_time_ms();
    stats.set_duration(global_start, global_end);
    
    json report = stats.get_stats();
    report["service"] = "ActiveMQ";
    report["language"] = "C++";
    report["async"] = true;

    cout << "\nTest Results (ASYNC):" << endl;
    cout << "total_sent: " << stats.sent_count << endl;
    cout << "total_received: " << stats.received_count << endl;
    cout << "duration_ms: " << stats.get_duration_ms() << endl;

    ofstream rf("report.txt", ios::app);
    if (rf.good()) {
        rf << report.dump() << endl;
        rf.close();
    }

    activemq::library::ActiveMQCPP::shutdownLibrary();
    return 0;
}
