#include "process.h"

#include <boost/format.hpp>
#include <simgrid/s4u.hpp>
#include <xbt/random.hpp>

XBT_LOG_NEW_DEFAULT_CATEGORY(ping_pong, "Ping-Pong");

void Message::Destroy(void* message) {
    delete static_cast<Message*>(message);
}

void Root(sg4::Mailbox* in, std::vector<sg4::Mailbox*> process_mailboxes, bool asymmetric) {
    in->set_receiver(sg4::Actor::self());
    int active_proc_count = process_mailboxes.size();
    for (auto const& mailbox : process_mailboxes) {
        auto* start = new Message(MessageType::START, sg4::Engine::get_clock(), in);
        mailbox->put_init(start, 1)->detach(Message::Destroy);
    }
    if (!asymmetric) {
        while (active_proc_count > 0) {
            auto* msg = in->get<Message>();
            xbt_assert(msg->type == MessageType::COMPLETED);
            XBT_INFO("Received COMPLETED");
            delete msg;
            --active_proc_count;
        }
        for (auto const& mailbox : process_mailboxes) {
            auto* stop = new Message(MessageType::STOP, sg4::Engine::get_clock(), in);
            mailbox->put_init(stop, 1)->detach(Message::Destroy);
            XBT_INFO("Sent STOP");
        }
    }
}

void Process(int id, sg4::Mailbox* in, std::vector<sg4::Mailbox*> peers, int iterations) {
    in->set_receiver(sg4::Actor::self());
    simgrid::xbt::random::XbtRandom random;
    random.set_seed(id);

    // wait for Start message
    auto* msg = in->get<Message>();
    xbt_assert(msg->type == MessageType::START);
    sg4::Mailbox* root = msg->from;
    delete msg;
    XBT_INFO("Started");

    unsigned int peer_count = peers.size();
    int pings_to_send = iterations;
    bool wait_reply = false;
    bool stopped = false;
    while (!stopped) {
        if (pings_to_send > 0 && !wait_reply) {
            // select ping target (avoiding calling random for single peer seems to give slight
            // speed improvement)
            sg4::Mailbox* out =
                (peer_count == 1) ? peers[0] : peers[random.uniform_int(0, peer_count - 1)];
            auto* ping = new Message(MessageType::PING, sg4::Engine::get_clock(), in);
            out->put_init(ping, kMessagePayloadSize)
                ->detach(Message::Destroy);  // out->put_async is very slow
            XBT_INFO("Sent PING");
            pings_to_send -= 1;
            wait_reply = true;
        }

        msg = in->get<Message>();
        if (msg->type == MessageType::PING) {
            XBT_INFO("Received PING");
            auto* pong = new Message(MessageType::PONG, sg4::Engine::get_clock(), in);
            msg->from->put_init(pong, kMessagePayloadSize)
                ->detach(Message::Destroy);  // out->put_async is very slow
            XBT_INFO("Sent PONG");
        } else if (msg->type == MessageType::PONG) {
            XBT_INFO("Received PONG");
            wait_reply = false;
            if (pings_to_send == 0) {
                XBT_INFO("Completed");
                auto* completed = new Message(MessageType::COMPLETED, sg4::Engine::get_clock(), in);
                root->put(completed, 1);
            }
        } else if (msg->type == MessageType::STOP) {
            XBT_INFO("Received STOP");
            stopped = true;
        }
        delete msg;
    }
    xbt_assert(pings_to_send == 0);
    XBT_INFO("Stopped");
}

void ProcessAsymmetric(bool is_pinger, sg4::Mailbox* in, sg4::Mailbox* out, int iterations) {
    in->set_receiver(sg4::Actor::self());
    // wait for Start message
    auto* msg = in->get<Message>();
    xbt_assert(msg->type == MessageType::START);
    delete msg;
    XBT_INFO("Started");

    while (iterations > 0) {
        if (is_pinger) {
            auto* ping = new Message(MessageType::PING, sg4::Engine::get_clock(), in);
            out->put(ping, kMessagePayloadSize);
            XBT_INFO("Sent PING");
            auto* pong = in->get<Message>();
            XBT_INFO("Received PONG");
            delete pong;
            iterations -= 1;
        } else {
            auto* ping = in->get<Message>();
            XBT_INFO("Received PING");
            auto* pong = new Message(MessageType::PONG, sg4::Engine::get_clock(), in);
            ping->from->put(pong, kMessagePayloadSize);
            XBT_INFO("Sent PONG");
            delete ping;
            --iterations;
        }
    }
}
