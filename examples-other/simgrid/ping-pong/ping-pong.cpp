#include <random>
#include <stdlib.h>
#include <boost/format.hpp>
#include <simgrid/s4u.hpp>
#include <xbt/random.hpp>

namespace sg4 = simgrid::s4u;

XBT_LOG_NEW_DEFAULT_CATEGORY(ping_pong_app, "Ping-pong example");

enum class MessageType {
    START,
    PING,
    PONG,
    COMPLETED,
    STOP
};

class Message {
public:
    MessageType type;
    sg4::Mailbox* from = nullptr;

    explicit Message(MessageType type, sg4::Mailbox* from) : type(type), from(from) {}

    static void destroy(void* message);
};

void Message::destroy(void* message) {
    delete static_cast<Message*>(message);
}

static void root(sg4::Mailbox* in, std::vector<sg4::Mailbox*> process_mailboxes, bool asymmetric) {
    long active_proc_count = process_mailboxes.size();
    for(auto const& mailbox: process_mailboxes) {
        auto* start = new Message(MessageType::START, in);
        mailbox->put_init(start, 1)->detach(Message::destroy);
    }
    if (!asymmetric) {
        while (active_proc_count > 0) {
            auto* msg = in->get<Message>();
            xbt_assert(msg->type == MessageType::COMPLETED);
            delete msg;
            active_proc_count -= 1;
        }
        for(auto const& mailbox: process_mailboxes) {
            auto* stop = new Message(MessageType::STOP, in);
            mailbox->put_init(stop, 1)->detach(Message::destroy);
        }
    }
}

static void process(long id, sg4::Mailbox* in, std::vector<sg4::Mailbox*> peers, long iterations) {
    // in->set_receiver(sg4::Actor::self()); // has negative effect on performance
    simgrid::xbt::random::XbtRandom random;
    random.set_seed(id);

    // wait for Start message
    auto* msg = in->get<Message>();
    xbt_assert(msg->type == MessageType::START);
    sg4::Mailbox* root = msg->from;
    delete msg;
    XBT_DEBUG("Started");

    long peer_count = peers.size();
    long pings_to_send = iterations;
    bool wait_reply = false;
    bool stopped = false;
    while (!stopped) {
        if (pings_to_send > 0 && !wait_reply) {
            sg4::Mailbox* out = (peer_count == 1) ? peers[0] : peers[random.uniform_int(0, peer_count-1)];
            auto* ping = new Message(MessageType::PING, in);
            out->put_init(ping, 10)->detach(Message::destroy); // out->put_async is very slow
            XBT_DEBUG("Sent PING");
            pings_to_send -= 1;
            wait_reply = true;
        }

        msg = in->get<Message>();
        if (msg->type == MessageType::PING) {
            XBT_DEBUG("Received PING");
            auto* pong = new Message(MessageType::PONG, in);
            msg->from->put_init(pong, 10)->detach(Message::destroy); // out->put_async is very slow
            XBT_DEBUG("Sent PONG");
        } else if (msg->type == MessageType::PONG) {
            XBT_DEBUG("Received PONG");
            wait_reply = false;
            if (pings_to_send == 0) {
                XBT_DEBUG("Completed");
                auto* completed = new Message(MessageType::COMPLETED, in);
                root->put(completed, 1);
            }
        } else if (msg->type == MessageType::STOP) {
            XBT_DEBUG("Received STOP");
            stopped = true;
        }
        delete msg;
    }
    // in->set_receiver(nullptr);
    xbt_assert(pings_to_send == 0);
    XBT_DEBUG("Stopped");
}

static void process_asymmetric(bool is_pinger, sg4::Mailbox* in, sg4::Mailbox* out, long iterations) {
    // wait for Start message
    auto* msg = in->get<Message>();
    xbt_assert(msg->type == MessageType::START);
    delete msg;
    XBT_DEBUG("Started");

    while (iterations > 0) {
        if (is_pinger) {
            auto* ping = new Message(MessageType::PING, in);
            out->put(ping, 10);
            XBT_DEBUG("Sent PING");
            auto* pong = in->get<Message>();
            XBT_DEBUG("Received PONG");
            delete pong;
            iterations -= 1;
        } else {
            auto* ping = in->get<Message>();
            XBT_DEBUG("Received PING");
            auto* pong = new Message(MessageType::PONG, in);
            ping->from->put(pong, 10);
            XBT_DEBUG("Sent PONG");
            delete ping;
            iterations -= 1;
        }
    }
}

int main(int argc, char* argv[]) {
    sg4::Engine e(&argc, argv);
    simgrid::xbt::random::XbtRandom random;
    random.set_seed(123);

    xbt_assert(argc == 7, "Usage: %s PROC_COUNT PEER_COUNT ASYMMETRIC DISTRIBUTED ITERATIONS platform_file.xml", argv[0]);
    long proc_count = strtol(argv[1], NULL, 10);
    long peer_count = strtol(argv[2], NULL, 10);
    bool asymmetric = strtol(argv[3], NULL, 10);
    bool distributed = strtol(argv[4], NULL, 10);
    long iterations = strtol(argv[5], NULL, 10);
    xbt_assert(peer_count > 0, "PEER_COUNT should be positive");
    xbt_assert(iterations > 0, "ITERATIONS should be positive");
    xbt_assert(!asymmetric || proc_count % 2 == 0, "ASYMMETRIC case is supported only for even PROC_COUNT");
    xbt_assert(!asymmetric || peer_count == 1, "ASYMMETRIC case is supported only for PEER_COUNT=1");
    e.load_platform(argv[6]);

    std::vector<std::string> process_names;
    std::vector<sg4::Mailbox*> process_mailboxes;
    for (auto i = 1; i <= proc_count; i++) {
        auto proc_name = (boost::format("proc%1%") % i).str();
        process_names.push_back(proc_name);
        process_mailboxes.push_back(sg4::Mailbox::by_name(proc_name));
    }
    sg4::Actor::create(
        "root",
        sg4::Host::by_name("host1"),
        root,
        sg4::Mailbox::by_name("root"),
        process_mailboxes,
        asymmetric
    );
    for (auto i = 1; i <= proc_count; i++) {
        auto host_name = distributed ? (boost::format("host%1%") % (2 - i % 2)).str() : "host1";
        std::vector<sg4::Mailbox*> peers;
        if (peer_count == 1) {
            auto peer_id = i % proc_count + 1;
            peers.push_back(process_mailboxes[peer_id-1]);
        } else {
             while (peers.size() < peer_count) {
                auto peer_id = random.uniform_int(1, proc_count);
                if (peer_id != i) {
                    peers.push_back(process_mailboxes[peer_id-1]);
                }
            }
        }
        if (asymmetric) {
            bool is_pinger = i % 2;
            sg4::Mailbox* out = peers[0];
            sg4::Actor::create(
                process_names[i-1],
                sg4::Host::by_name(host_name),
                process_asymmetric,
                is_pinger,
                process_mailboxes[i-1],
                out,
                iterations
            );
        } else {
            sg4::Actor::create(
                process_names[i-1],
                sg4::Host::by_name(host_name),
                process,
                i,
                process_mailboxes[i-1],
                peers,
                iterations
            );
        }
    }

    auto start = std::chrono::steady_clock::now();
    e.run();
    auto stop = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
    XBT_INFO("Elapsed time: %d ms", duration);
    if (duration > 0) {
        auto ips = iterations * 1000 / duration;
        XBT_INFO("Iterations per second: %d", ips);
    }

    return 0;
}
