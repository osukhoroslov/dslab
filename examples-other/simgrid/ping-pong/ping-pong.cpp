#include <boost/format.hpp>
#include <simgrid/s4u.hpp>
#include <xbt/random.hpp>

namespace sg4 = simgrid::s4u;

XBT_LOG_NEW_DEFAULT_CATEGORY(ping_pong_app, "Ping-pong example");

static const int kMessagePayloadSize = 10;

enum class MessageType { START, PING, PONG, COMPLETED, STOP };

struct Message {
    MessageType type;
    double payload;  // current sender time is used as a message payload
    sg4::Mailbox* from = nullptr;

    explicit Message(MessageType type, double payload, sg4::Mailbox* from)
        : type(type), payload(payload), from(from) {
    }

    static void Destroy(void* message);
};

void Message::Destroy(void* message) {
    delete static_cast<Message*>(message);
}

static void Root(sg4::Mailbox* in, std::vector<sg4::Mailbox*> process_mailboxes, bool asymmetric) {
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

static void Process(int id, sg4::Mailbox* in, std::vector<sg4::Mailbox*> peers, int iterations) {
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

static void ProcessAsymmetric(bool is_pinger, sg4::Mailbox* in, sg4::Mailbox* out, int iterations) {
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

int main(int argc, char* argv[]) {
    sg4::Engine e(&argc, argv);
    // use simple network config
    sg4::Engine::set_config("network/latency-factor:1");
    sg4::Engine::set_config("network/bandwidth-factor:1");
    sg4::Engine::set_config("network/weight-S:0.0");
    // disabling cross-traffic significantly improves simulation speed for large cases
    sg4::Engine::set_config("network/crosstraffic:0");
    simgrid::xbt::random::XbtRandom random(123);

    xbt_assert(
        argc == 7,
        "Usage: %s PROC_COUNT PEER_COUNT ASYMMETRIC DISTRIBUTED ITERATIONS platform_file.xml",
        argv[0]);
    unsigned int proc_count = std::stoi(argv[1]);
    unsigned int peer_count = std::stoi(argv[2]);
    bool asymmetric = std::stoi(argv[3]);
    bool distributed = std::stoi(argv[4]);
    unsigned int iterations = std::stoi(argv[5]);
    xbt_assert(peer_count > 0, "PEER_COUNT should be positive");
    xbt_assert(iterations > 0, "ITERATIONS should be positive");
    xbt_assert(!asymmetric || proc_count % 2 == 0,
               "ASYMMETRIC case is supported only for even PROC_COUNT");
    xbt_assert(!asymmetric || peer_count == 1,
               "ASYMMETRIC case is supported only for PEER_COUNT=1");
    e.load_platform(argv[6]);

    std::vector<std::string> process_names;
    std::vector<sg4::Mailbox*> process_mailboxes;
    for (unsigned int i = 1; i <= proc_count; i++) {
        auto proc_name = (boost::format("proc%1%") % i).str();
        process_names.push_back(proc_name);
        process_mailboxes.push_back(sg4::Mailbox::by_name(proc_name));
    }
    sg4::Actor::create("root", sg4::Host::by_name("host1"), Root, sg4::Mailbox::by_name("root"),
                       process_mailboxes, asymmetric);
    for (unsigned int i = 1; i <= proc_count; i++) {
        auto host_name = distributed ? (boost::format("host%1%") % (2 - i % 2)).str() : "host1";
        std::vector<sg4::Mailbox*> peers;
        if (peer_count == 1) {
            auto peer_id = i % proc_count + 1;
            peers.push_back(process_mailboxes[peer_id - 1]);
        } else {
            while (peers.size() < peer_count) {
                unsigned int peer_id = random.uniform_int(1, proc_count);
                if (peer_id != i) {
                    peers.push_back(process_mailboxes[peer_id - 1]);
                }
            }
        }
        if (asymmetric) {
            bool is_pinger = i % 2;
            sg4::Mailbox* out = peers[0];
            sg4::Actor::create(process_names[i - 1], sg4::Host::by_name(host_name),
                               ProcessAsymmetric, is_pinger, process_mailboxes[i - 1], out,
                               iterations);
        } else {
            sg4::Actor::create(process_names[i - 1], sg4::Host::by_name(host_name), Process, i,
                               process_mailboxes[i - 1], peers, iterations);
        }
    }

    auto start = std::chrono::steady_clock::now();
    e.run();
    auto stop = std::chrono::steady_clock::now();
    auto duration =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count()) /
        1000;
    if (duration > 0) {
        printf("Processed %d iterations in %.2fs (%.2f iter/s)\n", iterations, duration,
               iterations / duration);
    }
}
