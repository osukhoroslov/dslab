#include <unordered_set>

#include <simgrid/s4u.hpp>
#include <xbt/random.hpp>

#include <boost/format.hpp>
#include <simgrid/s4u.hpp>
#include <xbt/random.hpp>

#include <simgrid/forward.h>

#include <vector>

XBT_LOG_NEW_DEFAULT_CATEGORY(network, "Network");

enum class MessageType { START, DATA, DATA_RECEIVED, COMPLETED };

namespace sg4 = simgrid::s4u;

struct Message {
    MessageType type;
    sg4::Mailbox* from = nullptr;

    explicit Message(MessageType type, sg4::Mailbox* from)
        : type(type), from(from) {
    }

    static void Destroy(void* message) {
        delete static_cast<Message*>(message);
    }
};

void Root(sg4::Mailbox* in, std::vector<sg4::Mailbox*> process_mailboxes) {
    in->set_receiver(sg4::Actor::self());
    int active_proc_count = process_mailboxes.size();
    for (auto const& mailbox : process_mailboxes) {
        auto* start = new Message(MessageType::START, in);
        mailbox->put_init(start, 1)->detach(Message::Destroy);
    }
    while (active_proc_count > 0) {
        auto* msg = in->get<Message>();
        xbt_assert(msg->type == MessageType::COMPLETED);
        XBT_INFO("Received COMPLETED");
        delete msg;
        --active_proc_count;
    }
}

std::mt19937 rng(123);

void Process(int id, sg4::Mailbox* in, std::vector<sg4::Mailbox*> peers) {
    in->set_receiver(sg4::Actor::self());
    simgrid::xbt::random::XbtRandom random;
    random.set_seed(id);

    // wait for Start message
    auto* msg = in->get<Message>();
    xbt_assert(msg->type == MessageType::START);
    sg4::Mailbox* root = msg->from;
    delete msg;
    XBT_INFO("Started");

    int done = 0;
    int acks_left = peers.size();

    std::vector<std::pair<int, sg4::Mailbox*>> peers_delay;
    for (auto peer : peers) {
        peers_delay.emplace_back(random.uniform_real(0, 10), peer);
    }

    std::sort(peers_delay.begin(), peers_delay.end(), [](const auto& a, const auto& b) {
        return a.first < b.first;
    });

    for (const auto& [send_time, peer] : peers_delay) {
        sg4::this_actor::sleep_until(send_time);
        auto* data = new Message(MessageType::DATA, in);
        peer->put_init(data, random.uniform_real(1, 1000) * 1'000'000)
            ->detach(Message::Destroy);  // out->put_async is very slow
        XBT_INFO("Sent DATA");
    }

    while (acks_left) {
        auto msg = in->get<Message>();
        if (msg->type == MessageType::DATA) {
            XBT_INFO("Received DATA");
            auto* data_received = new Message(MessageType::DATA_RECEIVED, in);
            msg->from->put_init(data_received, 0)
                ->detach(Message::Destroy);  // out->put_async is very slow
            XBT_INFO("Sent DATA_RECEIVED");
        } else if (msg->type == MessageType::DATA_RECEIVED) {
            XBT_INFO("Received DATA_RECEIVED");
            --acks_left;
            if (acks_left == 0) {
                XBT_INFO("Completed");
                auto* completed = new Message(MessageType::COMPLETED, in);
                root->put(completed, 1);
                break;
            }
        }
        delete msg;
    }
    xbt_assert(acks_left == 0);
    XBT_INFO("Stopped");
}

void make_full_mesh_topology(sg4::NetZone* zone, int host_count) {
    for (int i = 0; i < host_count; ++i) {
        for (int j = 0; j <= i; ++j) {
            if (i == j && i != 0) continue;
            sg4::LinkInRoute link{zone->create_link("link-" + std::to_string(i) + "-" + std::to_string(j), "1000MBps")->set_latency(1e-4)->set_sharing_policy(sg4::Link::SharingPolicy::SHARED)};
            zone->add_route(
                sg4::Host::by_name("host-" + std::to_string(i))->get_netpoint(),
                sg4::Host::by_name("host-" + std::to_string(j))->get_netpoint(),
                nullptr, nullptr, {link}, false);
            if (i != j) {
                zone->add_route(
                    sg4::Host::by_name("host-" + std::to_string(j))->get_netpoint(),
                    sg4::Host::by_name("host-" + std::to_string(i))->get_netpoint(),
                    nullptr, nullptr, {link}, false);
            }
        }
    }
}

void make_star_topology(sg4::NetZone* zone, int host_count) {
    std::vector<sg4::Link*> links;
    for (uint32_t i = 0; i < host_count; i++) {
        auto link = zone->create_link("link-" + std::to_string(i), "1000MBps")->set_latency(1e-4)->set_sharing_policy(sg4::Link::SharingPolicy::SHARED);
        links.push_back(link);
    }

    for (int i = 0; i < host_count; ++i) {
        for (int j = 0; j < host_count; ++j) {
            if (i == j && i != 0) continue;
            sg4::LinkInRoute a{links[i]};
            sg4::LinkInRoute b{links[j]};
            zone->add_route(
                sg4::Host::by_name("host-" + std::to_string(i))->get_netpoint(),
                sg4::Host::by_name("host-" + std::to_string(j))->get_netpoint(),
                nullptr, nullptr, {a, b}, false);
        }
    }
}

void make_tree_topology(sg4::NetZone* zone, int star_count, int hosts_per_star) {
    int host_count = star_count * hosts_per_star;

    std::vector<sg4::Link*> star_links;
    for (uint32_t i = 0; i < star_count; i++) {
        auto link = zone->create_link("link-" + std::to_string(i), std::to_string(1000 * hosts_per_star) + "MBps")->set_latency(1e-4)->set_sharing_policy(sg4::Link::SharingPolicy::SHARED);
        star_links.push_back(link);
    }
    std::vector<sg4::Link*> host_links;
    for (uint32_t i = 0; i < hosts_per_star * star_count; i++) {
        auto link = zone->create_link("link-host-" + std::to_string(i), "1000MBps")->set_latency(1e-4)->set_sharing_policy(sg4::Link::SharingPolicy::SHARED);
        host_links.push_back(link);
    }

    for (int i = 0; i < host_count; ++i) {
        for (int j = 0; j < host_count; ++j) {
            if (i == j && i != 0) continue;
            if (i / hosts_per_star == j / hosts_per_star) {
                sg4::LinkInRoute a{host_links[i]};
                sg4::LinkInRoute b{host_links[j]};
                zone->add_route(
                    sg4::Host::by_name("host-" + std::to_string(i))->get_netpoint(),
                    sg4::Host::by_name("host-" + std::to_string(j))->get_netpoint(),
                    nullptr, nullptr, {a, b}, false);
            } else {
                sg4::LinkInRoute a{host_links[i]};
                sg4::LinkInRoute b{star_links[i / hosts_per_star]};
                sg4::LinkInRoute c{star_links[j / hosts_per_star]};
                sg4::LinkInRoute d{host_links[j]};
                zone->add_route(
                    sg4::Host::by_name("host-" + std::to_string(i))->get_netpoint(),
                    sg4::Host::by_name("host-" + std::to_string(j))->get_netpoint(),
                    nullptr, nullptr, {a, b, c, d}, false);
            }
        }
    }
}

int main(int argc, char* argv[]) {
    sg4::Engine e(&argc, argv);
    simgrid::xbt::random::XbtRandom random(123);

    xbt_assert(argc == 4 || argc == 3, "Usage: %s NET_TYPE HOST_COUNT [STAR_COUNT]", argv[0]);
    std::string net_type = argv[1];
    xbt_assert(net_type == "full_mesh" || net_type == "star" || net_type == "tree", "NET_TYPE has to be one of [full_mesh, star, tree]");
    if (net_type == "tree") {
        xbt_assert(argc == 4, "Usage: %s tree HOST_COUNT STAR_COUNT", argv[0]);
    } else {
        xbt_assert(argc == 3, "Usage: %s [full_mesh,star] HOST_COUNT", argv[0]);
    }

    uint32_t host_count = std::stoi(argv[2]);
    uint32_t star_count = argc == 4 ? std::stoi(argv[3]) : 0;

    auto* zone = sg4::create_full_zone("net");

    std::vector<std::string> process_names;
    std::vector<sg4::Mailbox*> process_mailboxes;
    for (unsigned int i = 0; i < host_count; i++) {
        auto proc_name = (boost::format("proc%1%") % i).str();
        process_names.push_back(proc_name);
        process_mailboxes.push_back(sg4::Mailbox::by_name(proc_name));
    }

    for (uint32_t i = 0; i < host_count; i++) {
        std::string hostname = "host-" + std::to_string(i);
        auto host = zone->create_host(hostname, 1);

        std::vector<sg4::Mailbox*> peers = process_mailboxes;
        peers.erase(peers.begin() + i);

        sg4::Actor::create(process_names[i], host, Process, i, process_mailboxes[i], peers);
    }

    sg4::Actor::create("root", sg4::Host::by_name("host-0"), Root, sg4::Mailbox::by_name("root"),
                       process_mailboxes);

    if (net_type == "full_mesh") {
        make_full_mesh_topology(zone, host_count);
    } else if (net_type == "star") {
        make_star_topology(zone, host_count);
    } else if (net_type == "tree") {
        make_tree_topology(zone, star_count, host_count / star_count);
    }

    auto start = std::chrono::steady_clock::now();
    e.run();
    auto stop = std::chrono::steady_clock::now();
    auto duration =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count()) /
        1000;
    std::cout << "duration: " << duration << "s" << std::endl;
}
