#include <boost/format.hpp>
#include <simgrid/s4u.hpp>
#include <xbt/random.hpp>

#include "process.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(main, "Main");

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
