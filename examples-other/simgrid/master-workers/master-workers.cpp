#include <unordered_set>

#include <simgrid/s4u.hpp>
#include <xbt/random.hpp>

#include "common.h"
#include "master.h"
#include "worker.h"
#include "client.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(main, "Main");

int main(int argc, char* argv[]) {
    sg4::Engine e(&argc, argv);
    // use simple network config
    sg4::Engine::set_config("network/TCP-gamma:0");
    sg4::Engine::set_config("network/latency-factor:1");
    sg4::Engine::set_config("network/bandwidth-factor:1");
    sg4::Engine::set_config("network/weight-S:0.0");
    // disabling cross-traffic significantly improves simulation speed for large cases
    sg4::Engine::set_config("network/crosstraffic:0");
    simgrid::xbt::random::XbtRandom random(123);

    xbt_assert(argc == 3, "Usage: %s HOST_COUNT TASK_COUNT", argv[0]);
    uint32_t host_count = std::stoi(argv[1]);
    uint32_t task_count = std::stoi(argv[2]);

    // build platform and create actors
    auto* zone = sg4::create_full_zone("net");
    sg4::Mailbox* master_mailbox = sg4::Mailbox::by_name("master");
    double scheduling_time = 0;
    for (uint32_t i = 0; i < host_count; i++) {
        std::string hostname = "host-" + std::to_string(i);
        double speed = random.uniform_int(1, 10);
        int cores = random.uniform_int(1, 8);
        double memory = random.uniform_int(1, 4) * 1024;
        auto host = zone->create_host(hostname, speed);
        host->set_core_count(cores);
        auto disk = host->create_disk(hostname + "-fs", "1GBps", "1GBps");
        disk->set_property("size", "1000GiB");
        disk->set_property("mount", "/");
        // loopback link is used for intra-host communications
        const sg4::Link* loopback = zone->create_link(hostname + "-loopback", "100GBps")
                                        ->set_sharing_policy(sg4::Link::SharingPolicy::FATPIPE)
                                        ->set_latency(0)
                                        ->seal();
        zone->add_route(host->get_netpoint(), host->get_netpoint(), nullptr, nullptr,
                        {sg4::LinkInRoute(loopback)});
        if (i == 0) {
            sg4::Actor::create("master", host, Master("master", task_count, true, scheduling_time));
            sg4::Actor::create("client", host,
                               Client("client", task_count, master_mailbox, &random));
        }
        std::string worker_name = "worker-" + std::to_string(i);
        sg4::Actor::create(worker_name, host,
                           Worker(worker_name, speed, cores, memory, true, master_mailbox,
                                  e.host_by_name("host-0")));
    }
    // single backbone link is used for inter-host communication
    const sg4::Link* link =
        zone->create_link("backbone", "10GBps")
            ->set_sharing_policy(sg4::Link::SharingPolicy::FATPIPE)  // transfers use full bandwidth
            //->set_sharing_policy(sg4::Link::SharingPolicy::SHARED) // transfers share bandwidth
            ->set_latency("10us")
            ->seal();
    sg4::LinkInRoute backbone(link);
    auto master_host = e.host_by_name("host-0");
    for (uint32_t i = 1; i < host_count; i++) {
        std::string host = "host-" + std::to_string(i);
        zone->add_route(master_host->get_netpoint(), e.host_by_name(host)->get_netpoint(), nullptr,
                        nullptr, {backbone});
    }
    zone->seal();

    // run simulation
    auto start = std::chrono::steady_clock::now();
    e.run();
    auto stop = std::chrono::steady_clock::now();
    auto duration =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count()) /
        1000;
    printf("Processed %d tasks on %d hosts in %.2fs (%.2f tasks/s)\n", task_count, host_count,
           e.get_clock(), task_count / e.get_clock());
    printf("Elapsed time: %.2fs\n", duration);
    printf("Scheduling time: %.2fs\n", scheduling_time);
    printf("Simulation speedup: %.2f\n", e.get_clock() / duration);
}
