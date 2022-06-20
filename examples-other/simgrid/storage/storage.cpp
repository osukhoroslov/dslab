#include "disk.h"

#include <xbt/log.h>
#include <simgrid/s4u.hpp>

#include <random>
#include <iostream>

using dslab::simgrid_examples::DiskWrapper;
namespace sg4 = simgrid::s4u;

static constexpr uint64_t kReadBw = 100;
static constexpr uint64_t kWriteBw = 100;
static constexpr uint64_t kActivitiesCount = 10000;

XBT_LOG_NEW_DEFAULT_CATEGORY(disk_test, "Disk example");

std::unique_ptr<DiskWrapper> MakeSimpleDisk(sg4::Host* host) {
    auto dt = std::make_unique<DiskWrapper>(host, "sample_disk", kReadBw, kWriteBw);
    dt->ApplyAndSeal();
    return dt;
}

std::unique_ptr<DiskWrapper> MakeDiskWithDegradation(sg4::Host* host) {
    auto dt = std::make_unique<DiskWrapper>(host, "sample_disk", kReadBw, kWriteBw);
    dt->SetReadCapacityDegradation([]([[maybe_unused]] double capacity, int n_activities) {
        if (n_activities > 1000) {
            return capacity / 2;
        }
        return capacity;
    });
    dt->ApplyAndSeal();
    return dt;
}

template <typename F>
void RunWithTimeMeasure(F&& f) {
    XBT_INFO("Starting");
    auto start_time = std::chrono::steady_clock::now();
    f();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start_time)
                       .count();
    XBT_INFO("Done. Elapsed %.10g ms", static_cast<double>(elapsed));
}

int main(int argc, char** argv) {
    sg4::Engine e(&argc, argv);

    auto* zone = sg4::create_full_zone("sample_zone");
    auto* host = zone->create_host("sample_host", 1e6);

    auto dt = MakeSimpleDisk(host);

    zone->seal();
    sg4::Actor::create("sample_actor", host, [&dt]() { dt->Run(kActivitiesCount); });

    RunWithTimeMeasure([&e] { e.run(); });
}
