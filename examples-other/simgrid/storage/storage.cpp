#include "disk.h"

#include <argparse/argparse.hpp>

#include <xbt/log.h>
#include <simgrid/s4u.hpp>

#include <random>
#include <iostream>

using dslab::simgrid_examples::DiskWrapper;
namespace sg4 = simgrid::s4u;

static constexpr uint64_t kReadBw = 100;
static constexpr uint64_t kWriteBw = 100;

static constexpr uint64_t kDefaultActivitiesCount = 1;
static constexpr uint64_t kDefaultMaxSize = 1e9 + 6;
static constexpr uint64_t kDefaultMaxDelay = 0;

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
    XBT_WARN("Starting");
    auto start_time = std::chrono::steady_clock::now();
    f();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start_time)
                       .count();
    XBT_WARN("Done. Elapsed %zu ms", static_cast<size_t>(elapsed));
}

int main(int argc, char** argv) {
    sg4::Engine e(&argc, argv);

    argparse::ArgumentParser parser("simulator");

    parser.add_argument("--activities")
        .help("Number of activities (>= 1)")
        .nargs(1)
        .action([](const std::string& value) { return static_cast<uint64_t>(std::stoull(value)); })
        .default_value(kDefaultActivitiesCount);

    parser.add_argument("--max-size")
        .help("Maximal size (>= 1)")
        .nargs(1)
        .action([](const std::string& value) { return static_cast<uint64_t>(std::stoull(value)); })
        .default_value(kDefaultMaxSize);

    parser.add_argument("--max-delay")
        .help("Maximal delay between activities start (0 by default, so all will start at 0)")
        .nargs(1)
        .action([](const std::string& value) { return static_cast<uint64_t>(std::stoull(value)); })
        .default_value(kDefaultMaxDelay);

    uint64_t activities_count = kDefaultActivitiesCount, max_size = kDefaultMaxSize,
             max_delay = kDefaultMaxDelay;
    try {
        parser.parse_args(argc, argv);

        activities_count = parser.get<uint64_t>("--activities");
        max_size = parser.get<uint64_t>("--max-size");
        max_delay = parser.get<uint64_t>("--max-delay");
    } catch (const std::runtime_error& re) {
        std::cerr << "Argument parse error: " << re.what() << "\n";
        std::cerr << parser << "\n";
        std::exit(1);
    }

    auto* zone = sg4::create_full_zone("sample_zone");
    auto* host = zone->create_host("sample_host", 1e6);

    auto dt = MakeSimpleDisk(host);

    zone->seal();
    sg4::Actor::create("sample_actor", host,
                       [&]() { dt->Run(activities_count, max_size, max_delay); });

    RunWithTimeMeasure([&e] { e.run(); });
}
