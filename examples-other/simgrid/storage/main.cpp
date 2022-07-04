#include "disk.h"
#include "random.h"

#include <argparse/argparse.hpp>

#include <xbt/log.h>
#include <xbt/asserts.h>

#include <simgrid/s4u.hpp>
#include <simgrid/kernel/Timer.hpp>

#include <random>
#include <iostream>

using dslab::simgrid_examples::DisksSuit;
namespace sg4 = simgrid::s4u;

static constexpr uint64_t kReadBw = 100;
static constexpr uint64_t kWriteBw = 100;

static constexpr uint64_t kDefaultActivitiesCount = 1;
static constexpr uint64_t kDefaultDisksCount = 1;
static constexpr uint64_t kDefaultMaxSize = 1e9 + 6;
static constexpr uint64_t kDefaultMaxStartTime = 0;

XBT_LOG_NEW_DEFAULT_CATEGORY(disk_test, "Disk example");

namespace {

std::unique_ptr<DisksSuit> MakeSimpleDisks(sg4::Host* host, uint64_t count) {
    auto suit = std::make_unique<DisksSuit>(host, "simple-disk", kReadBw, kWriteBw);
    suit->MakeDisks(count);
    return suit;
}

[[maybe_unused]] std::unique_ptr<DisksSuit> MakeDisksWithDegradation(sg4::Host* host,
                                                                     uint64_t count) {
    auto suit = std::make_unique<DisksSuit>(host, "dedrading-disk", kReadBw, kWriteBw);
    suit->SetReadCapacityDegradation([]([[maybe_unused]] double capacity, int n_activities) {
        if (n_activities > 1000) {
            return capacity / 2;
        }
        return capacity;
    });
    suit->MakeDisks(count);
    return suit;
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

struct DiskReadRequest {
    DiskReadRequest(uint64_t disk_idx, uint64_t start_time, uint64_t size)
        : disk_idx(disk_idx), start_time(start_time), size(size) {
    }

    uint64_t disk_idx, start_time, size;
};

std::vector<DiskReadRequest> GeneratePlan(uint64_t disks_count, uint64_t activities_count,
                                          uint64_t max_size, uint64_t max_start_time) {
    CustomRandom rnd(16);

    std::vector<DiskReadRequest> plan;
    plan.reserve(activities_count);

    // For technical reasons first activity should start at time = 0
    uint64_t first_disk_idx = rnd.Next() % disks_count, first_size = rnd.Next() % (max_size + 1);
    plan.emplace_back(first_disk_idx, 0, first_size);

    for (size_t i = 0; i < activities_count - 1; ++i) {
        uint64_t disk_idx = rnd.Next() % disks_count, start_time = rnd.Next() % (max_start_time + 1),
                 size = rnd.Next() % (max_size + 1);
        plan.emplace_back(disk_idx, start_time, size);
    }

    return plan;
}

}  // namespace

int main(int argc, char** argv) {
    sg4::Engine e(&argc, argv);

    argparse::ArgumentParser parser("simulator");

    parser.add_argument("--activities")
        .help("Number of activities (>= 1)")
        .nargs(1)
        .action([](const std::string& value) { return static_cast<uint64_t>(std::stoull(value)); })
        .default_value(kDefaultActivitiesCount);

    parser.add_argument("--disks")
        .help("Number of disks (>= 1)")
        .nargs(1)
        .action([](const std::string& value) { return static_cast<uint64_t>(std::stoull(value)); })
        .default_value(kDefaultDisksCount);

    parser.add_argument("--max-size")
        .help("Maximal size (>= 1)")
        .nargs(1)
        .action([](const std::string& value) { return static_cast<uint64_t>(std::stoull(value)); })
        .default_value(kDefaultMaxSize);

    parser.add_argument("--max-start-time")
        .help("Maximal activity start time (0 by default, so all will start at 0)")
        .nargs(1)
        .action([](const std::string& value) { return static_cast<uint64_t>(std::stoull(value)); })
        .default_value(kDefaultMaxStartTime);

    uint64_t activities_count = kDefaultActivitiesCount, disks_count = kDefaultDisksCount,
             max_size = kDefaultMaxSize, max_start_time = kDefaultMaxStartTime;
    try {
        parser.parse_args(argc, argv);

        activities_count = parser.get<uint64_t>("--activities");
        disks_count = parser.get<uint64_t>("--disks");
        max_size = parser.get<uint64_t>("--max-size");
        max_start_time = parser.get<uint64_t>("--max-start-time");
    } catch (const std::runtime_error& re) {
        std::cerr << "Argument parse error: " << re.what() << "\n";
        std::cerr << parser << "\n";
        std::exit(1);
    }

    auto* zone = sg4::create_full_zone("sample_zone");
    auto* host = zone->create_host("sample_host", 1e6);

    auto disks_suit = MakeSimpleDisks(host, disks_count);

    zone->seal();

    sg4::Actor::create("runner", host, [&]() {
        XBT_INFO("Starting disk benchmark");

        std::vector<sg4::ActivityPtr> activities;
        activities.reserve(activities_count);

        std::vector<size_t> activities_order_remapping;
        activities_order_remapping.reserve(activities_count);

        auto plan = GeneratePlan(disks_count, activities_count, max_size, max_start_time);

        // For technical reasons first activity should start at time = 0
        xbt_assert(!plan.empty());
        xbt_assert(plan[0].start_time == 0);
        activities.push_back(disks_suit->ReadAsync(plan[0].disk_idx, plan[0].size));
        activities_order_remapping.push_back(0);

        // Other activities are pushed by timer
        for (size_t i = 1; i < activities_count; ++i) {
            simgrid::kernel::timer::Timer::set(plan[i].start_time, [&, i]() {
                XBT_INFO("Starting read with from disk-%lu, size = %lu", plan[i].disk_idx,
                         plan[i].size);
                activities.push_back(disks_suit->ReadAsync(plan[i].disk_idx, plan[i].size));
                activities_order_remapping.push_back(i);
            });
        }

        for (size_t i = 0; i < activities_count; ++i) {
            size_t finished_idx = sg4::Activity::wait_any(activities);
            const auto& request = plan[activities_order_remapping[finished_idx]];
            XBT_INFO("Completed reading from disk-%lu, size = %lu", request.disk_idx, request.size);

            std::swap(activities[finished_idx], activities.back());
            activities.pop_back();
            std::swap(activities_order_remapping[finished_idx], activities_order_remapping.back());
            activities_order_remapping.pop_back();
        }
    });

    RunWithTimeMeasure([&e] { e.run(); });
}
