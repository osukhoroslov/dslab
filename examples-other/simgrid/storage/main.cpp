#include "disk.h"
#include "random.h"

#include <argparse/argparse.hpp>

#include <xbt/log.h>
#include <xbt/asserts.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <simgrid/s4u.hpp>
#include <simgrid/kernel/Timer.hpp>

#include <random>
#include <iostream>

using dslab::simgrid_examples::DisksSuite;
namespace sg4 = simgrid::s4u;

static constexpr uint64_t kReadBw = 100;
static constexpr uint64_t kWriteBw = 100;

static constexpr uint64_t kDefaultActivitiesCount = 1;
static constexpr uint64_t kDefaultDisksCount = 1;
static constexpr uint64_t kDefaultMaxSize = 1e9 + 6;
static constexpr uint64_t kDefaultMaxStartTime = 0;

XBT_LOG_NEW_DEFAULT_CATEGORY(disk_test, "Disk example");

namespace {

std::unique_ptr<DisksSuite> MakeSimpleDisks(sg4::Host* host, uint64_t count) {
    auto suit = std::make_unique<DisksSuite>(host, "simple-disk", kReadBw, kWriteBw);
    suit->MakeDisks(count);
    return suit;
}

[[maybe_unused]] std::unique_ptr<DisksSuite> MakeDisksWithDegradation(sg4::Host* host,
                                                                      uint64_t count) {
    auto suit = std::make_unique<DisksSuite>(host, "dedrading-disk", kReadBw, kWriteBw);
    suit->SetReadCapacityDegradation([]([[maybe_unused]] double capacity, int n_requests) {
        if (n_requests > 1000) {
            return capacity / 2;
        }
        return capacity;
    });
    suit->MakeDisks(count);
    return suit;
}

template <typename F>
void RunWithTimeMeasure(F&& f) {
    XBT_INFO("Starting");
    auto start_time = std::chrono::steady_clock::now();
    f();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start_time)
                       .count();
    XBT_INFO("Done. Elapsed %zu ms", static_cast<size_t>(elapsed));
}

struct DiskReadRequest {
    DiskReadRequest(uint64_t disk_idx, uint64_t start_time, uint64_t size)
        : disk_idx(disk_idx), start_time(start_time), size(size) {
    }

    uint64_t disk_idx, start_time, size;
};

std::vector<DiskReadRequest> GenerateRequests(uint64_t disks_count, uint64_t requests_count,
                                              uint64_t max_size, uint64_t max_start_time) {
    CustomRandom rnd(16);

    std::vector<DiskReadRequest> requests;
    requests.reserve(requests_count);

    for (size_t i = 0; i < requests_count; ++i) {
        uint64_t disk_idx = rnd.Next() % disks_count,
                 start_time = rnd.Next() % (max_start_time + 1), size = rnd.Next() % (max_size + 1);
        requests.emplace_back(disk_idx, start_time, size);
    }

    return requests;
}

}  // namespace

int main(int argc, char** argv) {
    sg4::Engine e(&argc, argv);

    argparse::ArgumentParser parser("simulator");

    auto str_to_ull = [](const std::string& value) {
        return static_cast<uint64_t>(std::stoull(value));
    };

    parser.add_argument("--requests")
        .help("Number of requests (>= 1)")
        .nargs(1)
        .action(str_to_ull)
        .default_value(kDefaultActivitiesCount);

    parser.add_argument("--disks")
        .help("Number of disks (>= 1)")
        .nargs(1)
        .action(str_to_ull)
        .default_value(kDefaultDisksCount);

    parser.add_argument("--max-size")
        .help("Maximal size (>= 1)")
        .nargs(1)
        .action(str_to_ull)
        .default_value(kDefaultMaxSize);

    parser.add_argument("--max-start-time")
        .help("Maximal activity start time (0 by default, so all will start at 0)")
        .nargs(1)
        .action(str_to_ull)
        .default_value(kDefaultMaxStartTime);

    uint64_t requests_count = kDefaultActivitiesCount, disks_count = kDefaultDisksCount,
             max_size = kDefaultMaxSize, max_start_time = kDefaultMaxStartTime;
    try {
        parser.parse_args(argc, argv);

        requests_count = parser.get<uint64_t>("--requests");
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

    auto* mb = sg4::Mailbox::by_name("");

    auto requests = GenerateRequests(disks_count, requests_count, max_size, max_start_time);

    // Need to sort for sequental awaiting in `starter` actor
    std::sort(requests.begin(), requests.end(),
              [](const DiskReadRequest& lhs, const DiskReadRequest& rhs) {
                  return std::tie(lhs.start_time, lhs.disk_idx, lhs.size) <
                         std::tie(rhs.start_time, rhs.disk_idx, rhs.size);
              });

    sg4::Actor::create("starter", host, [&] {
        for (const auto& req : requests) {
            simgrid::s4u::this_actor::sleep_until(req.start_time);
            mb->put(new int, 0);
        }
    });

    sg4::Actor::create("runner", host, [&] {
        XBT_INFO("Starting disk benchmark");

        std::vector<sg4::ActivityPtr> activities;
        int* dummy = nullptr;
        activities.push_back(mb->get_async<int>(&dummy));

        size_t next_activity_to_start = 0;
        std::vector<size_t> activities_to_requests;

        for (size_t i = 0; i < 2 * requests_count; ++i) {
            if (size_t finished_idx = sg4::Activity::wait_any(activities); finished_idx == 0) {
                // Time to run next disk activity
                auto& req = requests[next_activity_to_start];
                activities.emplace_back(disks_suit->ReadAsync(req.disk_idx, req.size));
                activities_to_requests.push_back(next_activity_to_start);
                ++next_activity_to_start;
                activities[0] = mb->get_async<int>(&dummy);
            } else {
                // Some disk activity completed
                auto& req = requests[activities_to_requests[finished_idx]];
                XBT_INFO("Completed reading from disk-%lu, size = %lu", req.disk_idx, req.size);
                std::swap(activities[finished_idx], activities.back());
                std::swap(activities_to_requests[finished_idx], activities_to_requests.back());
                activities.pop_back();
                activities_to_requests.pop_back();
            }
        }
        XBT_INFO("Exit");
    });

    RunWithTimeMeasure([&e] { e.run(); });
}
