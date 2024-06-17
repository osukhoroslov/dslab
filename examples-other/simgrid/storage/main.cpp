#include "disk.h"
#include "random.h"

#include <argparse/argparse.hpp>

#include <xbt/log.h>
#include <xbt/asserts.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <simgrid/s4u.hpp>

#include <random>
#include <iostream>

using dslab::simgrid_examples::DisksSuite;
namespace sg4 = simgrid::s4u;

static constexpr uint64_t kReadBw = 100;
static constexpr uint64_t kWriteBw = 100;

static constexpr uint64_t kDefaultRequestsCount = 1;
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
    std::cout << "Starting" << std::endl;
    auto start_time = std::chrono::steady_clock::now();
    f();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start_time)
                       .count();
    std::cout << "Done. Elapsed " << static_cast<size_t>(elapsed) << " ms" << std::endl;
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
        .default_value(kDefaultRequestsCount);

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
        .help("Maximal request start time (0 by default, so all will start at 0)")
        .nargs(1)
        .action(str_to_ull)
        .default_value(kDefaultMaxStartTime);

    uint64_t requests_count = kDefaultRequestsCount, disks_count = kDefaultDisksCount,
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

    auto disks_suite = MakeSimpleDisks(host, disks_count);

    zone->seal();

    auto* mb = sg4::Mailbox::by_name("");

    auto requests = GenerateRequests(disks_count, requests_count, max_size, max_start_time);

    // Need to sort for sequential awaiting in `starter` actor
    std::sort(requests.begin(), requests.end(),
              [](const DiskReadRequest& lhs, const DiskReadRequest& rhs) {
                  return std::tie(lhs.start_time, lhs.disk_idx, lhs.size) <
                         std::tie(rhs.start_time, rhs.disk_idx, rhs.size);
              });

    // Starter notifies Runner to submit requests in specified time moments
    sg4::Actor::create("starter", host, [&] {
        for (const auto& req : requests) {
            sg4::this_actor::sleep_until(req.start_time);
            mb->put(new int, 0);
        }
    });

    // Runner submits requests to disks and logs request completion
    sg4::Actor::create("runner", host, [&] {
        XBT_INFO("Starting disk benchmark");

        size_t next_request_id = 0;
        std::vector<double> real_start_times;
        real_start_times.resize(requests_count);

        sg4::ActivitySet activities;
        int* dummy = nullptr;
        activities.push(mb->get_async<int>(&dummy));

        for (size_t i = 0; i < 2 * requests_count; ++i) {
            sg4::ActivityPtr completed = activities.wait_any();
            const std::string& completed_name = completed->get_name();
            if (completed_name == "unnamed") {
                // Time to submit next request
                auto& req = requests[next_request_id];

                sg4::IoPtr io = disks_suite->ReadAsync(req.disk_idx, req.size);
                io->set_name(std::to_string(next_request_id));
                activities.push(io);
                real_start_times[next_request_id] = sg4::Engine::get_clock();

                XBT_INFO(
                    "Starting request #%lu: read from disk-%lu, size = %lu, expected start time = "
                    "%.3f",
                    next_request_id, req.disk_idx, req.size,
                    static_cast<double>(req.start_time));
                ++next_request_id;

                activities.push(mb->get_async<int>(&dummy));
            } else {
                // Some request is completed
                std::stringstream ss(completed_name);
                size_t request_id;
                ss >> request_id;
                auto& req = requests[request_id];

                double elapsed_time = sg4::Engine::get_clock() - real_start_times[request_id];
                XBT_INFO(
                    "Completed request #%lu: read from disk-%lu, size = %lu, elapsed simulation "
                    "time = %.3f",
                    request_id, req.disk_idx, req.size, elapsed_time);
            }
        }
        XBT_INFO("Exit");
    });
    RunWithTimeMeasure([&e] { e.run(); });
}
