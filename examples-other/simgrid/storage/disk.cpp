#include "disk.h"
#include "random.h"

#include <simgrid/s4u.hpp>
#include <xbt/log.h>

XBT_LOG_NEW_DEFAULT_CATEGORY(disk_test_impl, "Disk tests");

namespace dslab::simgrid_examples {

DiskWrapper::DiskWrapper(sg4::Host* host, const std::string& name, double read_bw,
                         double write_bw) {
    disk_ = host->create_disk(name, read_bw, write_bw);
}

void DiskWrapper::SetReadCapacityDegradation(DegradationRule rule) {
    read_degradation_rule_ = std::move(rule);
}

void DiskWrapper::SetWriteCapacityDegradation(DegradationRule rule) {
    write_degradation_rule_ = std::move(rule);
}

void DiskWrapper::SetReadBandwidthFunction(BandwidthFunction bf) {
    read_bf_ = std::move(bf);
}

void DiskWrapper::SetWriteBandwidthFunction(BandwidthFunction bf) {
    write_bf_ = std::move(bf);
}

void DiskWrapper::ApplyAndSeal() {
    if (read_degradation_rule_) {
        disk_->set_sharing_policy(sg4::Disk::Operation::READ, sg4::Disk::SharingPolicy::NONLINEAR,
                                  read_degradation_rule_);
    } else {
        disk_->set_sharing_policy(sg4::Disk::Operation::READ, sg4::Disk::SharingPolicy::LINEAR);
    }

    if (write_degradation_rule_) {
        disk_->set_sharing_policy(sg4::Disk::Operation::WRITE, sg4::Disk::SharingPolicy::NONLINEAR,
                                  write_degradation_rule_);
    } else {
        disk_->set_sharing_policy(sg4::Disk::Operation::WRITE, sg4::Disk::SharingPolicy::LINEAR);
    }

    if (read_bf_ || write_bf_) {
        disk_->set_factor_cb([this](sg_size_t size, sg4::Io::OpType op) {
            if (op == sg4::Io::OpType::READ && read_bf_) {
                return read_bf_(size);
            } else if (op == sg4::Io::OpType::WRITE && write_bf_) {
                return write_bf_(size);
            }
            return 1.;
        });
    }

    disk_->seal();
}

void DiskWrapper::Run(size_t activities_count) {
    XBT_WARN("Starting disk benchmark");

    std::vector<sg4::IoPtr> activities;
    activities.reserve(activities_count);

    CustomRandom rnd(16);
    for (size_t i = 0; i < activities_count; ++i) {
        uint64_t size = rnd.Next();
        XBT_INFO("Starting read of size %lu", size);
        activities.push_back(disk_->read_async(size));
    }

    XBT_WARN("Started %lu activities. Waiting for complete...", activities_count);

    for (size_t i = 0; i < activities_count; ++i) {
        size_t finished_idx = sg4::Io::wait_any(activities);
        XBT_INFO("Completed reading size = %llu", activities[finished_idx]->get_performed_ioops());
        std::swap(activities[finished_idx], activities.back());
        activities.pop_back();
    }
}

}  // namespace dslab::simgrid_examples
