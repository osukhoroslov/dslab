#include "disk.h"

#include <simgrid/s4u.hpp>

namespace dslab::simgrid_examples {

DisksSuit::DisksSuit(sg4::Host* host, std::string name_prefix, double read_bw, double write_bw)
    : host_(host), name_prefix_(std::move(name_prefix)), read_bw_(read_bw), write_bw_(write_bw) {
}

void DisksSuit::SetReadCapacityDegradation(DegradationRule rule) {
    read_degradation_rule_ = std::move(rule);
}

void DisksSuit::SetWriteCapacityDegradation(DegradationRule rule) {
    write_degradation_rule_ = std::move(rule);
}

void DisksSuit::SetReadBandwidthFunction(BandwidthFunction bf) {
    read_bf_ = std::move(bf);
}

void DisksSuit::SetWriteBandwidthFunction(BandwidthFunction bf) {
    write_bf_ = std::move(bf);
}

void DisksSuit::MakeDisks(uint64_t count) {
    disks_.reserve(count);

    for (size_t idx = 1; idx <= count; ++idx) {
        auto disk =
            host_->create_disk(name_prefix_ + "-" + std::to_string(idx), read_bw_, write_bw_);

        if (read_degradation_rule_) {
            disk->set_sharing_policy(sg4::Disk::Operation::READ,
                                     sg4::Disk::SharingPolicy::NONLINEAR, read_degradation_rule_);
        } else {
            disk->set_sharing_policy(sg4::Disk::Operation::READ, sg4::Disk::SharingPolicy::LINEAR);
        }

        if (write_degradation_rule_) {
            disk->set_sharing_policy(sg4::Disk::Operation::WRITE,
                                     sg4::Disk::SharingPolicy::NONLINEAR, write_degradation_rule_);
        } else {
            disk->set_sharing_policy(sg4::Disk::Operation::WRITE, sg4::Disk::SharingPolicy::LINEAR);
        }

        if (read_bf_ || write_bf_) {
            disk->set_factor_cb([this](sg_size_t size, sg4::Io::OpType op) {
                if (op == sg4::Io::OpType::READ && read_bf_) {
                    return read_bf_(size);
                } else if (op == sg4::Io::OpType::WRITE && write_bf_) {
                    return write_bf_(size);
                }
                return 1.;
            });
        }

        disk->seal();
        disks_.push_back(disk);
    }
}

sg4::IoPtr DisksSuit::ReadAsync(uint64_t disk_idx, uint64_t size) {
    return disks_[disk_idx]->read_async(size);
}

}  // namespace dslab::simgrid_examples
