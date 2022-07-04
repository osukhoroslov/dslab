#pragma once

#include <simgrid/forward.h>

#include <cstdint>
#include <functional>
#include <string>

namespace dslab::simgrid_examples {

namespace sg4 = simgrid::s4u;

using DegradationRule = sg4::NonLinearResourceCb;

using BandwidthFunction = std::function<double(sg_size_t)>;

class DisksSuit {
public:
    DisksSuit(sg4::Host* host, std::string name_prefix, double read_bw, double write_bw);

    void SetReadCapacityDegradation(DegradationRule rule);
    void SetWriteCapacityDegradation(DegradationRule rule);
    void SetReadBandwidthFunction(BandwidthFunction bf);
    void SetWriteBandwidthFunction(BandwidthFunction bf);

    void MakeDisks(uint64_t count);

    sg4::IoPtr ReadAsync(uint64_t disk_idx, uint64_t size);

private:
    sg4::Host* host_;
    std::string name_prefix_;
    double read_bw_ = 0., write_bw_ = 0.;

    DegradationRule read_degradation_rule_, write_degradation_rule_;
    BandwidthFunction read_bf_, write_bf_;
    std::vector<sg4::Disk*> disks_;
};

}  // namespace dslab::simgrid_examples
