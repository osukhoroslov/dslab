#pragma once

#include <simgrid/forward.h>

#include <functional>

namespace dslab::simgrid_examples {

namespace sg4 = simgrid::s4u;

using DegradationRule = sg4::NonLinearResourceCb;

using BandwidthFunction = std::function<double(sg_size_t)>;

class DiskWrapper {
public:
    DiskWrapper(sg4::Host* host, const std::string& name, double read_bw, double write_bw);

    void SetReadCapacityDegradation(DegradationRule rule);
    void SetWriteCapacityDegradation(DegradationRule rule);
    void SetReadBandwidthFunction(BandwidthFunction bf);
    void SetWriteBandwidthFunction(BandwidthFunction bf);

    void ApplyAndSeal();
    void Run(size_t activities_count);

private:
    DegradationRule read_degradation_rule_, write_degradation_rule_;
    BandwidthFunction read_bf_, write_bf_;
    sg4::Disk* disk_ = nullptr;
};

}  // namespace dslab::simgrid_examples
