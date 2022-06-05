#pragma once

#include "common.h"

namespace simgrid::xbt::random {
class XbtRandom;
}

class Client {
public:
    explicit Client(std::string name, uint32_t task_count, sg4::Mailbox* master_mb,
                    simgrid::xbt::random::XbtRandom* random);

    void operator()();

private:
    uint32_t task_count_ = 0;
    sg4::Mailbox* mb_ = nullptr;
    sg4::Mailbox* master_mb_ = nullptr;
    simgrid::xbt::random::XbtRandom* random_ = nullptr;
};
