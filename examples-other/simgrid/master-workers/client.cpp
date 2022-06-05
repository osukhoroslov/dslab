#include "client.h"

#include <simgrid/s4u.hpp>
#include <xbt/random.hpp>

XBT_LOG_NEW_DEFAULT_CATEGORY(client, "Client");

Client::Client(std::string name, uint32_t task_count, sg4::Mailbox* master_mb,
               simgrid::xbt::random::XbtRandom* random)
    : task_count_(task_count), master_mb_(master_mb), random_(random) {
    mb_ = sg4::Mailbox::by_name(name);
}

void Client::operator()() {
    // generate and submit tasks to master
    for (uint32_t i = 0; i < task_count_; i++) {
        int flops = random_->uniform_int(100, 1000);
        double memory = random_->uniform_int(1, 8) * 128;
        int cores = 1;
        double input_size = random_->uniform_int(100, 1000) * 10e6;
        double output_size = random_->uniform_int(10, 100) * 10e6;
        auto* req =
            new TaskRequest{static_cast<int>(i), flops, memory, cores, input_size, output_size};
        auto* msg = new Message(MessageType::TASK_REQUEST, req, mb_);
        master_mb_->put(msg, kMessagePayloadSize);
    }
    XBT_DEBUG("Exiting");
}
