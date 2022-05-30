#pragma once

#include "fwd.h"

#include <vector>

static inline constexpr int kMessagePayloadSize = 10;

enum class MessageType { START, PING, PONG, COMPLETED, STOP };

namespace sg4 = simgrid::s4u;

struct Message {
    MessageType type;
    double payload;  // current sender time is used as a message payload
    sg4::Mailbox* from = nullptr;

    explicit Message(MessageType type, double payload, sg4::Mailbox* from)
        : type(type), payload(payload), from(from) {
    }

    static void Destroy(void* message);
};

void Root(sg4::Mailbox* in, std::vector<sg4::Mailbox*> process_mailboxes, bool asymmetric);
void Process(int id, sg4::Mailbox* in, std::vector<sg4::Mailbox*> peers, int iterations);
void ProcessAsymmetric(bool is_pinger, sg4::Mailbox* in, sg4::Mailbox* out, int iterations);
