#pragma once

#include <string>

static inline constexpr int kSchedulePeriod = 10;
static inline constexpr int kReportStatusPeriod = 100;
static inline constexpr int kMessagePayloadSize = 10;

namespace simgrid::s4u {
class Mailbox;
}

namespace sg4 = simgrid::s4u;

enum MessageType { START, WORKER_REGISTER, TASK_REQUEST, TASK_COMPLETED, STOP };

struct Message {
    MessageType type;
    void* data;
    sg4::Mailbox* from = nullptr;

    explicit Message(MessageType type, void* data, sg4::Mailbox* from)
        : type(type), data(data), from(from) {
    }
};

struct WorkerRegister {
    std::string name;
    int speed;
    int cpus_total;
    double memory_total;
};

struct TaskRequest {
    int id;
    int flops;
    double memory;
    int cores;
    double input_size;
    double output_size;
};

enum TaskState { NEW, ASSIGNED, DOWNLOADING, READING, RUNNING, WRITING, UPLOADING, COMPLETED };

struct TaskInfo {
    TaskRequest* req;
    TaskState state;
};

struct TaskCompleted {
    int task_id;
};
