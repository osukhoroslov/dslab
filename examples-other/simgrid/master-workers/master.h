#pragma once

#include <unordered_map>
#include <vector>
#include <map>

#include "common.h"

enum WorkerState { ONLINE, OFFLINE };

struct WorkerInfo {
    std::string id;
    WorkerState state;
    int speed;
    int cpus_total;
    int cpus_available;
    double memory_total;
    double memory_available;
    sg4::Mailbox* mb;
};

class Master {
public:
    explicit Master(std::string name, uint32_t task_count, bool blocking, double& scheduling_time);

    void operator()();

    // Blocking implementation of main loop
    // - uses blocking get() to receive incoming messages
    // - as a consequence, periodic activities can be delayed
    void BlockingImpl();

    // Non-blocking implementation of main loop
    // - uses non-blocking test() to check for incoming messages
    // - periodic activities are not delayed, but sleep() is needed which may delay message
    // receiving
    void NonblockingImpl();

private:
    void OnWorkerRegister(WorkerRegister* reg, sg4::Mailbox* worker_mb);
    void OnTaskRequest(TaskRequest* req);
    void OnTaskCompleted(TaskCompleted* msg, sg4::Mailbox* worker_mb);
    void ScheduleTasks();
    void ReportStatus();

    uint32_t task_count_ = 0;
    bool blocking_ = true;
    sg4::Mailbox* mb_ = nullptr;
    int cpus_total_ = 0;
    int cpus_available_ = 0;
    double memory_total_ = 0;
    double memory_available_ = 0;
    std::unordered_map<std::string, WorkerInfo*> workers_;
    std::vector<WorkerInfo*> idle_workers_;
    std::map<int, TaskInfo> unassigned_tasks_;
    std::unordered_map<int, TaskInfo> assigned_tasks_;
    std::unordered_map<int, TaskInfo> completed_tasks_;
    double next_schedule_time_ = 10;
    double next_report_time_ = 10;
    double& scheduling_time_;
};
