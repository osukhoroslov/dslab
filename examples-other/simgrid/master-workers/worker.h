#pragma once

#include <unordered_map>
#include <vector>

#include "common.h"

class Worker {
public:
    explicit Worker(const std::string& name, int speed, int cores, double memory, bool async_mode,
                    sg4::Mailbox* master_mb, sg4::Host* master_host);

    void operator()();

private:
    void RegisterOnMaster();

    // Synchronous version of task processing (slow, since it processes only a single task at time)
    void OnTaskRequestSync(TaskRequest* req);
    void OnTaskRequestAsync(TaskRequest* req);
    void OnDataDownloadCompleted(int task_id);
    void OnDataReadCompleted(int task_id);
    void OnTaskExecCompleted(int task_id);
    void OnDataWriteCompleted(int task_id);
    void OnDataUploadCompleted(int task_id);

    std::string name_;
    int speed_;
    int cores_;
    double memory_;
    bool async_mode_ = true;
    std::unordered_map<int, TaskInfo> tasks_;
    sg4::Mailbox* mb_ = nullptr;
    sg4::Mailbox* master_mb_ = nullptr;
    sg4::Host* master_host_ = nullptr;
    sg4::ActivitySet* pending_activities_ = nullptr;
    std::unordered_map<std::string, int> activity_tasks_;
};
