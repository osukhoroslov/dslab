#include "master.h"

#include <unordered_set>

#include <simgrid/s4u.hpp>
#include <xbt/random.hpp>

XBT_LOG_NEW_DEFAULT_CATEGORY(master, "Master");

Master::Master(std::string name, uint32_t task_count, bool blocking, double& scheduling_time)
    : task_count_(task_count), blocking_(blocking), scheduling_time_(scheduling_time) {
    mb_ = sg4::Mailbox::by_name(name);
}

void Master::operator()() {
    mb_->set_receiver(sg4::Actor::self());
    if (blocking_) {
        BlockingImpl();
    } else {
        NonblockingImpl();
    }
    ReportStatus();
    // stop all workers
    for (auto& [worker_id, worker] : workers_) {
        auto* msg = new Message(MessageType::STOP, nullptr, mb_);
        worker->mb->put(msg, kMessagePayloadSize);
    }
    XBT_DEBUG("Exiting");
}

// Blocking implementation of main loop
// - uses blocking get() to receive incoming messages
// - as a consequence, periodic activities can be delayed
void Master::BlockingImpl() {
    while (completed_tasks_.size() != task_count_) {
        // receive messages from client and workers
        Message* msg = mb_->get<Message>();
        switch (msg->type) {
            case MessageType::WORKER_REGISTER: {
                OnWorkerRegister(static_cast<WorkerRegister*>(msg->data), msg->from);
                break;
            }
            case MessageType::TASK_REQUEST: {
                OnTaskRequest(static_cast<TaskRequest*>(msg->data));
                break;
            }
            case MessageType::TASK_COMPLETED: {
                OnTaskCompleted(static_cast<TaskCompleted*>(msg->data), msg->from);
                break;
            }
            default:
                std::abort();
        }
        delete msg;
        // execute periodic activities
        double now = sg4::Engine::get_clock();
        if (now >= next_report_time_ || unassigned_tasks_.size() == task_count_) {
            ReportStatus();
            next_report_time_ = now + kReportStatusPeriod;
        }
        if (now >= next_schedule_time_ || unassigned_tasks_.size() == task_count_ ||
            (!completed_tasks_.empty() && assigned_tasks_.empty())) {
            ScheduleTasks();
            next_schedule_time_ = now + kSchedulePeriod;
        }
    }
}

// Non-blocking implementation of main loop
// - uses non-blocking test() to check for incoming messages
// - periodic activities are not delayed, but sleep() is needed which may delay message receiving
void Master::NonblockingImpl() {
    Message* msg;
    auto comm = mb_->get_async<Message>(&msg);
    while (completed_tasks_.size() != task_count_) {
        bool comm_completed = false;
        // receive messages from client and workers
        if (comm->test()) {  // cannot use wait_for(timeout) since it breaks sending activities on
                             // worker side!
            switch (msg->type) {
                case MessageType::WORKER_REGISTER: {
                    OnWorkerRegister(static_cast<WorkerRegister*>(msg->data), msg->from);
                    break;
                }
                case MessageType::TASK_REQUEST: {
                    OnTaskRequest(static_cast<TaskRequest*>(msg->data));
                    break;
                }
                case MessageType::TASK_COMPLETED: {
                    OnTaskCompleted(static_cast<TaskCompleted*>(msg->data), msg->from);
                    break;
                }
                default:
                    std::abort();
            }
            delete msg;
            comm_completed = true;
            comm = mb_->get_async<Message>(&msg);
        }
        // periodic activities
        double now = sg4::Engine::get_clock();
        if (now >= next_report_time_ || unassigned_tasks_.size() == task_count_) {
            ReportStatus();
            next_report_time_ = now + kReportStatusPeriod;
        }
        if (now >= next_schedule_time_ || unassigned_tasks_.size() == task_count_ ||
            (!completed_tasks_.empty() && assigned_tasks_.empty())) {
            ScheduleTasks();
            next_schedule_time_ = now + kSchedulePeriod;
        }
        // sleep
        if (!comm_completed) {
            sg4::this_actor::sleep_for(0.1);
        }
    }
}

void Master::OnWorkerRegister(WorkerRegister* reg, sg4::Mailbox* worker_mb) {
    XBT_DEBUG("Worker %s", reg->name.c_str());
    WorkerInfo* info =
        new WorkerInfo{reg->name,       WorkerState::ONLINE, reg->speed,        reg->cpus_total,
                       reg->cpus_total, reg->memory_total,   reg->memory_total, worker_mb};
    workers_.emplace(reg->name, info);
    idle_workers_.push_back(info);
    cpus_total_ += info->cpus_total;
    cpus_available_ += info->cpus_available;
    memory_total_ += info->memory_total;
    memory_available_ += info->memory_available;
}

void Master::OnTaskRequest(TaskRequest* req) {
    XBT_DEBUG("Task %d", req->id);
    unassigned_tasks_.emplace(req->id, TaskInfo{req, TaskState::NEW});
}

void Master::OnTaskCompleted(TaskCompleted* msg, sg4::Mailbox* worker_mb) {
    int task_id = msg->task_id;
    XBT_DEBUG("Completed task %d", task_id);
    auto& task = assigned_tasks_[task_id];
    task.state = TaskState::COMPLETED;
    completed_tasks_.emplace(task_id, task);
    assigned_tasks_.erase(task_id);

    auto* worker = workers_[worker_mb->get_name()];
    if (worker->cpus_available == 0 || worker->memory_available == 0) {
        idle_workers_.push_back(worker);
    }
    worker->cpus_available += task.req->cores;
    worker->memory_available += task.req->memory;
    cpus_available_ += task.req->cores;
    memory_available_ += task.req->memory;
}

void Master::ScheduleTasks() {
    if (unassigned_tasks_.empty()) {
        return;
    }
    auto start = std::chrono::steady_clock::now();
    XBT_DEBUG(">> Available resources: %d %f", cpus_available_, memory_available_);
    std::unordered_set<int> assigned;
    for (auto& [task_id, task] : unassigned_tasks_) {
        // XBT_DEBUG("- %d: %d flops, %d cores, %d memory", task_id, task.req->flops,
        // task.req->cores, task.req->memory);
        if (idle_workers_.empty()) {
            break;
        }
        if (cpus_available_ < task.req->cores || memory_available_ < task.req->memory) {
            continue;
        }
        std::sort(idle_workers_.begin(), idle_workers_.end(), [](WorkerInfo* w1, WorkerInfo* w2) {
            return std::tie(w1->memory_available, w1->cpus_available, w1->speed, w1->id) >
                   std::tie(w2->memory_available, w2->cpus_available, w2->speed, w2->id);
        });
        for (auto it = idle_workers_.begin(); it != idle_workers_.end();) {
            WorkerInfo* worker = *it;
            // XBT_DEBUG("-- w %s: %d %d %d", worker->id.c_str(), worker->cpus_available,
            // worker->memory_available, worker->speed);
            if (worker->cpus_available >= task.req->cores &&
                worker->memory_available >= task.req->memory) {
                XBT_DEBUG("Assigned %d to %s", task_id, worker->id.c_str());
                worker->cpus_available -= task.req->cores;
                worker->memory_available -= task.req->memory;
                cpus_available_ -= task.req->cores;
                memory_available_ -= task.req->memory;
                auto* msg = new Message(MessageType::TASK_REQUEST, task.req, mb_);
                worker->mb->put_init(msg, kMessagePayloadSize)->detach();
                assigned.insert(task_id);
                if (worker->cpus_available == 0 || worker->memory_available == 0) {
                    std::swap(*it, idle_workers_.back());
                    idle_workers_.pop_back();
                }
                break;
            } else {
                it++;
            }
        }
    }
    for (auto const& task_id : assigned) {
        auto& task = unassigned_tasks_[task_id];
        task.state = TaskState::ASSIGNED;
        assigned_tasks_.emplace(task_id, task);
        unassigned_tasks_.erase(task_id);
    }
    auto stop = std::chrono::steady_clock::now();
    double duration =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count()) /
        1000;
    XBT_INFO("schedule tasks: assigned %ld tasks in %.2f ms", assigned.size(), duration);
    scheduling_time_ += duration / 1000;
}

void Master::ReportStatus() {
    XBT_INFO("CPU: %f / MEMORY: %f / UNASSIGNED: %ld / ASSIGNED: %ld / COMPLETED: %ld",
             (double)(cpus_total_ - cpus_available_) / cpus_total_,
             (memory_total_ - memory_available_) / memory_total_, unassigned_tasks_.size(),
             assigned_tasks_.size(), completed_tasks_.size());
}
