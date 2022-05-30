#include "worker.h"

#include <simgrid/s4u.hpp>
#include <xbt/random.hpp>
#include <simgrid/s4u/Mailbox.hpp>

XBT_LOG_NEW_DEFAULT_CATEGORY(worker, "Worker");

Worker::Worker(const std::string& name, int speed, int cores, double memory, bool async_mode,
               sg4::Mailbox* master_mb, sg4::Host* master_host)
    : name_(name),
      speed_(speed),
      cores_(cores),
      memory_(memory),
      async_mode_(async_mode),
      master_mb_(master_mb),
      master_host_(master_host) {
    mb_ = sg4::Mailbox::by_name(name);
}

void Worker::operator()() {
    mb_->set_receiver(sg4::Actor::self());
    RegisterOnMaster();

    // start message receive activity
    Message* msg;
    auto comm = mb_->get_async<Message>(&msg);
    pending_activities_.push_back(comm);

    bool stopped = false;
    while (!stopped) {
        // wait for completion of any pending activities (message receive, data transfer, task exec,
        // disk I/O...)
        ssize_t changed_pos = sg4::Activity::wait_any(pending_activities_);
        if (changed_pos != -1) {
            auto* completed = pending_activities_[changed_pos].get();
            XBT_DEBUG("Completed %s", completed->get_cname());
            const std::string& completed_name = completed->get_name();
            // message received
            if (completed_name == "unnamed") {
                switch (msg->type) {
                    case MessageType::TASK_REQUEST: {
                        if (async_mode_) {
                            // process task asynchronously
                            OnTaskRequestAsync(static_cast<TaskRequest*>(msg->data));
                        } else {
                            // process task synchronously
                            OnTaskRequestAsync(static_cast<TaskRequest*>(msg->data));
                        }
                        break;
                    }
                    case MessageType::STOP: {
                        XBT_DEBUG("Got STOP");
                        stopped = true;
                        break;
                    }
                    default:
                        std::abort();
                }
                delete msg;
                // start next message receive activity
                if (!stopped) {
                    comm = mb_->get_async<Message>(&msg);
                    pending_activities_.push_back(comm);
                }
                // task-related activities
            } else {
                int task_id = activity_tasks_[completed_name];
                activity_tasks_.erase(completed_name);
                // data download completed
                if (completed_name.starts_with("download-")) {
                    OnDataDownloadCompleted(task_id);
                    // data upload completed
                } else if (completed_name.starts_with("upload-")) {
                    OnDataUploadCompleted(task_id);
                    // task execution completed
                } else if (completed_name.starts_with("exec-")) {
                    OnTaskExecCompleted(task_id);
                    // disk read completed
                } else if (completed_name.starts_with("read-")) {
                    OnDataReadCompleted(task_id);
                    // disk write completed
                } else if (completed_name.starts_with("write-")) {
                    OnDataWriteCompleted(task_id);
                }
            }
            std::swap(pending_activities_[changed_pos], pending_activities_.back());
            pending_activities_.pop_back();
        }
    }
    XBT_DEBUG("Exiting");
}

void Worker::RegisterOnMaster() {
    auto* reg = new WorkerRegister{name_, speed_, cores_, memory_};
    auto* msg = new Message(MessageType::WORKER_REGISTER, reg, mb_);
    master_mb_->put(msg, kMessagePayloadSize);
}

// Synchronous version of task processing (slow, since it processes only a single task at time)
void Worker::OnTaskRequestSync(TaskRequest* req) {
    XBT_DEBUG("Task %d: received", req->id);
    tasks_.emplace(req->id, TaskInfo{req, TaskState::DOWNLOADING});

    // download task input data from master
    sg4::Comm::sendto(master_host_, sg4::this_actor::get_host(), req->input_size);
    XBT_DEBUG("Task %d: downloaded input", req->id);

    // read input data from disk
    tasks_[req->id].state = TaskState::READING;
    sg4::Host::current()->get_disks().front()->read(req->input_size);
    XBT_DEBUG("Task %d: read input", req->id);

    // run task
    tasks_[req->id].state = TaskState::RUNNING;
    sg4::this_actor::execute(req->flops);
    XBT_DEBUG("Task %d: completed execution", req->id);

    // write output data to disk
    tasks_[req->id].state = TaskState::WRITING;
    sg4::Host::current()->get_disks().front()->write(req->output_size);
    XBT_DEBUG("Task %d: wrote output", req->id);

    // upload task output data to master
    tasks_[req->id].state = TaskState::UPLOADING;
    sg4::Comm::sendto(sg4::this_actor::get_host(), master_host_, req->output_size);
    XBT_DEBUG("Task %d: uploaded output", req->id);

    tasks_[req->id].state = TaskState::COMPLETED;
    auto* msg = new Message(MessageType::TASK_COMPLETED, new TaskCompleted{req->id}, mb_);
    master_mb_->put(msg, kMessagePayloadSize);
}

void Worker::OnTaskRequestAsync(TaskRequest* req) {
    int task_id = req->id;
    XBT_DEBUG("Task %d: received", task_id);
    tasks_.emplace(req->id, TaskInfo{req, TaskState::DOWNLOADING});
    // download task input data asynchronously
    auto comm =
        sg4::Comm::sendto_async(master_host_, sg4::this_actor::get_host(), req->output_size);
    comm->set_name("download-" + std::to_string(task_id));
    pending_activities_.push_back(comm);
    activity_tasks_.emplace(comm->get_name(), task_id);
}

void Worker::OnDataDownloadCompleted(int task_id) {
    auto& task = tasks_[task_id];
    task.state = TaskState::READING;
    // read data from disk asynchronously
    auto io = sg4::Host::current()->get_disks().front()->read_async(task.req->input_size);
    io->set_name("read-" + std::to_string(task_id));
    pending_activities_.push_back(io);
    activity_tasks_.emplace(io->get_name(), task_id);
}

void Worker::OnDataReadCompleted(int task_id) {
    auto& task = tasks_[task_id];
    task.state = TaskState::RUNNING;
    // execute task asynchronously
    auto exec = sg4::this_actor::exec_async(task.req->flops);
    exec->set_name("exec-" + std::to_string(task_id));
    pending_activities_.push_back(exec);
    activity_tasks_.emplace(exec->get_name(), task_id);
}

void Worker::OnTaskExecCompleted(int task_id) {
    auto& task = tasks_[task_id];
    task.state = TaskState::WRITING;
    // write data to disk asynchronously
    auto io = sg4::Host::current()->get_disks().front()->write_async(task.req->output_size);
    io->set_name("write-" + std::to_string(task_id));
    pending_activities_.push_back(io);
    activity_tasks_.emplace(io->get_name(), task_id);
}

void Worker::OnDataWriteCompleted(int task_id) {
    auto& task = tasks_[task_id];
    task.state = TaskState::UPLOADING;
    // upload task output data asynchronously
    auto comm =
        sg4::Comm::sendto_async(sg4::this_actor::get_host(), master_host_, task.req->output_size);
    comm->set_name("upload-" + std::to_string(task_id));
    pending_activities_.push_back(comm);
    activity_tasks_.emplace(comm->get_name(), task_id);
}

void Worker::OnDataUploadCompleted(int task_id) {
    auto& task = tasks_[task_id];
    task.state = TaskState::COMPLETED;
    // report task completion to master
    auto* msg = new Message(MessageType::TASK_COMPLETED, new TaskCompleted{task_id}, mb_);
    master_mb_->put(msg, kMessagePayloadSize);
}
