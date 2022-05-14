#include <unordered_set>
#include <simgrid/s4u.hpp>
#include <xbt/random.hpp>

namespace sg4 = simgrid::s4u;

XBT_LOG_NEW_DEFAULT_CATEGORY(master_workers_app, "Master-workers example");

static const int SCHEDULE_PERIOD = 10;
static const int REPORT_STATUS_PERIOD = 100;
static const int MESSAGE_PAYLOAD_SIZE = 10;

// MESSAGES + COMMON STRUCTS ///////////////////////////////////////////////////////////////////////////////////////////

enum MessageType {
    START,
    WORKER_REGISTER,
    TASK_REQUEST,
    TASK_COMPLETED,
    STOP
};

struct Message {
    MessageType type;
    void* data;
    sg4::Mailbox* from = nullptr;

    explicit Message(MessageType type, void* data, sg4::Mailbox* from) : type(type), data(data), from(from) {}
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

enum TaskState {
    NEW,
    ASSIGNED,
    DOWNLOADING,
    READING,
    RUNNING,
    WRITING,
    UPLOADING,
    COMPLETED
};

struct TaskInfo {
    TaskRequest* req;
    TaskState state;
};

struct TaskCompleted {
    int task_id;
};

// MASTER //////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum WorkerState {
    ONLINE,
    OFFLINE
};

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
    unsigned int task_count = 0;
    bool blocking = true;
    sg4::Mailbox* mb = nullptr;
    int cpus_total = 0;
    int cpus_available = 0;
    double memory_total = 0;
    double memory_available = 0;
    std::unordered_map<std::string, WorkerInfo*> workers;
    std::vector<WorkerInfo*> idle_workers;
    std::map<int, TaskInfo> unassigned_tasks;
    std::unordered_map<int, TaskInfo> assigned_tasks;
    std::unordered_map<int, TaskInfo> completed_tasks;
    double next_schedule_time = 10;
    double next_report_time = 10;
    double& scheduling_time;

public:
    explicit Master(std::string name, unsigned int task_count, bool blocking, double& scheduling_time)
            : task_count(task_count), blocking(blocking), scheduling_time(scheduling_time) {
        mb = sg4::Mailbox::by_name(name);
    }

    void operator()() {
        mb->set_receiver(sg4::Actor::self());
        if (blocking) {
            blocking_impl();
        } else {
            nonblocking_impl();
        }
        report_status();
        // stop all workers
        for (auto& [worker_id, worker] : workers) {
            auto* msg = new Message(MessageType::STOP, nullptr, mb);
            worker->mb->put(msg, MESSAGE_PAYLOAD_SIZE);
        }
        XBT_DEBUG("Exiting");
    }

    // Blocking implementation of main loop
    // - uses blocking get() to receive incoming messages
    // - as a consequence, periodic activities can be delayed
    void blocking_impl() {
        while(completed_tasks.size() != task_count) {
            // receive messages from client and workers
            Message* msg = mb->get<Message>();
            switch(msg->type) {
                case MessageType::WORKER_REGISTER: {
                    on_worker_register((WorkerRegister*)msg->data, msg->from);
                    break;
                }
                case MessageType::TASK_REQUEST: {
                    on_task_request((TaskRequest*)msg->data);
                    break;
                }
                case MessageType::TASK_COMPLETED: {
                    on_task_completed((TaskCompleted*)msg->data, msg->from);
                    break;
                }
                default:
                    std::abort();
            }
            delete msg;
            // execute periodic activities
            double now = sg4::Engine::get_clock();
            if (now >= next_report_time || unassigned_tasks.size() == task_count) {
                report_status();
                next_report_time = now + REPORT_STATUS_PERIOD;
            }
            if (now >= next_schedule_time
                    || unassigned_tasks.size() == task_count
                    || (!completed_tasks.empty() && assigned_tasks.empty())) {
                schedule_tasks();
                next_schedule_time = now + SCHEDULE_PERIOD;
            }
        }
    }

    // Non-blocking implementation of main loop
    // - uses non-blocking test() to check for incoming messages
    // - periodic activities are not delayed, but sleep() is needed which may delay message receiving
    void nonblocking_impl() {
        Message* msg;
        auto comm = mb->get_async<Message>(&msg);
        while(completed_tasks.size() != task_count) {
            bool comm_completed = false;
            // receive messages from client and workers
            if (comm->test()) {  // cannot use wait_for(timeout) since it breaks sending activities on worker side!
                switch(msg->type) {
                    case MessageType::WORKER_REGISTER: {
                        on_worker_register((WorkerRegister*)msg->data, msg->from);
                        break;
                    }
                    case MessageType::TASK_REQUEST: {
                        on_task_request((TaskRequest*)msg->data);
                        break;
                    }
                    case MessageType::TASK_COMPLETED: {
                        on_task_completed((TaskCompleted*)msg->data, msg->from);
                        break;
                    }
                    default:
                        std::abort();
                }
                delete msg;
                comm_completed = true;
                comm = mb->get_async<Message>(&msg);
            }
            // periodic activities
            double now = sg4::Engine::get_clock();
            if (now >= next_report_time || unassigned_tasks.size() == task_count) {
                report_status();
                next_report_time = now + REPORT_STATUS_PERIOD;
            }
            if (now >= next_schedule_time
                    || unassigned_tasks.size() == task_count
                    || (!completed_tasks.empty() && assigned_tasks.empty())) {
                schedule_tasks();
                next_schedule_time = now + SCHEDULE_PERIOD;
            }
            // sleep
            if (!comm_completed) {
                sg4::this_actor::sleep_for(0.1);
            }
        }
    }

private:
    void on_worker_register(WorkerRegister* reg, sg4::Mailbox* worker_mb) {
        XBT_DEBUG("Worker %s", reg->name.c_str());
        WorkerInfo* info = new WorkerInfo {
            reg->name,
            WorkerState::ONLINE, reg->speed,
            reg->cpus_total, reg->cpus_total,
            reg->memory_total, reg->memory_total,
            worker_mb
        };
        workers.emplace(reg->name, info);
        idle_workers.push_back(info);
        cpus_total += info->cpus_total;
        cpus_available += info->cpus_available;
        memory_total += info->memory_total;
        memory_available += info->memory_available;
    }

    void on_task_request(TaskRequest* req) {
        XBT_DEBUG("Task %d", req->id);
        unassigned_tasks.emplace(req->id, TaskInfo{req, TaskState::NEW});
    }

    void on_task_completed(TaskCompleted* msg, sg4::Mailbox* worker_mb) {
        int task_id = msg->task_id;
        XBT_DEBUG("Completed task %d", task_id);
        auto& task = assigned_tasks[task_id];
        task.state = TaskState::COMPLETED;
        completed_tasks.emplace(task_id, task);
        assigned_tasks.erase(task_id);

        auto* worker = workers[worker_mb->get_name()];
        if (worker->cpus_available == 0 || worker->memory_available == 0) {
            idle_workers.push_back(worker);
        }
        worker->cpus_available += task.req->cores;
        worker->memory_available += task.req->memory;
        cpus_available += task.req->cores;
        memory_available += task.req->memory;
    }

    void schedule_tasks() {
        if (unassigned_tasks.empty()) return;
        auto start = std::chrono::steady_clock::now();
        XBT_DEBUG(">> Available resources: %d %f", cpus_available, memory_available);
        std::unordered_set<int> assigned;
        for (auto& [task_id, task] : unassigned_tasks) {
            //XBT_DEBUG("- %d: %d flops, %d cores, %d memory", task_id, task.req->flops, task.req->cores, task.req->memory);
            if (idle_workers.empty()) {
                break;
            }
            if (cpus_available < task.req->cores || memory_available < task.req->memory) {
                continue;
            }
            std::sort(
                idle_workers.begin(), idle_workers.end(),
                [](WorkerInfo* w1, WorkerInfo* w2) {
                    return std::tie(w1->memory_available, w1->cpus_available, w1->speed, w1->id)
                           > std::tie(w2->memory_available, w2->cpus_available, w2->speed, w2->id);
                }
            );
            for (auto it = idle_workers.begin(); it != idle_workers.end(); ) {
                WorkerInfo* worker = *it;
                //XBT_DEBUG("-- w %s: %d %d %d", worker->id.c_str(), worker->cpus_available, worker->memory_available, worker->speed);
                if (worker->cpus_available >= task.req->cores && worker->memory_available >= task.req->memory) {
                    XBT_DEBUG("Assigned %d to %s", task_id, worker->id.c_str());
                    worker->cpus_available -= task.req->cores;
                    worker->memory_available -= task.req->memory;
                    cpus_available -= task.req->cores;
                    memory_available -= task.req->memory;
                    auto* msg = new Message(MessageType::TASK_REQUEST, task.req, mb);
                    worker->mb->put_init(msg, MESSAGE_PAYLOAD_SIZE)->detach();
                    assigned.insert(task_id);
                    if (worker->cpus_available == 0 || worker->memory_available == 0) {
                        std::swap(*it, idle_workers.back());
                        idle_workers.pop_back();
                    }
                    break;
                } else {
                    it++;
                }
            }
        }
        for (auto const& task_id : assigned) {
            auto& task = unassigned_tasks[task_id];
            task.state = TaskState::ASSIGNED;
            assigned_tasks.emplace(task_id, task);
            unassigned_tasks.erase(task_id);
        }
        auto stop = std::chrono::steady_clock::now();
        double duration = (double)(std::chrono::duration_cast<std::chrono::microseconds>(stop - start).count()) / 1000;
        XBT_INFO("schedule tasks: assigned %ld tasks in %.2f ms", assigned.size(), duration);
        scheduling_time += duration / 1000;
    }

    void report_status() {
        XBT_INFO("CPU: %f / MEMORY: %f / UNASSIGNED: %ld / ASSIGNED: %ld / COMPLETED: %ld",
                (double)(cpus_total - cpus_available) / cpus_total, (memory_total - memory_available) / memory_total,
                unassigned_tasks.size(), assigned_tasks.size(), completed_tasks.size());
    }
};

// WORKER //////////////////////////////////////////////////////////////////////////////////////////////////////////////

class Worker {
    std::string name;
    int speed;
    int cores;
    double memory;
    bool async_mode = true;
    std::unordered_map<int, TaskInfo> tasks;
    sg4::Mailbox* mb = nullptr;
    sg4::Mailbox* master_mb = nullptr;
    sg4::Host* master_host = nullptr;
    std::vector<sg4::ActivityPtr> pending_activities;
    std::unordered_map<std::string, int> activity_tasks;

public:
    explicit Worker(const std::string& name, int speed, int cores, double memory, bool async_mode,
                    sg4::Mailbox* master_mb, sg4::Host* master_host)
            : name(name), speed(speed), cores(cores), memory(memory), async_mode(async_mode),
                    master_mb(master_mb), master_host(master_host) {
        mb = sg4::Mailbox::by_name(name);
    }

    void operator()() {
        mb->set_receiver(sg4::Actor::self());
        register_on_master();

        // start message receive activity
        Message* msg;
        auto comm = mb->get_async<Message>(&msg);
        pending_activities.push_back(comm);

        bool stopped = false;
        while (!stopped) {
            // wait for completion of any pending activities (message receive, data transfer, task exec, disk I/O...)
            ssize_t changed_pos = sg4::Activity::wait_any(pending_activities);
            if (changed_pos != -1) {
                auto* completed = pending_activities[changed_pos].get();
                XBT_DEBUG("Completed %s", completed->get_cname());
                const std::string& completed_name = completed->get_name();
                int task_id = -1;
                if (completed_name != "unnamed") {
                    task_id = activity_tasks[completed_name];
                    activity_tasks.erase(completed_name);
                }
                // communication completed
                if (dynamic_cast<sg4::Comm*>(completed)) {
                    // message received
                    if (task_id == -1) {
                        switch(msg->type) {
                            case MessageType::TASK_REQUEST: {
                                if (async_mode) {
                                    // process task asynchronously
                                    on_task_request_async((TaskRequest*)msg->data);
                                } else {
                                    // process task synchronously
                                    on_task_request_sync((TaskRequest*)msg->data);
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
                            comm = mb->get_async<Message>(&msg);
                            pending_activities.push_back(comm);
                        }
                    // data download completed
                    } else if (completed_name.starts_with("download-")) {
                        on_data_download_completed(task_id);
                    // data upload completed
                    } else if (completed_name.starts_with("upload-")) {
                        on_data_upload_completed(task_id);
                    }
                // task execution completed
                } else if (dynamic_cast<sg4::Exec*>(completed)) {
                    on_task_exec_completed(task_id);
                // disk I/O completed
                } else if (dynamic_cast<sg4::Io*>(completed)) {
                    // disk read completed
                    if (completed_name.starts_with("read-")) {
                        on_data_read_completed(task_id);
                    // disk write completed
                    } else if (completed_name.starts_with("write-")) {
                        on_data_write_completed(task_id);
                    }
                }
                std::swap(pending_activities[changed_pos], pending_activities.back());
                pending_activities.pop_back();
            }
        }
        XBT_DEBUG("Exiting");
    }

private:
    void register_on_master() {
        auto* reg = new WorkerRegister{name, speed, cores, memory};
        auto* msg = new Message(MessageType::WORKER_REGISTER, reg, mb);
        master_mb->put(msg, MESSAGE_PAYLOAD_SIZE);
    }

    // Synchronous version of task processing (slow, since it processes only a single task at time)
    void on_task_request_sync(TaskRequest* req) {
        XBT_DEBUG("Task %d: received", req->id);
        tasks.emplace(req->id, TaskInfo{req, TaskState::DOWNLOADING});

        // download task input data from master
        sg4::Comm::sendto(master_host, sg4::this_actor::get_host(), req->input_size);
        XBT_DEBUG("Task %d: downloaded input", req->id);

        // read input data from disk
        tasks[req->id].state = TaskState::READING;
        sg4::Host::current()->get_disks().front()->read(req->input_size);
        XBT_DEBUG("Task %d: read input", req->id);

        // run task
        tasks[req->id].state = TaskState::RUNNING;
        sg4::this_actor::execute(req->flops);
        XBT_DEBUG("Task %d: completed execution", req->id);

        // write output data to disk
        tasks[req->id].state = TaskState::WRITING;
        sg4::Host::current()->get_disks().front()->write(req->output_size);
        XBT_DEBUG("Task %d: wrote output", req->id);

        // upload task output data to master
        tasks[req->id].state = TaskState::UPLOADING;
        sg4::Comm::sendto(sg4::this_actor::get_host(), master_host, req->output_size);
        XBT_DEBUG("Task %d: uploaded output", req->id);

        tasks[req->id].state = TaskState::COMPLETED;
        auto* msg = new Message(MessageType::TASK_COMPLETED, new TaskCompleted{req-> id}, mb);
        master_mb->put(msg, MESSAGE_PAYLOAD_SIZE);
    }

    void on_task_request_async(TaskRequest* req) {
        int task_id = req->id;
        XBT_DEBUG("Task %d: received", task_id);
        tasks.emplace(req->id, TaskInfo{req, TaskState::DOWNLOADING});
        // download task input data asynchronously
        auto comm = sg4::Comm::sendto_async(master_host, sg4::this_actor::get_host(), req->output_size);
        comm->set_name("download-" + std::to_string(task_id));
        pending_activities.push_back(comm);
        activity_tasks.emplace(comm->get_name(), task_id);
    }

    void on_data_download_completed(int task_id) {
        auto& task = tasks[task_id];
        task.state = TaskState::READING;
        // read data from disk asynchronously
        auto io = sg4::Host::current()->get_disks().front()->read_async(task.req->input_size);
        io->set_name("read-" + std::to_string(task_id));
        pending_activities.push_back(io);
        activity_tasks.emplace(io->get_name(), task_id);
    }

    void on_data_read_completed(int task_id) {
        auto& task = tasks[task_id];
        task.state = TaskState::RUNNING;
        // execute task asynchronously
        auto exec = sg4::this_actor::exec_async(task.req->flops);
        exec->set_name("exec-" + std::to_string(task_id));
        pending_activities.push_back(exec);
        activity_tasks.emplace(exec->get_name(), task_id);
    }

    void on_task_exec_completed(int task_id) {
        auto& task = tasks[task_id];
        task.state = TaskState::WRITING;
        // write data to disk asynchronously
        auto io = sg4::Host::current()->get_disks().front()->write_async(task.req->output_size);
        io->set_name("write-" + std::to_string(task_id));
        pending_activities.push_back(io);
        activity_tasks.emplace(io->get_name(), task_id);
    }

    void on_data_write_completed(int task_id) {
        auto& task = tasks[task_id];
        task.state = TaskState::UPLOADING;
        // upload task output data asynchronously
        auto comm = sg4::Comm::sendto_async(sg4::this_actor::get_host(), master_host, task.req->output_size);
        comm->set_name("upload-" + std::to_string(task_id));
        pending_activities.push_back(comm);
        activity_tasks.emplace(comm->get_name(), task_id);
    }

    void on_data_upload_completed(int task_id) {
        auto& task = tasks[task_id];
        task.state = TaskState::COMPLETED;
        // report task completion to master
        auto* msg = new Message(MessageType::TASK_COMPLETED, new TaskCompleted{task_id}, mb);
        master_mb->put(msg, MESSAGE_PAYLOAD_SIZE);
    }
};

// CLIENT //////////////////////////////////////////////////////////////////////////////////////////////////////////////

class Client {
    unsigned int task_count = 0;
    sg4::Mailbox* mb = nullptr;
    sg4::Mailbox* master_mb = nullptr;
    simgrid::xbt::random::XbtRandom* random = nullptr;

public:
    explicit Client(std::string name, unsigned int task_count, sg4::Mailbox* master_mb,
                    simgrid::xbt::random::XbtRandom* random):
                    task_count(task_count), master_mb(master_mb), random(random) {
        mb = sg4::Mailbox::by_name(name);
    };

    void operator()() {
        // generate and submit tasks to master
        for (unsigned int i=0; i < task_count; i++) {
            int flops = random->uniform_int(100, 1000);
            double memory = random->uniform_int(1, 8) * 128;
            int cores = 1;
            double input_size = random->uniform_int(100, 1000) * 10e6;
            double output_size = random->uniform_int(10, 100) * 10e6;
            auto* req = new TaskRequest{(int)i, flops, memory, cores, input_size, output_size};
            auto* msg = new Message(MessageType::TASK_REQUEST, req, mb);
            master_mb->put(msg, MESSAGE_PAYLOAD_SIZE);
        }
        XBT_DEBUG("Exiting");
    }
};

// MAIN ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[]) {
    sg4::Engine e(&argc, argv);
    simgrid::xbt::random::XbtRandom random(123);

    xbt_assert(argc == 3, "Usage: %s HOST_COUNT TASK_COUNT", argv[0]);
    unsigned int host_count = std::stoi(argv[1]);
    unsigned int task_count = std::stoi(argv[2]);

    // build platform and create actors
    auto* zone = sg4::create_full_zone("net");
    sg4::Mailbox* master_mailbox = sg4::Mailbox::by_name("master");
    double scheduling_time = 0;
    for (unsigned int i=0; i < host_count; i++) {
        std::string hostname = "host-" + std::to_string(i);
        double speed = random.uniform_int(1, 10);
        int cores = random.uniform_int(1, 8);
        double memory = random.uniform_int(1, 4) * 1024;
        auto host = zone->create_host(hostname, speed);
        host->set_core_count(cores);
        auto disk = host->create_disk(hostname + "-fs", "1GBps", "1GBps");
        disk->set_property("size", "1000GiB");
        disk->set_property("mount", "/");
        // loopback link is used for intra-host communications
        const sg4::Link* loopback = zone->create_link(hostname + "-loopback", "100GBps")
                                        ->set_sharing_policy(sg4::Link::SharingPolicy::FATPIPE)
                                        ->set_latency(0)
                                        ->seal();
        zone->add_route(host->get_netpoint(), host->get_netpoint(), nullptr, nullptr, {sg4::LinkInRoute(loopback)});
        if (i == 0) {
            sg4::Actor::create("master", host, Master("master", task_count, true, scheduling_time));
            sg4::Actor::create("client", host, Client("client", task_count, master_mailbox, &random));
        }
        std::string worker_name = "worker-" + std::to_string(i);
        sg4::Actor::create(worker_name, host, Worker(worker_name, speed, cores, memory, true,
                           master_mailbox, e.host_by_name("host-0")));
    }
    // single backbone link is used for inter-host communication
    const sg4::Link* link = zone->create_link("backbone", "10GBps")
                                ->set_sharing_policy(sg4::Link::SharingPolicy::FATPIPE) // transfers use full bandwidth
                                ->set_latency("10us")
                                ->seal();
    sg4::LinkInRoute backbone(link);
    for (unsigned int i=0; i < host_count; i++) {
        std::string host1 = "host-" + std::to_string(i);
        for (unsigned int j=i+1; j < host_count; j++) {
            std::string host2 = "host-" + std::to_string(j);
            zone->add_route(e.host_by_name(host1)->get_netpoint(), e.host_by_name(host2)->get_netpoint(),
                            nullptr, nullptr, {backbone});
        }
    }
    zone->seal();

    // run simulation
    auto start = std::chrono::steady_clock::now();
    e.run();
    auto stop = std::chrono::steady_clock::now();
    auto duration = (double)(std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count()) / 1000;
    printf("Processed %d tasks on %d hosts in %.2fs (%.2f tasks/s)\n",
            task_count, host_count, e.get_clock(), task_count / e.get_clock());
    printf("Elapsed time: %.2fs\n", duration);
    printf("Scheduling time: %.2fs\n", scheduling_time);
    printf("Simulation speedup: %.2f\n", e.get_clock() / duration);

    return 0;
}
