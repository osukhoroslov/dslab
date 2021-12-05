use compute::computation::Computation;

pub enum TaskState {
    Queue,
    Ready,
    Completed,
}

#[derive(Debug)]
pub struct Edge {
    source: usize,
    target: usize,
    weight: u64,
}

impl Edge {
    pub fn new(source: usize, target: usize, weight: u64) -> Self {
        Self {
            source: source,
            target: target,
            weight: weight,
        }
    }
}

pub struct Workflow {
    pub tasks: Vec<Computation>,
    edges: Vec<Edge>,
    outcoming: Vec<Vec<usize>>,
    incoming: Vec<Vec<usize>>,
    pub ready_tasks: Vec<usize>,
    ready_tasks_index: Vec<usize>,
    task_state: Vec<TaskState>,
    incoming_balance: Vec<usize>,
    completed_count: usize,
}

impl Workflow {
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            edges: Vec::new(),
            outcoming: Vec::new(),
            incoming: Vec::new(),
            ready_tasks: Vec::new(),
            ready_tasks_index: Vec::new(),
            task_state: Vec::new(),
            incoming_balance: Vec::new(),
            completed_count: 0,
        }
    }

    pub fn add_task(&mut self, computation: Computation) {
        self.ready_tasks_index.push(self.ready_tasks.len());
        self.ready_tasks.push(self.tasks.len());
        self.task_state.push(TaskState::Ready);
        self.incoming_balance.push(0);
        self.tasks.push(computation);
        self.outcoming.push(Vec::new());
        self.incoming.push(Vec::new());
    }

    pub fn add_edge(&mut self, edge: Edge) {
        self.outcoming[edge.source].push(self.edges.len());
        self.incoming[edge.target].push(self.edges.len());
        self.incoming_balance[edge.target] += 1;
        if let TaskState::Ready = self.task_state[edge.target] {
            self.task_state[edge.target] = TaskState::Queue;
            self.unready(edge.target);
        }
        self.edges.push(edge);
    }

    pub fn size(&self) -> usize {
        self.tasks.len()
    }

    pub fn validate(&self) -> bool {
        let order = self.topsort();
        let mut index = vec![0 as usize; self.size()];
        for i in 0..self.size() {
            index[order[i]] = i;
        }
        for edge in self.edges.iter() {
            if index[edge.source] > index[edge.target] {
                return false;
            }
        }
        true
    }

    pub fn topsort(&self) -> Vec<usize> {
        let mut used = vec![false; self.size()];
        let mut result: Vec<usize> = Vec::new();
        for v in 0..self.size() {
            if !used[v] {
                self.dfs(v, &mut used, &mut result);
            }
        }
        result.reverse();
        result
    }

    pub fn mark_completed(&mut self, task: usize) {
        self.completed_count += 1;
        self.unready(task);
        self.task_state[task] = TaskState::Completed;
        for &edge_index in self.outcoming[task].iter() {
            let to = self.edges[edge_index].target;
            self.incoming_balance[to] -= 1;
            if self.incoming_balance[to] == 0 {
                self.task_state[to] = TaskState::Ready;
                self.ready_tasks_index[to] = self.ready_tasks.len();
                self.ready_tasks.push(to);
            }
        }
    }

    pub fn completed(&self) -> bool {
        self.completed_count == self.size()
    }

    fn dfs(&self, v: usize, used: &mut Vec<bool>, result: &mut Vec<usize>) {
        used[v] = true;
        for &edge_index in self.outcoming[v].iter() {
            let to = self.edges[edge_index].target;
            if !used[to] {
                self.dfs(to, used, result);
            }
        }
        result.push(v);
    }

    fn unready(&mut self, task: usize) {
        let last_index = self.ready_tasks.len() - 1;
        self.ready_tasks_index[self.ready_tasks[last_index]] = self.ready_tasks_index[task];
        self.ready_tasks.swap(self.ready_tasks_index[task], last_index);
        self.ready_tasks.pop();
    }
}
