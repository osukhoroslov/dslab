use std::boxed::Box;
use std::time::Instant;

use rand::prelude::*;
use rand_pcg::Pcg64;

use crate::ls::accept::AcceptanceCriterion;
use crate::ls::common::*;
use crate::ls::initial::InitialSolutionGenerator;
use crate::ls::neighborhood::Neighborhood;

pub struct LocalSearch {
    acceptance: Box<dyn AcceptanceCriterion>,
    generator: Box<dyn InitialSolutionGenerator>,
    goal: OptimizationGoal,
    neighborhood: Box<dyn Neighborhood>,
    rng: Pcg64,
    timeout: f64,
}

impl LocalSearch {
    pub fn new(
        acceptance: Box<dyn AcceptanceCriterion>,
        goal: OptimizationGoal,
        generator: Box<dyn InitialSolutionGenerator>,
        neighborhood: Box<dyn Neighborhood>,
        seed: u64,
        timeout: f64,
    ) -> Self {
        Self {
            acceptance,
            generator,
            goal,
            neighborhood,
            rng: Pcg64::seed_from_u64(seed),
            timeout,
        }
    }

    pub fn run(&mut self, instance: &Instance, init: Option<State>) -> State {
        let mut curr = init.unwrap_or(self.generator.generate(instance, &mut self.rng));
        let mut best = curr.clone();
        let begin = Instant::now();
        loop {
            let next = self.neighborhood.step(&curr, instance, &mut self.rng);
            if self.goal.is_better(next.objective, best.objective) {
                best = next.clone();
            }
            if self.acceptance.accept(&curr, &next, &self.goal, &mut self.rng) {
                curr = next;
            }
            if Instant::now().duration_since(begin).as_secs_f64() > self.timeout {
                break;
            }
        }
        self.acceptance.reset();
        self.neighborhood.reset();
        best
    }
}

pub struct IteratedLocalSearch {
    acceptance: Box<dyn AcceptanceCriterion>,
    generator: Box<dyn InitialSolutionGenerator>,
    goal: OptimizationGoal,
    inner: LocalSearch,
    neighborhood: Box<dyn Neighborhood>,
    rng: Pcg64,
    timeout: f64,
}

impl IteratedLocalSearch {
    pub fn new(
        acceptance: Box<dyn AcceptanceCriterion>,
        goal: OptimizationGoal,
        generator: Box<dyn InitialSolutionGenerator>,
        inner: LocalSearch,
        neighborhood: Box<dyn Neighborhood>,
        seed: u64,
        timeout: f64,
    ) -> Self {
        Self {
            acceptance,
            generator,
            goal,
            inner,
            neighborhood,
            rng: Pcg64::seed_from_u64(seed),
            timeout,
        }
    }

    pub fn run(&mut self, instance: &Instance, init: Option<State>) -> State {
        let mut curr = self.inner.run(
            instance,
            Some(init.unwrap_or(self.generator.generate(instance, &mut self.rng))),
        );
        let mut best = curr.clone();
        let begin = Instant::now();
        loop {
            let next = self
                .inner
                .run(instance, Some(self.neighborhood.step(&curr, instance, &mut self.rng)));
            if self.goal.is_better(next.objective, best.objective) {
                best = next.clone();
            }
            if self.acceptance.accept(&curr, &next, &self.goal, &mut self.rng) {
                curr = next;
            }
            if Instant::now().duration_since(begin).as_secs_f64() > self.timeout {
                break;
            }
        }
        self.acceptance.reset();
        self.neighborhood.reset();
        best
    }
}
