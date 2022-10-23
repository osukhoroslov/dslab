use std::boxed::Box;

use rand::prelude::*;
use rand_pcg::Pcg64;

use crate::ls::annealing::AnnealingSchedule;
use crate::ls::common::*;

pub trait AcceptanceCriterion {
    fn accept(&mut self, old: &State, new: &State, goal: &OptimizationGoal, rng: &mut Pcg64) -> bool;
    /// resets the criterion before the next local search run
    fn reset(&mut self);
}

pub struct HillClimbAcceptanceCriterion {}

impl AcceptanceCriterion for HillClimbAcceptanceCriterion {
    fn accept(&mut self, old: &State, new: &State, goal: &OptimizationGoal, _rng: &mut Pcg64) -> bool {
        goal.is_better(new.objective, old.objective)
    }

    fn reset(&mut self) {}
}

pub struct SimulatedAnnealingAcceptanceCriterion {
    annealing: Box<dyn AnnealingSchedule>,
}

impl SimulatedAnnealingAcceptanceCriterion {
    pub fn new(annealing: Box<dyn AnnealingSchedule>) -> Self {
        Self { annealing }
    }
}

impl AcceptanceCriterion for SimulatedAnnealingAcceptanceCriterion {
    fn accept(&mut self, old: &State, new: &State, goal: &OptimizationGoal, rng: &mut Pcg64) -> bool {
        if goal.is_better(new.objective, old.objective) {
            true
        } else {
            rng.gen_bool((-f64::abs(new.objective - old.objective) / self.annealing.get_temperature()).exp())
        }
    }

    fn reset(&mut self) {
        self.annealing.reset();
    }
}
