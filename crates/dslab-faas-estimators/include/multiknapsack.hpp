#pragma once
#include "rust/cxx.h"

// returns sum(cost) - opt where opt is the cost of the optimum solution.
uint64_t solve_multiknapsack(
        rust::Slice<const uint64_t> kind,
        rust::Slice<const uint64_t> cost,
        rust::Slice<const rust::Vec<uint64_t>> knapsacks,
        rust::Slice<const rust::Vec<uint64_t>> kinds);
