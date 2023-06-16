#include <algorithm>
#include <assert.h>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "ortools/linear_solver/linear_solver.h"
#include "ortools/sat/cp_model.h"
#include "dslab-faas-estimators/include/benders.hpp"

std::vector<std::vector<size_t>> overload_cuts(std::vector<int64_t> start,
           std::vector<int64_t> len,
           std::vector<size_t> app_id,
           rust::Slice<const rust::Vec<uint64_t>> app_resources,
           rust::Slice<const rust::Vec<uint64_t>> host_resources) {
    using namespace operations_research;
    size_t n = start.size();
    assert(n == len.size());
    assert(n == app_id.size());
    size_t h = host_resources.size();
    size_t r = host_resources[0].size();
    std::vector<std::tuple<int, int, int>> events;
    events.reserve(2 * n);
    for (size_t i = 0; i < n; i++) {
        events.emplace_back(start[i], 1, i);
        events.emplace_back(start[i] + len[i], 0, i);
    }
    std::sort(events.begin(), events.end());
    size_t ptr = 0;
    std::unordered_set<int> alive;
    std::vector<size_t> best_cut;
    while (ptr < events.size()) {
        size_t ptr2 = ptr;
        while (ptr2 < events.size() && std::get<0>(events[ptr]) == std::get<0>(events[ptr2])) {
            auto [t, type, id] = events[ptr2++];
            if (type) {
                alive.insert(id);
            } else {
                alive.erase(id);
            }
        }
        std::unique_ptr<MPSolver> solver(MPSolver::CreateSolver("CPLEX"));
        assert(solver);
        std::vector<int> items(alive.begin(), alive.end());
        std::vector<std::vector<MPVariable*>> assign(items.size());
        std::vector<MPVariable*> active;
        active.reserve(items.size());
        for (size_t i = 0; i < items.size(); i++) {
            active.push_back(solver->MakeBoolVar(""));
            assign[i].reserve(h);
            for (size_t j = 0; j < h; j++) {
                assign[i].push_back(solver->MakeBoolVar(""));
            }
            auto c = solver->MakeRowConstraint(0, 0);
            c->SetCoefficient(active[i], -1);
            for (auto var: assign[i]) {
                c->SetCoefficient(var, 1);
            }
            for (size_t j = 0; j < h; j++) {
                for (size_t k = 0; k < r; k++) {
                    auto c = solver->MakeRowConstraint(0, host_resources[j][k]);
                    for (size_t i = 0; i < n; i++) {
                        c->SetCoefficient(assign[i][j], app_resources[app[i]][k]);
                    }
                }
            }
            auto obj = solver->MutableObjective();
            for (size_t i = 0; i < n; i++) {
                obj->SetCoefficient(active[i], 1);
            }
            obj->SetMaximization();
            const auto status = solver->Solve();
            assert(status == MPSolver::OPTIMAL);
            //std::cout << "EXEC TIME = " << std::fixed << double(solver->wall_time()) / 1000.0 << std::endl;
        }
        ptr = ptr2;
    }
}


std::vector<size_t> slave(std::vector<int64_t> start,
           std::vector<int64_t> len,
           std::vector<size_t> app_id,
           rust::Slice<const rust::Vec<uint64_t>> app_resources,
           rust::Slice<const rust::Vec<uint64_t>> host_resources) {
    size_t n = start.size();
    assert(n == len.size());
    assert(n == app_id.size());
    size_t h = host_resources.size();
    size_t r = host_resources[0].size();
    using namespace operations_research::sat;
    CpModelBuilder builder;
    std::vector<std::vector<IntervalVar>> interval(n);
    std::vector<std::vector<BoolVar>> assign(n);
    std::vector<BoolVar> active;
    active.reserve(n);
    for (size_t i = 0; i < n; i++) {
        active.push_back(builder.NewBoolVar().WithName(std::to_string(i)));
        assign[i].reserve(h);
        interval[i].reserve(h);
        for (size_t j = 0; j < h; j++) {
            assign[i].push_back(builder.NewBoolVar());
            interval[i].push_back(builder.NewOptionalFixedSizeIntervalVar(start[i], len[i], assign[i][j]));
        }
        builder.AddEquality(LinearExpr::Sum(assign[i]), active[i]);
    }
    for (size_t j = 0; j < h; j++) {
        for (size_t k = 0; k < r; k++) {
            auto cum = builder.AddCumulative(host_resources[j][k]);
            for (size_t i = 0; i < n; i++) {
                cum.AddDemand(interval[i][j], app_resources[app_id[i]][k]);
            }
        }
    }
    builder.Maximize(LinearExpr::Sum(active));
    //builder.AddAssumptions(active);
    SatParameters parameters;
    parameters.set_num_search_workers(8);
    Model model;
    model.Add(NewSatParameters(parameters));
    auto response = SolveCpModel(builder.Build(), &model);
    std::cout << "slave time = " << response.wall_time() << std::endl;
    /*if (response.status() == CpSolverStatus::FEASIBLE || response.status() == CpSolverStatus::OPTIMAL) {
        return {};
    }*/
    std::vector<size_t> result;
    /*for (int idx: response.sufficient_assumptions_for_infeasibility()) {
        result.push_back(std::stol(builder.GetBoolVarFromProtoIndex(idx).Name()));
    }*/
    bool unsat = false;
    for (size_t i = 0; i < n; i++) {
        if (!SolutionIntegerValue(response, active[i])) {
            result.push_back(i);
            unsat = true;
            break;
        }
    }
    if (!unsat) {
        return {};
    }
    for (size_t i = 0; i < n; i++) {
        if (SolutionIntegerValue(response, active[i])) {
            result.push_back(i);
        }
    }
    return result;
}

uint64_t benders(
        rust::Slice<const uint64_t> arrival,
        rust::Slice<const uint64_t> duration,
        rust::Slice<const uint64_t> app,
        rust::Slice<const uint64_t> app_coldstart,
        rust::Slice<const rust::Vec<uint64_t>> app_resources,
        rust::Slice<const rust::Vec<uint64_t>> host_resources,
        uint64_t keepalive) {
    const double scale = 1;
    size_t n = arrival.size();
    assert(n == duration.size());
    assert(n == app.size());
    uint64_t base_len = keepalive + *std::max_element(duration.begin(), duration.end());
    using namespace operations_research;
    std::vector<std::vector<std::pair<int, std::pair<int, int>>>> cuts;
    std::vector<std::unordered_set<std::pair<int, int>>> aux_vars_ints(n);
    uint64_t best = 0;
    for (int iter = 0; iter < 100; iter++) {
        std::unique_ptr<MPSolver> solver(MPSolver::CreateSolver("CPLEX"));
        assert(solver);
        std::vector<std::vector<MPVariable*>> same(n);
        std::vector<std::vector<size_t>> can(n);
        std::vector<MPVariable*> first(n);
        std::vector<MPVariable*> start(n);
        auto obj = solver->MutableObjective();
        uint64_t obj_shift = std::accumulate(arrival.begin(), arrival.end(), 0ll);
        MPConstraint* obj_estimate = nullptr;
        const auto infinity = solver->infinity();
        int64_t base_horizon = 0;
        for (size_t i = 0; i < n; i++) { 
            base_horizon = std::max(base_horizon, int64_t(arrival[i] + app_coldstart[app[i]]));
        }
        double horizon = double(base_horizon) / scale;
        double bigM = 2 * horizon;
        std::vector<std::unordered_map<int, MPVariable*>> aux_vars(n);
        for (size_t i = 0; i < n; i++) {
            first[i] = solver->MakeBoolVar("");
            obj->SetCoefficient(first[i], app_coldstart[app[i]] / scale);
            for (int t: aux_vars_ints[i]) {
                auto var = solver->MakeBoolVar("");
                aux_vars[i][t] = var;
            }
            //start[i] = solver->MakeNumVar(arrival[i]/scale, (arrival[i] + app_coldstart[app[i]]) / scale, "");
            start[i] = solver->MakeIntVar(arrival[i]/scale, horizon, "");
            for (auto [t, v]: aux_vars[i]) {
                auto delta = solver->MakeBoolVar("");
                auto c0 = solver->MakeRowConstraint(t - bigM, infinity);
                c0->SetCoefficient(v, -bigM);
                c0->SetCoefficient(start[i], 1);
                auto c1 = solver->MakeRowConstraint(-infinity, t + bigM);
                c1->SetCoefficient(v, bigM);
                c1->SetCoefficient(start[i], -1);
                auto c2 = solver->MakeRowConstraint(1 + t, infinity);
                c2->SetCoefficient(start[i], 1);
                c2->SetCoefficient(v, 1);
                c2->SetCoefficient(delta, bigM);
                auto c3 = solver->MakeRowConstraint(-infinity, bigM + t - 1);
                c3->SetCoefficient(start[i], 1);
                c3->SetCoefficient(v, -1);
                c3->SetCoefficient(delta, bigM);
            }
            obj->SetCoefficient(start[i], 1);
            for (size_t j = 0; j < i; j++) {
                if (app[i] == app[j] && arrival[j] + duration[j] < arrival[i] + app_coldstart[app[i]]) { //&&
                        //arrival[j] + duration[j] + 2 * app_coldstart[app[i]] + keepalive > arrival[i])
                    can[i].push_back(j);
                    same[i].push_back(solver->MakeBoolVar(""));
                    auto lb = solver->MakeRowConstraint(-infinity, bigM - duration[j] / scale);
                    lb->SetCoefficient(start[i], -1);
                    lb->SetCoefficient(start[j], 1);
                    lb->SetCoefficient(first[j], app_coldstart[app[j]] / scale);
                    lb->SetCoefficient(same[i].back(), bigM);
                    auto ub = solver->MakeRowConstraint(-bigM - duration[j] / scale - keepalive / scale, infinity);
                    ub->SetCoefficient(start[i], -1);
                    ub->SetCoefficient(start[j], 1);
                    ub->SetCoefficient(first[j], app_coldstart[app[j]] / scale);
                    ub->SetCoefficient(same[i].back(), -bigM);
                }
            }
            auto from = solver->MakeRowConstraint(1, infinity);
            auto to = solver->MakeRowConstraint(0, bigM);
            from->SetCoefficient(first[i], 1);
            to->SetCoefficient(first[i], bigM);
            for (auto v: same[i]) {
                from->SetCoefficient(v, 1);
                to->SetCoefficient(v, 1);
            }
        }
        for (const auto& cut: cuts) {
            auto c = solver->MakeRowConstraint(0, cut.size() * 2 - 1);
            for (auto [i, val]: cut) {
                c->SetCoefficient(first[i], 1);
                c->SetCoefficient(aux_vars[i][val], 1);
            }
        }
        obj->SetMinimization();
        MPSolverParameters params{};
        //params.SetDoubleParam(params.RELATIVE_MIP_GAP, 1.0 / (2 * base_horizon));
        params.SetDoubleParam(params.RELATIVE_MIP_GAP, 1e-8);
        const auto status = solver->Solve(params);
        assert(status == MPSolver::OPTIMAL);
        std::cout << "EXEC TIME = " << std::fixed << double(solver->wall_time()) / 1000.0 << std::endl;
        auto val = uint64_t(obj->Value() * scale + 1e-6) - obj_shift;
        std::cout << "ITER OBJ = " << val << std::endl;
        best = std::max(best, val);
        std::vector<size_t> begins;
        std::vector<int64_t> slave_start, slave_len;
        std::vector<size_t> slave_app;
        for (size_t i = 0; i < n; i++) {
            if (first[i]->solution_value() > 0.5) {
                begins.push_back(i);
                slave_start.push_back(int64_t(start[i]->solution_value() + 0.1));
                slave_len.push_back(base_len + app_coldstart[app[i]]);
                slave_app.push_back(app[i]);
            }
        }
        auto over = overload_cuts(slave_start, slave_len, slave_app, app_resources, host_resources);
        if (!over.empty()) {

            continue;
        }
        auto unsat = slave(slave_start, slave_len, slave_app, app_resources, host_resources);
        if (unsat.empty()) {
            std::cout << "WOW SOLVED" << std::endl;
            break;
        } else {
            std::cout << "unsat core has " << unsat.size() << " conts out of " << begins.size() << std::endl;
            cuts.emplace_back();
            for (size_t j: unsat) {
                size_t i = begins[j];
                int64_t val = slave_start[j];
                aux_vars_ints[i].insert(val);
                cuts.back().emplace_back(i, val);
            }
        }
    }
    return best;
}
