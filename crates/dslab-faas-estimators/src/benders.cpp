#include <algorithm>
#include <assert.h>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "ortools/linear_solver/linear_solver.h"
#include "ortools/sat/cp_model.h"
#include "dslab-faas-estimators/include/benders.hpp"

std::vector<std::vector<std::tuple<size_t, int64_t, int64_t>>> overload_cuts(std::vector<int64_t> start,
           std::vector<int64_t> len,
           std::vector<size_t> app_id,
           rust::Slice<const rust::Vec<uint64_t>> app_resources,
           rust::Slice<const rust::Vec<uint64_t>> host_resources) {
    using namespace operations_research;
    size_t n = start.size();
    assert(n == len.size());
    assert(n == app_id.size());
    size_t h = host_resources.size();
    size_t res = host_resources[0].size();
    std::vector<std::tuple<int64_t, int, size_t>> events;
    events.reserve(2 * n);
    for (size_t i = 0; i < n; i++) {
        events.emplace_back(start[i], 1, i);
        events.emplace_back(start[i] + len[i], 0, i);
    }
    std::sort(events.begin(), events.end());
    size_t ptr = 0;
    std::unordered_set<size_t> alive;
    std::vector<std::vector<std::tuple<size_t, int64_t, int64_t>>> result;
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
        if (alive.empty()) {
            ptr = ptr2;
            continue;
        }
        int64_t l = std::get<0>(events[ptr]);
        int64_t r = std::get<0>(events[ptr2]);
        std::unique_ptr<MPSolver> solver(MPSolver::CreateSolver("CPLEX"));
        assert(solver);
        std::vector<size_t> items(alive.begin(), alive.end());
        std::sort(items.begin(), items.end());
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
        }
        for (size_t j = 0; j < h; j++) {
            for (size_t k = 0; k < res; k++) {
                auto c = solver->MakeRowConstraint(0, host_resources[j][k]);
                for (size_t i = 0; i < items.size(); i++) {
                    c->SetCoefficient(assign[i][j], app_resources[app_id[items[i]]][k]);
                }
            }
        }
        auto obj = solver->MutableObjective();
        for (size_t i = 0; i < items.size(); i++) {
            obj->SetCoefficient(active[i], 1);
        }
        obj->SetMaximization();
        const auto status = solver->Solve();
        assert(status == MPSolver::OPTIMAL);
        int val = size_t(obj->Value() + 0.1);
        if (val == active.size()) {
            ptr = ptr2;
            continue;
        }
        std::vector<std::tuple<size_t, int64_t, int64_t>> cut;
        cut.reserve(val + 1);
        for (size_t i = 0; i < items.size(); i++) {
            if (active[i]->solution_value() < 0.5) {
                cut.emplace_back(items[i], std::max((int64_t)0, r - len[items[i]]), r - 1);
                break;
            }
        }
        for (size_t i = 0; i < items.size(); i++) {
            if (active[i]->solution_value() > 0.5) {
                cut.emplace_back(items[i], std::max((int64_t)0, r - len[items[i]]), r - 1);
            }
        }
        result.push_back(cut);
        //std::cout << "EXEC TIME = " << std::fixed << double(solver->wall_time()) / 1000.0 << std::endl;
        ptr = ptr2;
    }
    return result;
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
        uint64_t keepalive,
        uint64_t iterations,
        uint64_t max_cuts) {
    const double scale = 1;
    size_t n = arrival.size();
    assert(n == duration.size());
    assert(n == app.size());
    using namespace operations_research;
    std::deque<std::vector<std::pair<int, std::pair<int64_t, int64_t>>>> cuts;
    std::vector<std::set<std::pair<int64_t, int64_t>>> aux_vars_ints(n);
    std::vector<std::map<std::pair<int64_t, int64_t>, int>> aux_vars_count(n);
    uint64_t best = 0;
    for (uint64_t iter = 0; iter < iterations; iter++) {
        while (cuts.size() > max_cuts) {
            auto cut = cuts.front();
            for (auto [i, pair]: cut) {
                if (!(--aux_vars_count[i][pair])) {
                    aux_vars_ints[i].erase(pair);
                }
            }
            cuts.pop_front();
        }
        std::cerr << "got " << cuts.size() << " cuts." << std::endl;
        std::unique_ptr<MPSolver> solver(MPSolver::CreateSolver("CPLEX"));
        assert(solver);
        std::vector<std::vector<MPVariable*>> same(n);
        std::vector<std::vector<size_t>> can(n);
        std::vector<std::vector<MPVariable*>> rev_same(n);
        std::vector<std::vector<size_t>> rev_can(n);
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
        std::vector<std::map<std::pair<int64_t, int64_t>, MPVariable*>> aux_vars(n);
        for (size_t i = 0; i < n; i++) {
            first[i] = solver->MakeBoolVar("");
            obj->SetCoefficient(first[i], app_coldstart[app[i]] / scale);
            for (auto pair: aux_vars_ints[i]) {
                auto var = solver->MakeBoolVar("");
                aux_vars[i][pair] = var;
            }
            //start[i] = solver->MakeNumVar(arrival[i]/scale, (arrival[i] + app_coldstart[app[i]]) / scale, "");
            start[i] = solver->MakeIntVar(arrival[i]/scale, horizon, "");
            for (auto [pair, v]: aux_vars[i]) {
                auto [l, r] = pair;
                auto delta = solver->MakeBoolVar("");
                auto c0 = solver->MakeRowConstraint(l - bigM, infinity);
                c0->SetCoefficient(v, -bigM);
                c0->SetCoefficient(start[i], 1);
                auto c1 = solver->MakeRowConstraint(-infinity, r + bigM);
                c1->SetCoefficient(v, bigM);
                c1->SetCoefficient(start[i], 1);
                auto c2 = solver->MakeRowConstraint(1 + r, infinity);
                c2->SetCoefficient(start[i], 1);
                c2->SetCoefficient(v, bigM);
                c2->SetCoefficient(delta, bigM);
                auto c3 = solver->MakeRowConstraint(-infinity, bigM + l - 1);
                c3->SetCoefficient(start[i], 1);
                c3->SetCoefficient(v, -bigM);
                c3->SetCoefficient(delta, bigM);
            }
            obj->SetCoefficient(start[i], 1);
            for (size_t j = 0; j < i; j++) {
                if (app[i] == app[j] && arrival[j] + duration[j] < arrival[i] + app_coldstart[app[i]]) { //&&
                        //arrival[j] + duration[j] + 2 * app_coldstart[app[i]] + keepalive > arrival[i])
                    can[i].push_back(j);
                    same[i].push_back(solver->MakeBoolVar(""));
                    rev_can[j].push_back(i);
                    rev_same[j].push_back(same[i].back());
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
        for (size_t i = 0; i < n; i++) {
            for (size_t p1 = 0; p1 < can[i].size(); p1++) {
                for (size_t p2 = 0; p2 < rev_can[i].size(); p2++) {
                    auto j = can[i][p1], k = rev_can[i][p2];
                    size_t p3 = std::find(can[k].begin(), can[k].end(), j) - can[k].begin();
                    if (p3 != can[k].size()) {
                        auto c = solver->MakeRowConstraint(-infinity, 1);
                        c->SetCoefficient(same[k][p3], -1);
                        c->SetCoefficient(same[i][p1], 1);
                        c->SetCoefficient(rev_same[i][p2], 1);
                    }
                }
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
        params.SetDoubleParam(params.RELATIVE_MIP_GAP, 1e-7);
        const auto status = solver->Solve(params);
        assert(status == MPSolver::OPTIMAL);
        std::cout << "EXEC TIME = " << std::fixed << double(solver->wall_time()) / 1000.0 << std::endl;
        auto val = uint64_t(obj->Value() * scale + 1e-6) - obj_shift;
        std::cout << "ITER OBJ = " << val << std::endl;
        // time to check cuts
        for (const auto& cut: cuts) {
            int viol = 0;
            for (auto [i, val]: cut) {
                if (first[i]->solution_value() > 0.5)
                    ++viol;
                bool vvar = false, vrange = false;
                if (aux_vars[i][val]->solution_value() > 0.5)
                    vvar = true;
                int64_t pos = int64_t(start[i]->solution_value() + 0.1);
                if (pos >= val.first && pos <= val.second)
                    vrange = true;
                assert(vrange == vvar);
                if (vvar)
                    ++viol;
            }
            assert(viol < 2 * cut.size());
        }
        best = std::max(best, val);
        std::vector<size_t> begins;
        std::vector<int64_t> slave_start, slave_len;
        std::vector<size_t> slave_app;
        for (size_t i = 0; i < n; i++) {
            if (first[i]->solution_value() > 0.5) {
                begins.push_back(i);
                slave_start.push_back(int64_t(start[i]->solution_value() + 0.1));
                slave_len.push_back(keepalive + app_coldstart[app[i]] + duration[i]);
                slave_app.push_back(app[i]);
            }
        }
        auto over = overload_cuts(slave_start, slave_len, slave_app, app_resources, host_resources);
        if (!over.empty()) {
            for (auto cut: over) {
                //std::cout << "new cut with size " << cut.size() << std::endl;
                for (auto [i, lb, ub]: cut) {
                    ++aux_vars_count[begins[i]][std::make_pair(lb, ub)];
                    aux_vars_ints[begins[i]].insert(std::make_pair(lb, ub));
                }
                cuts.emplace_back();
                cuts.back().resize(cut.size());
                std::transform(cut.begin(), cut.end(), cuts.back().begin(), [&](const auto& item) {
                    auto [id, l, r] = item;
                    assert(l <= r);
                    return std::make_pair(begins[id], std::make_pair(l, r));
                });
            }
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
                aux_vars_ints[i].insert(std::make_pair(val, val));
                ++aux_vars_count[i][std::make_pair(val, val)];
                cuts.back().emplace_back(i, std::make_pair(val, val));
            }
        }
    }
    return best;
}
