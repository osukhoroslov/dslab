#include <assert.h>
#include <vector>

#include "ortools/linear_solver/linear_solver.h"
#include "ortools/sat/cp_model.h"
#include "dslab-faas-estimators/include/lp_lower_cplex.hpp"

uint64_t lp_lower_bound_cp_sat(
        rust::Slice<const uint64_t> arrival,
        rust::Slice<const uint64_t> duration,
        rust::Slice<const uint64_t> app,
        rust::Slice<const uint64_t> app_coldstart,
        uint64_t keepalive) {
    size_t n = arrival.size();
    assert(n == duration.size());
    assert(n == app.size());
    using namespace operations_research::sat;
    CpModelBuilder builder;
    std::vector<std::vector<BoolVar>> seq_var(n);
    std::vector<std::vector<size_t>> can(n);
    std::vector<BoolVar> first(n);
    std::vector<IntVar> start(n);
    LinearExpr obj;
    uint64_t obj_shift = 0;
    int64_t horizon = 10000000;
    int64_t bigM = 2 * horizon;
    for (size_t i = 0; i < n; i++) {
        obj_shift += arrival[i];
        first[i] = builder.NewBoolVar();
        obj += LinearExpr::Term(first[i], app_coldstart[app[i]]);
        start[i] = builder.NewIntVar(operations_research::Domain(arrival[i], horizon));
        obj += start[i];
        for (size_t j = 0; j < i; j++) {
            if (app[i] == app[j]) {
                can[i].push_back(j);
                seq_var[i].push_back(builder.NewBoolVar());
                LinearExpr lb;
                lb -= start[i];
                lb += start[j];
                lb += LinearExpr::Term(first[j], app_coldstart[app[j]]);
                lb += LinearExpr::Term(seq_var[i].back(), bigM);
                builder.AddLessOrEqual(lb, bigM - (int64_t)duration[j]);
                LinearExpr ub;
                ub -= start[i];
                ub += start[j];
                ub += LinearExpr::Term(first[j], app_coldstart[app[j]]);
                ub -= LinearExpr::Term(seq_var[i].back(), bigM);
                builder.AddGreaterOrEqual(ub, -bigM - int64_t(duration[j] + keepalive));
            }
        }
        LinearExpr c = first[i];
        for (auto v: seq_var[i]) {
            c += v;
        }
        builder.AddEquality(c, 1);
    }
    for (size_t i = 0; i < n; i++) {
        std::vector<BoolVar> nxt;
        for (size_t j = i + 1; j < n; j++) {
            if (app[i] != app[j])
                continue;
            auto it = std::find(can[j].begin(), can[j].end(), i);
            if (it != can[j].end()) {
                size_t pos = it - can[j].begin();
                nxt.push_back(seq_var[j][pos]);
            }
        }
        if (!nxt.empty()) {
            builder.AddLinearConstraint(LinearExpr::Sum(nxt), operations_research::Domain(0, 1));
        }
    }
    builder.Minimize(obj);
    SatParameters parameters;
    parameters.set_num_search_workers(8);
    Model model;
    model.Add(NewSatParameters(parameters));
    const auto response = SolveCpModel(builder.Build(), &model);
    assert(response.status() == CpSolverStatus::OPTIMAL);
    int64_t obj_val = int64_t(response.objective_value() + 1e-6);
    std::cerr << "CP-SAT OBJ = " << obj_val << std::endl;
    return obj_val - obj_shift;
}

uint64_t lp_lower_bound(
        rust::Slice<const uint64_t> arrival,
        rust::Slice<const uint64_t> duration,
        rust::Slice<const uint64_t> app,
        rust::Slice<const uint64_t> app_coldstart,
        uint64_t keepalive,
        uint64_t init_estimate) {
    const double scale = 1000;
    using namespace operations_research;
    std::unique_ptr<MPSolver> solver(MPSolver::CreateSolver("CPLEX"));
    assert(solver);
    size_t n = arrival.size();
    assert(n == duration.size());
    assert(n == app.size());
    std::vector<std::vector<MPVariable*>> same(n);
    std::vector<std::vector<size_t>> can(n);
    std::vector<MPVariable*> first(n);
    std::vector<MPVariable*> start(n);
    auto obj = solver->MutableObjective();
    uint64_t obj_shift = std::accumulate(arrival.begin(), arrival.end(), 0ll);
    MPConstraint* obj_estimate = nullptr;
    if (init_estimate + 1 != 0) {
        obj_estimate = solver->MakeRowConstraint(0, (init_estimate + obj_shift) / scale);
    }
    const auto infinity = solver->infinity();
    double base_horizon = 0;
    for (size_t i = 0; i < n; i++) { 
        base_horizon = std::max(base_horizon, double(arrival[i] + app_coldstart[app[i]]));
    }
    double horizon = base_horizon / scale;
    double bigM = 2 * horizon;
    for (size_t i = 0; i < n; i++) {
        first[i] = solver->MakeBoolVar("");
        obj->SetCoefficient(first[i], app_coldstart[app[i]] / scale);
        //start[i] = solver->MakeNumVar(arrival[i]/scale, (arrival[i] + app_coldstart[app[i]]) / scale, "");
        start[i] = solver->MakeNumVar(arrival[i]/scale, horizon, "");
        obj->SetCoefficient(start[i], 1);
        if (init_estimate + 1 != 0) {
            obj_estimate->SetCoefficient(start[i], 1);
            obj_estimate->SetCoefficient(first[i], app_coldstart[app[i]] / scale);
        }
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
    /*for (size_t i = 0; i < n; i++) {
        auto c = solver->MakeRowConstraint(0, 1);
        for (size_t j = i + 1; j < n; j++) {
            if (app[i] != app[j])
                continue;
            auto it = std::find(can[j].begin(), can[j].end(), i);
            if (it != can[j].end()) {
                size_t pos = it - can[j].begin();
                c->SetCoefficient(seq_var[j][pos], 1);
            }
        }
    }*/
    //std::cerr << "TOTAL VARS = " << solver->NumVariables() << std::endl;
    obj->SetMinimization();
    MPSolverParameters params{};
    params.SetDoubleParam(params.RELATIVE_MIP_GAP, 1.0 / (2 * base_horizon));
    const auto status = solver->Solve(params);
    assert(status == MPSolver::OPTIMAL);
    std::cout << "EXEC TIME = " << std::fixed << double(solver->wall_time()) / 1000.0 << std::endl;
    //std::cerr << "RAW OBJ = " << std::fixed << obj->Value() << std::endl;
    //std::cerr << "OBJ = " << int64_t(obj->Value() * scale) << std::endl;
    //uint64_t cpsat = lp_lower_bound_cp_sat(arrival, duration, app, app_coldstart, keepalive);
    //std::cerr << "CMP TO " << cpsat << std::endl;
    return uint64_t(obj->Value() * scale + 1e-6) - obj_shift;
}
