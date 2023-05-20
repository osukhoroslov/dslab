#include <assert.h>
#include <vector>

#include "ortools/linear_solver/linear_solver.h"
#include "ortools/sat/cp_model.h"
#include "dslab-faas-estimators/include/multiknapsack.hpp"

uint64_t solve_multiknapsack(
        rust::Slice<const uint64_t> kind,
        rust::Slice<const uint64_t> cost,
        rust::Slice<const rust::Vec<uint64_t>> knapsacks,
        rust::Slice<const rust::Vec<uint64_t>> kinds) {
    size_t items = kind.size();
    assert(items == cost.size());
    size_t bins = knapsacks.size();
    uint64_t sum = 0;
    for (size_t i = 0; i < items; i++) {
        sum += cost[i];
    }
    if (knapsacks.empty()) {
        return sum;
    }
    size_t res_dim = knapsacks[0].size();
    /*using namespace operations_research::sat;
    CpModelBuilder builder;
    std::vector<std::vector<BoolVar>> assign(items);
    std::vector<BoolVar> obj_vars;
    std::vector<int64_t> obj_coeff;
    obj_vars.reserve(items * bins);
    obj_coeff.reserve(items * bins);
    for (size_t i = 0; i < items; i++) {
        assign[i].reserve(bins);
        for (size_t j = 0; j < bins; j++) {
            assign[i].push_back(builder.NewBoolVar());
            obj_vars.push_back(assign[i].back());
            obj_coeff.push_back(cost[i]);
        }
        builder.AddAtMostOne(assign[i]);
    }
    for (size_t j = 0; j < bins; j++) {
        std::vector<BoolVar> vars;
        vars.reserve(items);
        for (size_t i = 0; i < items; i++) {
            vars.push_back(assign[i][j]);
        }
        for (size_t r = 0; r < res_dim; r++) {
            std::vector<int64_t> coeff(items);
            for (size_t i = 0; i < items; i++) {
                coeff[i] = kinds[kind[i]][r];
            }
            builder.AddLessOrEqual(LinearExpr::WeightedSum(vars, coeff), knapsacks[j][r]);
        }
    }
    builder.Maximize(LinearExpr::WeightedSum(obj_vars, obj_coeff));
    SatParameters parameters;
    parameters.set_num_search_workers(8);
    parameters.set_max_time_in_seconds(10.0);
    Model model;
    model.Add(NewSatParameters(parameters));
    auto response = SolveCpModel(builder.Build(), &model);
    std::cout << "time = " << response.wall_time() << std::endl;
    if (response.status() == CpSolverStatus::FEASIBLE) {
        return 0;
    }
    assert(response.status() == CpSolverStatus::OPTIMAL);
    return sum - uint64_t(1e-6 + response.objective_value());*/
    using namespace operations_research;
    std::unique_ptr<MPSolver> solver(MPSolver::CreateSolver("CPLEX"));
    assert(solver);
    std::vector<std::vector<MPVariable*>> assign(items);
    auto obj = solver->MutableObjective();
    for (size_t i = 0; i < items; i++) {
        assign[i].reserve(bins);
        for (size_t j = 0; j < bins; j++) {
            assign[i].push_back(solver->MakeBoolVar(""));
            obj->SetCoefficient(assign[i].back(), cost[i]);
        }
        auto c = solver->MakeRowConstraint(0, 1);
        for (auto v: assign[i]) {
            c->SetCoefficient(v, 1);
        }
    }
    obj->SetMaximization();
    for (size_t j = 0; j < bins; j++) {
        std::vector<MPVariable*> vars;
        vars.reserve(items);
        for (size_t i = 0; i < items; i++) {
            vars.push_back(assign[i][j]);
        }
        for (size_t r = 0; r < res_dim; r++) {
            auto c = solver->MakeRowConstraint(0, knapsacks[j][r]);
            for (size_t i = 0; i < items; i++) {
                c->SetCoefficient(vars[i], kinds[kind[i]][r]);
            }
        }
    }
    MPSolverParameters params{};
    params.SetDoubleParam(params.RELATIVE_MIP_GAP, 1e-6);
    const auto status = solver->Solve(params);
    assert(status == MPSolver::OPTIMAL);
    //std::cout << "time = " << solver->wall_time() / 1000.0 << std::endl;
    return sum - uint64_t(1e-6 + obj->Value());
}
