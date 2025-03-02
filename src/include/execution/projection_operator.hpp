#pragma once

#include "execution/expression/projection.hpp"
#include "execution/operator.hpp"

namespace babydb {

/**
 * Projection Operator
 * If choose update_in_place(default), the output schema is the same as the input.
 * Otherwise the output schema is the same as the projection functions.
 */
class ProjectionOperator : public Operator {
public:
    ProjectionOperator(const ExecutionContext &exec_ctx,
                       const std::shared_ptr<Operator> &probe_child_operator,
                       std::vector<std::unique_ptr<Projection>> &&projections,
                       bool update_in_place = true);

    ~ProjectionOperator() = default;

    OperatorState Next(Chunk &output_chunk) override;

private:
    void SelfInit() override;

    void SelfCheck() override;

private:
    std::vector<std::unique_ptr<Projection>> projections_;
};

}