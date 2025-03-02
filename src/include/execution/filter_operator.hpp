#pragma once

#include "execution/expression/filter.hpp"
#include "execution/operator.hpp"

namespace babydb {

/**
 * Filter Operator
 * The output schema is the same as the input.
 */
class FilterOperator : public Operator {
public:
    FilterOperator(const ExecutionContext &exec_ctx,
                   const std::shared_ptr<Operator> &probe_child_operator,
                   std::unique_ptr<Filter> &&filter);

    FilterOperator(const ExecutionContext &exec_ctx,
                   const std::shared_ptr<Operator> &probe_child_operator,
                   std::vector<std::unique_ptr<Filter>> &&filters);

    ~FilterOperator() = default;

    OperatorState Next(Chunk &output_chunk) override;

private:
    void SelfInit() override;

    void SelfCheck() override;

private:
    std::vector<std::unique_ptr<Filter>> filters_;
};

}