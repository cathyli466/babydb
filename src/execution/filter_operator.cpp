#include "execution/filter_operator.hpp"

namespace babydb {

static std::vector<std::unique_ptr<Filter>> TransToVec(std::unique_ptr<Filter> &&filter) {
    std::vector<std::unique_ptr<Filter>> result;
    result.push_back(std::move(filter));
    return result;
}

FilterOperator::FilterOperator(const ExecutionContext &exec_ctx,
                               const std::shared_ptr<Operator> &probe_child_operator,
                               std::unique_ptr<Filter> &&filter)
    : FilterOperator(exec_ctx, std::move(probe_child_operator), TransToVec(std::move(filter))) {}

FilterOperator::FilterOperator(const ExecutionContext &exec_ctx,
                               const std::shared_ptr<Operator> &probe_child_operator,
                               std::vector<std::unique_ptr<Filter>> &&filters)
    : Operator(exec_ctx, {probe_child_operator}, probe_child_operator->GetOutputSchema()),
      filters_(std::move(filters)) {}

OperatorState FilterOperator::Next(Chunk &output_chunk) {
    output_chunk.clear();
    Chunk input_chunk;
    auto result = child_operators_[0]->Next(input_chunk);
    for (auto &data : input_chunk) {
        bool pass_tag = true;
        for (auto &filter : filters_) {
            if (!filter->Check(data.first)) {
                pass_tag = false;
                break;
            }
        }
        if (pass_tag) {
            output_chunk.push_back(std::move(data));
        }
    }
    return result;
}

void FilterOperator::SelfCheck() {
    for (auto &filter : filters_) {
        if (filter == nullptr) {
            throw std::logic_error("FilterOperator: Filter is nullptr");
        }
        filter->Init(output_schema_);
    }
}

void FilterOperator::SelfInit() {
    for (auto &filter : filters_) {
        filter->Init(output_schema_);
    }
}

}