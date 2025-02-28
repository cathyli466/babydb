#include "execution/hash_join_operator.hpp"

#include "common/config.hpp"

namespace babydb {

HashJoinOperator::HashJoinOperator(const std::shared_ptr<Operator> &probe_child_operator,
                                   const std::shared_ptr<Operator> &build_child_operator,
                                   const std::string &probe_column_name,
                                   const std::string &build_column_name,
                                   const ExecutionContext &execute_context)
    : Operator({probe_child_operator, build_child_operator}, execute_context),
      probe_column_name_(probe_column_name),
      build_column_name_(build_column_name) {}

static Tuple UnionTuple(const Tuple &a, const Tuple &b) {
    Tuple result = a;
    result.insert(result.end(), b.begin(), b.end());
    return result;
}

OperatorState HashJoinOperator::Next(Chunk &output_chunk) {
    output_chunk.clear();
    auto &probe_child_operator = child_operators_[0];
    auto probe_key_attr = output_schema_.GetKeyAttrs({probe_column_name_})[0];
    while (output_chunk.size() < CHUNK_SUGGEST_SIZE) {
        if (buffer_ptr_ == buffer_.size() && !probe_child_exhausted_) {
            if (probe_child_operator->Next(buffer_) == EXHAUSETED) {
                probe_child_exhausted_ = true;
            }
            buffer_ptr_ = 0;
        }
        if (buffer_ptr_ == buffer_.size()) {
            return EXHAUSETED;
        }

        auto &probe_tuple = buffer_[buffer_ptr_].first;
        buffer_ptr_++;
        auto match_range = hash_table_.equal_range(probe_tuple.KeyFromTuple(probe_key_attr));
        for (auto match_ite = match_range.first; match_ite != match_range.second; match_ite++) {
            output_chunk.push_back(std::make_pair(UnionTuple(probe_tuple, match_ite->second), INVALID_ID));
        }
    }
    return HAVE_MORE_OUTPUT;
}

void HashJoinOperator::SelfInit() {
    hash_table_.clear();
    buffer_.clear();
    buffer_ptr_ = 0;
    probe_child_exhausted_ = false;

    BuildHashTable();
}

void HashJoinOperator::BuildHashTable() {
    auto &build_child_operator = child_operators_[1];
    Chunk build_chunk;
    const idx_t build_key_attr = output_schema_.GetKeyAttrs({build_column_name_})[0];
    while (build_child_operator->Next(build_chunk) != EXHAUSETED) {
        for (auto &chunk_row : build_chunk) {
            auto &tuple = chunk_row.first;
            hash_table_.insert(std::make_pair(tuple.KeyFromTuple(build_key_attr), tuple));
        }
    }
}

}