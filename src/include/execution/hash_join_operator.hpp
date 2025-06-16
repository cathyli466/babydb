#pragma once

#include "execution/operator.hpp"

#include <string>
#include <unordered_map>
#include <array>

namespace babydb {

/**
 * Hash Join Operator
 * We only support equavilant join on one column.
 * The output schema is just the union of the input's schema.
 */
class HashJoinOperator : public Operator {
public:
    HashJoinOperator(const ExecutionContext &exec_ctx,
                     const std::shared_ptr<Operator> &probe_child_operator,
                     const std::shared_ptr<Operator> &build_child_operator,
                     const std::string &probe_column_name,
                     const std::string &build_column_name);

    ~HashJoinOperator() override = default;
    
    OperatorState Next(Chunk &output_chunk) override;

    void SelfInit() override;

    void SelfCheck() override;

private:
    void BuildHashTable();
    void ProcessMatches(
        Chunk &output_chunk, 
        idx_t &output_size,
        const Tuple &probe_tuple,
        const std::pair<
            std::unordered_multimap<data_t, idx_t>::const_iterator,
            std::unordered_multimap<data_t, idx_t>::const_iterator> &match_range
    );

private:
    // Configuration constants
    static constexpr size_t NUM_PARTITIONS = 8;
    static constexpr idx_t SMALL_TABLE_THRESHOLD = 100;
    static constexpr idx_t LARGE_TABLE_THRESHOLD = 10000;
    static constexpr idx_t BASE_BATCH_SIZE = 4;

    // Join state
    std::array<std::unordered_multimap<data_t, idx_t>, NUM_PARTITIONS> partitioned_pointer_tables_;
    std::unordered_multimap<data_t, idx_t> pointer_table_; // Fallback for small tables
    std::vector<data_t> tuples_;
    Chunk buffer_;

    // Column information
    std::string probe_column_name_;
    std::string build_column_name_;

    // Counters and flags
    idx_t tuple_count_;
    idx_t width_;
    idx_t buffer_ptr_;
    bool probe_child_exhausted_;
    bool hash_table_build_;
    
    // Adaptive optimization flags
    bool use_partitioning_;
    bool use_batching_;
};

} // namespace babydb
