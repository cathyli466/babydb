#include "execution/hash_join_operator.hpp"
#include "common/config.hpp"
#include <array>
#include <memory>

namespace babydb {

// Configuration constants
static constexpr size_t NUM_PARTITIONS = 8;
static constexpr idx_t SMALL_TABLE_THRESHOLD = 100;
static constexpr idx_t LARGE_TABLE_THRESHOLD = 10000;
static constexpr idx_t BASE_BATCH_SIZE = 4;

HashJoinOperator::HashJoinOperator(const ExecutionContext &exec_ctx,
                                 const std::shared_ptr<Operator> &probe_child_operator,
                                 const std::shared_ptr<Operator> &build_child_operator,
                                 const std::string &probe_column_name,
                                 const std::string &build_column_name)
    : Operator(exec_ctx, {probe_child_operator, build_child_operator}),
      probe_column_name_(probe_column_name),
      build_column_name_(build_column_name),
      use_partitioning_(false),
      use_batching_(true) {}

static Tuple UnionTuple(const Tuple &a, const std::vector<data_t>::iterator &start, idx_t width) {
    Tuple result = a;
    result.insert(result.end(), start, start + width);
    return result;
}

void HashJoinOperator::ProcessMatches(Chunk &output_chunk, idx_t &output_size, 
                                    const Tuple &probe_tuple, 
                                    const std::pair<
                                        std::unordered_multimap<data_t, idx_t>::const_iterator,
                                        std::unordered_multimap<data_t, idx_t>::const_iterator> &match_range) {
    for (auto match_ite = match_range.first; match_ite != match_range.second; ++match_ite) {
        if (output_size == output_chunk.size()) {
            output_chunk.push_back(
                std::make_pair(UnionTuple(probe_tuple, tuples_.begin() + match_ite->second, width_),
                INVALID_ID));
        } else {
            output_chunk[output_size].first = UnionTuple(probe_tuple, tuples_.begin() + match_ite->second, width_);
            output_chunk[output_size].second = INVALID_ID;
        }
        output_size++;
    }
}

void HashJoinOperator::BuildHashTable() {
    auto &build_child_operator = child_operators_[1];
    OperatorState state = HAVE_MORE_OUTPUT;
    Chunk build_chunk;
    const idx_t build_key_attr = build_child_operator->GetOutputSchema().GetKeyAttrs({build_column_name_})[0];
    
    // Build phase - collect all tuples from build side
    while (state != EXHAUSETED) {
        state = build_child_operator->Next(build_chunk);
        for (auto &chunk_row : build_chunk) {
            auto &tuple = chunk_row.first;
            tuples_.insert(tuples_.end(), tuple.begin(), tuple.end());
            tuple_count_++;
        }
    }
    
    // Determine optimal partitioning strategy
    use_partitioning_ = tuple_count_ > LARGE_TABLE_THRESHOLD;
    use_batching_ = tuple_count_ > SMALL_TABLE_THRESHOLD;
    
    if (use_partitioning_) {
        // Reserve space for partitioned tables
        size_t partition_size = (tuple_count_ / NUM_PARTITIONS) * 1.5;
        for (auto &table : partitioned_pointer_tables_) {
            table.reserve(partition_size);
        }
    } else {
        // Reserve space for single table
        pointer_table_.reserve(tuple_count_ * 1.5);
    }
    
    // Build hash table(s)
    for (idx_t i = 0; i < tuple_count_; i++) {
        idx_t pos = i * width_;
        auto &key = tuples_[pos + build_key_attr];
        
        if (use_partitioning_) {
            size_t partition = std::hash<data_t>{}(key) % NUM_PARTITIONS;
            partitioned_pointer_tables_[partition].emplace(key, pos);
        } else {
            pointer_table_.emplace(key, pos);
        }
    }
}

OperatorState HashJoinOperator::Next(Chunk &output_chunk) {
    idx_t output_size = 0;

    if (!hash_table_build_) {
        hash_table_build_ = true;
        BuildHashTable();
    }

    auto &probe_child_operator = child_operators_[0];
    auto probe_key_attr = probe_child_operator->GetOutputSchema().GetKeyAttrs({probe_column_name_})[0];
    
    // Determine optimal batch size dynamically
    const idx_t batch_size = use_batching_ ? 
        std::min(BASE_BATCH_SIZE, exec_ctx_.config_.CHUNK_SUGGEST_SIZE - output_size) : 1;
    
    while (output_size < exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
        // Refill buffer if needed
        if (buffer_ptr_ == buffer_.size() && !probe_child_exhausted_) {
            if (probe_child_operator->Next(buffer_) == EXHAUSETED) {
                probe_child_exhausted_ = true;
            }
            buffer_ptr_ = 0;
        }
        
        if (buffer_ptr_ == buffer_.size()) {
            output_chunk.resize(output_size);
            return EXHAUSETED;
        }

        // Process tuples (with or without batching)
        if (use_batching_) {
            // Process in batches for better cache utilization
            idx_t batch_count = 0;
            while (batch_count < batch_size && buffer_ptr_ < buffer_.size()) {
                auto &probe_tuple = buffer_[buffer_ptr_].first;
                auto key = probe_tuple.KeyFromTuple(probe_key_attr);
                
                if (use_partitioning_) {
                    size_t partition = std::hash<data_t>{}(key) % NUM_PARTITIONS;
                    auto match_range = partitioned_pointer_tables_[partition].equal_range(key);
                    ProcessMatches(output_chunk, output_size, probe_tuple, match_range);
                } else {
                    auto match_range = pointer_table_.equal_range(key);
                    ProcessMatches(output_chunk, output_size, probe_tuple, match_range);
                }
                
                buffer_ptr_++;
                batch_count++;
            }
        } else {
            // Process single tuple (optimized for small tables)
            auto &probe_tuple = buffer_[buffer_ptr_].first;
            auto key = probe_tuple.KeyFromTuple(probe_key_attr);
            auto match_range = pointer_table_.equal_range(key);
            ProcessMatches(output_chunk, output_size, probe_tuple, match_range);
            buffer_ptr_++;
        }
    }
    
    output_chunk.resize(output_size);
    return HAVE_MORE_OUTPUT;
}

void HashJoinOperator::SelfInit() {
    tuple_count_ = 0;
    width_ = child_operators_[1]->GetOutputSchema().size();
    tuples_.clear();
    pointer_table_.clear();
    for (auto &table : partitioned_pointer_tables_) {
        table.clear();
    }
    buffer_.clear();
    buffer_ptr_ = 0;
    probe_child_exhausted_ = false;
    hash_table_build_ = false;
    use_partitioning_ = false;
    use_batching_ = true;
}

void HashJoinOperator::SelfCheck() {
    child_operators_[0]->GetOutputSchema().GetKeyAttrs({probe_column_name_});
    child_operators_[1]->GetOutputSchema().GetKeyAttrs({build_column_name_});
}

} // namespace babydb
