#include "common/log/log.h"
#include "sql/operator/orderby_physical_operator.h"
#include "storage/record/record.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/field/field.h"
#include <chrono>
#include <algorithm>

OrderByPhysicalOperator::OrderByPhysicalOperator(vector<unique_ptr<OrderByUnit>>&& orderby_units,
    vector<unique_ptr<Expression>> &&exprs)
    : orderby_units_(std::move(orderby_units))
{
    tuple_.init(std::move(exprs));
}

RC OrderByPhysicalOperator::open(Trx *trx)
{
    if (children_.size() != 1) {
        LOG_WARN("OrderByPhysicalOperator must have exactly one child");
        return RC::INTERNAL;
    }

    RC rc = children_[0]->open(trx);
    if (rc != RC::SUCCESS) {
        LOG_WARN("Failed to open child operator: %s", strrc(rc));
        return rc;
    }

    return fetch_and_sort_tables();
}

RC OrderByPhysicalOperator::fetch_and_sort_tables()
{
    RC rc = RC::SUCCESS;
    vector<pair<vector<Value>, size_t>> sort_table; // <sort_keys, original_index>
    values_.clear();
    ordered_idx_.clear();

    // 1. 收集所有数据
    size_t row_count = 0;
    while ((rc = children_[0]->next()) == RC::SUCCESS) {
        Tuple* current_tuple = children_[0]->current_tuple();
        if (current_tuple == nullptr) {
            LOG_WARN("Current tuple is null");
            return RC::INTERNAL;
        }

        // 获取排序键值
        vector<Value> sort_keys;
        bool has_error = false;
        
        for (size_t i = 0; i < orderby_units_.size(); i++) {
            auto &unit = orderby_units_[i];
            if (unit->expr() == nullptr) {
                LOG_WARN("Sort expression at index %d is null", i);
                return RC::INTERNAL;
            }

            Value val;
            rc = unit->expr()->get_value(*current_tuple, val);
            if (rc != RC::SUCCESS) {
                LOG_WARN("Failed to get sort key value at index %d: %s", i, strrc(rc));
                has_error = true;
                break;
            }
            sort_keys.push_back(val);
        }

        if (has_error) {
            continue; // Skip this row if we couldn't get the sort key
        }

        // 存储所有列的数据
        vector<Value> row_values;
        int cell_num = current_tuple->cell_num();
        for (int i = 0; i < cell_num; i++) {
            Value val;
            rc = current_tuple->cell_at(i, val);
            if (rc != RC::SUCCESS) {
                LOG_WARN("Failed to get column value at index %d: %s", i, strrc(rc));
                return rc;
            }
            row_values.push_back(val);
        }
        
        sort_table.emplace_back(std::move(sort_keys), row_count++);
        values_.emplace_back(std::move(row_values));
    }

    if (rc != RC::RECORD_EOF) {
        LOG_WARN("Failed to fetch all records: %s", strrc(rc));
        return rc;
    }

    if (sort_table.empty()) {
        LOG_TRACE("No records to sort");
        return RC::SUCCESS;
    }

    // 2. 排序数据
    auto cmp = [this](const auto &a, const auto &b) {
        for (size_t i = 0; i < orderby_units_.size(); ++i) {
            bool is_asc = orderby_units_[i]->sort_type();
            const Value &va = a.first[i];
            const Value &vb = b.first[i];

            // 处理NULL值
            if (va.is_null() && vb.is_null()) continue;
            if (va.is_null()) return is_asc;
            if (vb.is_null()) return !is_asc;

            // 比较非NULL值
            int cmp_result = va.compare(vb);
            if (cmp_result != 0) {
                return is_asc ? (cmp_result < 0) : (cmp_result > 0);
            }
        }
        return false;
    };

    sort(sort_table.begin(), sort_table.end(), cmp);

    // 3. 保存排序结果
    ordered_idx_.clear();
    ordered_idx_.reserve(sort_table.size());
    for (const auto &entry : sort_table) {
        ordered_idx_.push_back(entry.second);
    }
    it_ = ordered_idx_.begin();

    return RC::SUCCESS;
}

RC OrderByPhysicalOperator::next()
{
    if (it_ == ordered_idx_.end() || static_cast<size_t>(*it_) >= values_.size()) {
        return RC::RECORD_EOF;
    }

    const vector<Value>& row_values = values_[*it_];
    tuple_.set_cells(&row_values);
    ++it_;
    return RC::SUCCESS;
}

RC OrderByPhysicalOperator::close()
{
    if (!children_.empty() && children_[0] != nullptr) {
        return children_[0]->close();
    }
    return RC::SUCCESS;
}

Tuple *OrderByPhysicalOperator::current_tuple()
{
    return &tuple_;
}