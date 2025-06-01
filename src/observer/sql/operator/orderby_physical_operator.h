#pragma once

#include <memory>
#include <vector> 

#include "sql/operator/physical_operator.h"
#include "sql/expr/expression.h"
#include "sql/stmt/orderby_stmt.h"
#include "sql/expr/tuple.h"

/**
 * @brief 物理算子
 * @ingroup PhysicalOperator
 */
class OrderByPhysicalOperator : public PhysicalOperator
{
public:
  OrderByPhysicalOperator(vector<unique_ptr<OrderByUnit >> &&orderby_units,
                         vector<unique_ptr<Expression>> &&exprs);

  virtual ~OrderByPhysicalOperator() = default;

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::ORDERBY;
  }

  RC fetch_and_sort_tables();
  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

private:
  vector<unique_ptr<OrderByUnit >> orderby_units_; //排序列
  vector<vector<Value>> values_;
  OrderByTuple tuple_;

  std::unique_ptr<OrderByTuple> header_tuple_;  // 使用OrderByTuple存储表头
  std::unique_ptr<OrderByTuple> current_tuple_; // 当前处理的tuple
  bool header_emitted_ = false;

  vector<int> ordered_idx_;//存储从 values_中取 数据的顺序
  vector<int>::iterator it_;
};