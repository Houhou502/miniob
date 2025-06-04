#pragma once

#include <memory>
#include "sql/operator/physical_operator.h"
#include "sql/expr/expression.h"
#include "sql/stmt/groupby_stmt.h"

/**
 * @brief 物理算子
 * @ingroup PhysicalOperator
 */
class GroupByPhysicalOperator : public PhysicalOperator
{
public:
  GroupByPhysicalOperator(std::vector<std::unique_ptr<Expression>>&& groupby_fields,
                          std::vector<std::unique_ptr<AggregateExpr>> &&agg_exprs,
                          std::vector<std::unique_ptr<FieldExpr>> &&field_exprs);

  virtual ~GroupByPhysicalOperator() = default;

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::GROUPBY;
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

private:
  bool is_first_ = true;
  bool is_new_group_ = true;
  bool is_record_eof_ = false;
  std::vector<std::unique_ptr<Expression>> groupby_fields_;
  std::vector<Value> pre_values_;  // its size equal to groupby_units.size

  GroupTuple tuple_;
};
