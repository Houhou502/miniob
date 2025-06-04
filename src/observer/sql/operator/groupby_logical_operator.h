#pragma once

#include <vector>

#include "sql/operator/logical_operator.h"
#include "sql/parser/parse_defs.h"
#include "sql/parser/value.h"
#include "storage/field/field.h"
#include "sql/stmt/groupby_stmt.h"
/**
 * @brief 逻辑算子
 * @ingroup LogicalOperator
 */
class GroupByLogicalOperator : public LogicalOperator
{
public:
  GroupByLogicalOperator(std::vector<std::unique_ptr<Expression>>&& groupby_fields,
                         std::vector<std::unique_ptr<AggregateExpr>> &&agg_exprs,
                         std::vector<std::unique_ptr<FieldExpr>> &&field_exprs);
  virtual ~GroupByLogicalOperator() = default;

  LogicalOperatorType type() const override
  {
    return LogicalOperatorType::GROUPBY;
  }
  std::vector<std::unique_ptr<Expression>>& groupby_fields()
  {
    return groupby_fields_;
  }
  std::vector<std::unique_ptr<AggregateExpr>> &agg_exprs()
  {
    return agg_exprs_;
  }
  std::vector<std::unique_ptr<FieldExpr>> &field_exprs()
  {
    return field_exprs_;
  }
private:
  std::vector<std::unique_ptr<Expression>> groupby_fields_;
  std::vector<std::unique_ptr<AggregateExpr>> agg_exprs_;
  std::vector<std::unique_ptr<FieldExpr>> field_exprs_;
};