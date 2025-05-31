
#pragma once

#include <vector>

#include "sql/operator/logical_operator.h"
#include "sql/parser/parse_defs.h"
#include "storage/field/field.h"
#include "sql/stmt/orderby_stmt.h"
/**
 * @brief 逻辑算子
 * @ingroup LogicalOperator
 */
class OrderByLogicalOperator : public LogicalOperator
{
public:
  OrderByLogicalOperator(vector<unique_ptr<OrderByUnit> > &&orderby_units,
                         vector<unique_ptr<Expression> > &&exprs);

  virtual ~OrderByLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::ORDERBY; }

  vector<unique_ptr<OrderByUnit >> &orderby_units() { return orderby_units_; }

  vector<unique_ptr<Expression>> &exprs() { return exprs_; }
  
private:
  vector<unique_ptr<OrderByUnit >> orderby_units_; //排序列
  ///在 create order by stmt 之前提取 select clause 后的 field_expr (非a gg_expr 中的)和 agg_expr
  vector<unique_ptr<Expression>> exprs_;
};