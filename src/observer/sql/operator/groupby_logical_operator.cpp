#include "sql/operator/groupby_logical_operator.h"
GroupByLogicalOperator::GroupByLogicalOperator(std::vector<std::unique_ptr<Expression>> &&groupby_fields,
    std::vector<std::unique_ptr<AggregateExpr>> &&agg_exprs,
    std::vector<std::unique_ptr<FieldExpr>> &&field_exprs)
    : groupby_fields_(std::move(groupby_fields)),
      agg_exprs_(std::move(agg_exprs)),
      field_exprs_(std::move(field_exprs))
{
}