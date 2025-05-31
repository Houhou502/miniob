#include "sql/operator/orderby_logical_operator.h"

OrderByLogicalOperator::OrderByLogicalOperator(vector<unique_ptr<OrderByUnit >> &&orderby_units,
    vector<unique_ptr<Expression>> &&exprs)
    : orderby_units_(std::move(orderby_units)),
      exprs_(std::move(exprs))
{}