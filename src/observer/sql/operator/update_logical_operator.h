#pragma once

#include "sql/operator/logical_operator.h"
#include "sql/parser/parse_defs.h"

/**
 * @brief 更新逻辑算子
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(Table *table, const char *attribute_name, const Value *value);
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }

  OpType get_op_type() const override { return OpType::LOGICALUPDATE; }

  Table *table() const { return table_; }
  const char *attribute_name() const { return attribute_name_; }
  const Value *value() const { return value_; }

private:
  Table *table_ = nullptr;
  const char *attribute_name_ = nullptr;  // 要更新的字段名
  const Value *value_ = nullptr;          // 更新后的值
};