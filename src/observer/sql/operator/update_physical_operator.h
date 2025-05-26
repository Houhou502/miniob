#pragma once

#include "sql/operator/physical_operator.h"
#include "sql/parser/parse_defs.h"

/**
 * @brief 更新物理算子
 * @ingroup PhysicalOperator
 */
class UpdatePhysicalOperator : public PhysicalOperator
{
public:
  UpdatePhysicalOperator(Table *table, const char *attribute_name, const Value *value);
  
  virtual ~UpdatePhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::UPDATE; }

  OpType get_op_type() const override { return OpType::UPDATE; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override { return nullptr; }

private:
  RC update_record(Record &old_record, Record &new_record);

private:
  Table         *table_ = nullptr;
  const char    *attribute_name_ = nullptr;
  const Value   *value_ = nullptr;
  Trx           *trx_ = nullptr;
  vector<Record> records_;
  vector<Value>  new_values_;
};
