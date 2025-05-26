#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "storage/record/record_manager.h"

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, const char *attribute_name, const Value *value)
    : table_(table), attribute_name_(attribute_name), value_(value)
{}

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  unique_ptr<PhysicalOperator> &child = children_[0];
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  // 1. 收集所有需要更新的记录
  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    records_.emplace_back(row_tuple->record());
  }

  child->close();

  // 2. 为每条记录准备新值
  const TableMeta &table_meta = table_->table_meta();
  const int field_num = table_meta.field_num();
  new_values_.resize(field_num);

  // 3. 执行更新操作（先删除旧记录，再插入新记录）
  for (Record &old_record : records_) {
    // 获取旧记录的所有字段值
    rc = table_->get_record_values(old_record, new_values_.data());
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get record values: %s", strrc(rc));
      return rc;
    }

    // 更新指定字段的值
    int field_index = table_meta.find_field_index_by_name(attribute_name_);
    if (field_index < 0) {
      LOG_WARN("field not found: %s", attribute_name_);
      return RC::SCHEMA_FIELD_MISSING;
    }
    new_values_[field_index] = *value_;

    // 创建新记录
    Record new_record;
    rc = table_->make_record(field_num, new_values_.data(), new_record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to make record: %s", strrc(rc));
      return rc;
    }

    // 先删除旧记录
    rc = trx_->delete_record(table_, old_record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to delete record: %s", strrc(rc));
      return rc;
    }

    // 再插入新记录
    rc = trx_->insert_record(table_, new_record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to insert record: %s", strrc(rc));
      // 尝试恢复旧记录
      trx_->insert_record(table_, old_record);
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  records_.clear();
  new_values_.clear();
  return RC::SUCCESS;
}