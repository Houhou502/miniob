#pragma once

#include "common/sys/rc.h"
#include "sql/stmt/stmt.h"
#include "sql/parser/parse_defs.h"
#include <vector>

class Table;
class Db;

/**
 * @brief 更新语句
 * @ingroup Statement
 */
class UpdateStmt : public Stmt
{
public:
  UpdateStmt() = default;
  UpdateStmt(Table *table, const char *attribute_name, const Value *value, 
            const std::vector<ConditionSqlNode> &conditions);

  StmtType type() const override { return StmtType::UPDATE; }

public:
  static RC create(Db *db, const UpdateSqlNode &update_sql, Stmt *&stmt);

public:
  Table *table() const { return table_; }
  const char *attribute_name() const { return attribute_name_; }
  const Value *value() const { return value_; }
  const std::vector<ConditionSqlNode> &conditions() const { return conditions_; }

private:
  Table *table_ = nullptr;
  const char *attribute_name_ = nullptr;  ///< 要更新的字段名
  const Value *value_ = nullptr;         ///< 更新后的值
  std::vector<ConditionSqlNode> conditions_; ///< 更新条件
};