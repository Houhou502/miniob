/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/filter_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "common/sys/rc.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

FilterStmt::~FilterStmt()
{
  for (FilterUnit *unit : filter_units_) {
    delete unit;
  }
  filter_units_.clear();
}

RC FilterStmt::create(Db *db, Table *default_table, unordered_map<string, Table *> *tables,
    const ConditionSqlNode *conditions, int condition_num, FilterStmt *&stmt)
{
  RC rc = RC::SUCCESS;
  stmt = nullptr;

  FilterStmt *tmp_stmt = new FilterStmt();
  for (int i = 0; i < condition_num; i++) {
    const ConditionSqlNode &cond = conditions[i];
    FilterUnit *filter_unit = nullptr;

    // 创建过滤单元
    rc = create_filter_unit(db, default_table, tables, cond, filter_unit);
    if (rc != RC::SUCCESS) {
      delete tmp_stmt;
      LOG_WARN("failed to create filter unit. condition index=%d", i);
      return rc;
    }

    tmp_stmt->filter_units_.push_back(filter_unit);
  }

  stmt = tmp_stmt;
  return rc;
}

RC FilterStmt::create_filter_unit(Db *db, Table *default_table, 
    unordered_map<string, Table *> *tables, const ConditionSqlNode &cond,
    FilterUnit *&filter_unit)
{
  RC rc = RC::SUCCESS;
  filter_unit = nullptr;

  // 创建新的过滤单元
  FilterUnit *tmp_unit = new FilterUnit();
  tmp_unit->set_comp(cond.comp_op);

  // 处理左表达式
  if (cond.left_expr) {
    Expression *left_expr = nullptr;
    rc = Expression::create_expression(db, default_table, tables, cond.left_expr.get(), left_expr);
    if (rc != RC::SUCCESS) {
      delete tmp_unit;
      LOG_WARN("failed to create left expression");
      return rc;
    }
    tmp_unit->set_left(left_expr);
  } else {
    // 如果没有表达式，保持原有逻辑（兼容性处理）
    LOG_ERROR("left expression is null");
    delete tmp_unit;
    return RC::INVALID_ARGUMENT;
  }

  // 处理右表达式
  if (cond.right_expr) {
    Expression *right_expr = nullptr;
    rc = Expression::create_expression(db, default_table, tables, cond.right_expr.get(), right_expr);
    if (rc != RC::SUCCESS) {
      delete tmp_unit;
      LOG_WARN("failed to create right expression");
      return rc;
    }
    tmp_unit->set_right(right_expr);
  } else {
    // 如果没有表达式，保持原有逻辑（兼容性处理）
    LOG_ERROR("right expression is null");
    delete tmp_unit;
    return RC::INVALID_ARGUMENT;
  }

  filter_unit = tmp_unit;
  return RC::SUCCESS;
}

// RC get_table_and_field(Db *db, Table *default_table, unordered_map<string, Table *> *tables,
//     const RelAttrSqlNode &attr, Table *&table, const FieldMeta *&field)
// {
//   if (common::is_blank(attr.relation_name.c_str())) {
//     table = default_table;
//   } else if (nullptr != tables) {
//     auto iter = tables->find(attr.relation_name);
//     if (iter != tables->end()) {
//       table = iter->second;
//     }
//   } else {
//     table = db->find_table(attr.relation_name.c_str());
//   }
//   if (nullptr == table) {
//     LOG_WARN("No such table: attr.relation_name: %s", attr.relation_name.c_str());
//     return RC::SCHEMA_TABLE_NOT_EXIST;
//   }

//   field = table->table_meta().field(attr.attribute_name.c_str());
//   if (nullptr == field) {
//     LOG_WARN("no such field in table: table %s, field %s", table->name(), attr.attribute_name.c_str());
//     table = nullptr;
//     return RC::SCHEMA_FIELD_NOT_EXIST;
//   }

//   return RC::SUCCESS;
// }
