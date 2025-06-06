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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/orderby_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/expression_binder.h"

using namespace std;
using namespace common;

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC SelectStmt::create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  BinderContext binder_context;

  // collect tables in `from` statement
  vector<Table *>                tables;
  unordered_map<string, Table *> table_map;
  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    const char *table_name = select_sql.relations[i].c_str();
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. relation name is null. index=%d", i);
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    binder_context.add_table(table);
    tables.push_back(table);
    table_map.insert({table_name, table});
  }

  // collect query fields in `select` statement
  vector<unique_ptr<Expression>> bound_expressions;
  ExpressionBinder expression_binder(binder_context);

  bool has_agg = false;
  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    if (expression->type() == ExprType::UNBOUND_AGGREGATION) {
      has_agg = true;
      break;
    }
  }

  if (has_agg) {
    for (unique_ptr<Expression> &select_expr : select_sql.expressions) {
      if (select_expr->type() == ExprType::UNBOUND_AGGREGATION) {
        continue;
      }

      // 聚合函数的算术运算
      if (select_expr->type() == ExprType::ARITHMETIC) {
        ArithmeticExpr *arith_expr = static_cast<ArithmeticExpr *>(select_expr.get());
        if (arith_expr->left() != nullptr && arith_expr->left()->type() == ExprType::UNBOUND_AGGREGATION &&
            arith_expr->right() != nullptr && arith_expr->right()->type() == ExprType::UNBOUND_AGGREGATION) {
          continue;
        }
      }

      bool found = false;
      for (unique_ptr<Expression> &group_by_expr : select_sql.group_by) {
        if (select_expr->equal(*group_by_expr)) {
          found = true;
          break;
        }
      }
      if (!found) {
        LOG_WARN("non-aggregate expression not in group by");
        return RC::INVALID_ARGUMENT;
      }
    }
  }

  
  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    RC rc = expression_binder.bind_expression(expression, bound_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  vector<unique_ptr<Expression>> group_by_expressions;
  for (unique_ptr<Expression> &expression : select_sql.group_by) {
    RC rc = expression_binder.bind_expression(expression, group_by_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }


  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  RC          rc          = FilterStmt::create(db,
      default_table,
      &table_map,
      select_sql.conditions.data(),
      static_cast<int>(select_sql.conditions.size()),
      filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // create order by statement
  vector<unique_ptr<OrderByStmt>> orderby_stmts;
  if (!select_sql.orderbys.empty()) {

    vector<unique_ptr<Expression>> bound_orderby_exprs;
    for (auto &orderby_sql : select_sql.orderbys) {
      RC rc = expression_binder.bind_expression(orderby_sql.expr, bound_orderby_exprs);
      if (OB_FAIL(rc)) {
        LOG_WARN("bind order by expression failed");
        if (filter_stmt != nullptr) {
          delete filter_stmt;
        }
        return rc;
      }
    }
    
    for (auto &orderby_sql : select_sql.orderbys) {
      // 将 OrderBySqlNode 转换为 OrderByStmt 需要的格式
      vector<OrderBySqlNode> orderby_nodes;
      orderby_nodes.emplace_back(OrderBySqlNode{
          .expr = std::move(orderby_sql.expr),
          .is_asc = orderby_sql.is_asc
      });

      OrderByStmt *orderby_stmt = nullptr;
      rc = OrderByStmt::create(db, default_table, &table_map, std::move(orderby_nodes), orderby_stmt, std::move(bound_expressions));
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to create order by stmt");
        if (filter_stmt != nullptr) {
          delete filter_stmt;
        }
        return rc;
      }
      orderby_stmts.emplace_back(orderby_stmt);
    }
  }

  // everything alright
  SelectStmt *select_stmt = new SelectStmt();

  select_stmt->tables_.swap(tables);
  select_stmt->query_expressions_.swap(bound_expressions);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->orderby_stmt_.swap(orderby_stmts);
  select_stmt->group_by_.swap(group_by_expressions);

  stmt                      = select_stmt;
  return RC::SUCCESS;
}
