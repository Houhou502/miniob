/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/stmt/orderby_stmt.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

RC OrderByStmt::create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
      std::vector<OrderBySqlNode> &&orderby_sql_nodes, OrderByStmt *&stmt,
      std::vector<std::unique_ptr<Expression>> &&exprs)
{
  RC rc = RC::SUCCESS;
  stmt = nullptr;

  std::vector<std::unique_ptr<OrderByUnit>> tmp_units;
  
  for(auto &node : orderby_sql_nodes)
  {
    //rc = Expression::create_expression(db, default_table, tables, node.expr.get(), left_expr);
    tmp_units.emplace_back(std::make_unique<OrderByUnit>(std::move(node.expr),node.is_asc));
  }

  stmt = new OrderByStmt();
  stmt->set_orderby_units(std::move(tmp_units));
  stmt->set_exprs(std::move(exprs));

  return rc;
}