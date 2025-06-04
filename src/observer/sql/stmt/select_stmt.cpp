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

/**
 * 将表达式中的表别名转换为真实表名，并记录原始别名信息
 * 
 * 该函数递归遍历表达式树，处理以下情况：
 * 1. 将 UnboundFieldExpr 中的表别名替换为真实表名
 * 2. 记录原始别名到 table_alias 字段
 * 3. 处理特殊表达式类型（如聚合函数、系统函数）中的子表达式
 * 
 * @param expr 待处理的表达式节点
 * @param alias_to_name 表别名到真实表名的映射表
 * @return RC::SUCCESS 成功 | 其他错误码 失败
 */
RC SelectStmt::convert_alias_to_name(Expression *expr, std::shared_ptr<std::unordered_map<string, string>> alias_to_name)
{
  // 基础情况：值表达式、值列表和子查询无需处理
  if (expr->type() == ExprType::VALUE || expr->type() == ExprType::VALUE_LIST || expr->type() == ExprType::SUB_QUERY) {
    return RC::SUCCESS;
  }
  
  // 处理算术表达式（如 a + b）
  if (expr->type() == ExprType::ARITHMETIC) {
    ArithmeticExpr *arith_expr = static_cast<ArithmeticExpr *>(expr);
    
    // 递归处理左操作数
    if (arith_expr->left() != nullptr) {
      RC rc = SelectStmt::convert_alias_to_name(arith_expr->left().get(), alias_to_name);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to process left operand of arithmetic expression");
        return rc;
      }
    }
    
    // 递归处理右操作数
    if (arith_expr->right() != nullptr) {
      RC rc = SelectStmt::convert_alias_to_name(arith_expr->right().get(), alias_to_name);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to process right operand of arithmetic expression");
        return rc;
      }
    }
    return RC::SUCCESS;
  } 

  // // 处理通配符表达式（如 table.*）
  // else if (expr->type() == ExprType::STAR) {
  //   StarExpr *star_expr = static_cast<StarExpr *>(expr);
  //   const char *table_name = star_expr->table_name();
    
  //   // 如果指定了具体表别名（而非通用的*）
  //   if (!is_blank(table_name) && 0 != strcmp(table_name, "*")) {
  //     // 查找别名对应的真实表名
  //     if (alias_to_name->find(table_name) != alias_to_name->end()) {
  //       std::string true_table_name = (*alias_to_name)[table_name];
  //       LOG_DEBUG("convert alias to name: %s -> %s", table_name, true_table_name.c_str());
        
  //       // 更新表名为真实表名，并记录原始别名
  //       star_expr->set_table_name(true_table_name.c_str());
  //       expr->set_table_alias(table_name);
  //     }
  //   }
  //   return RC::SUCCESS;
  // }

  // 处理聚合函数表达式（如 SUM(table.col)）
  else if (expr->type() == ExprType::UNBOUND_AGGREGATION) {
    UnboundAggregateExpr *aggre_expr = static_cast<UnboundAggregateExpr *>(expr);
    
    // 聚合函数必须有子表达式
    if (aggre_expr->child() == nullptr) {
      LOG_WARN("invalid aggregate expression: missing child expression");
      return RC::INVALID_ARGUMENT;
    }
    
    // 递归处理子表达式
    RC rc = SelectStmt::convert_alias_to_name(aggre_expr->child().get(), alias_to_name);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to process child expression of aggregate function");
      return rc;
    }
    return rc;
  } 
  
  // 处理系统函数表达式（如 DATE_FORMAT(table.date, '%Y-%m')）
  else if (expr->type() == ExprType::SYS_FUNCTION) {
    SysFunctionExpr *sys_func_expr = static_cast<SysFunctionExpr *>(expr);
    
    // 系统函数必须有参数
    if (sys_func_expr->params().size() == 0) {
      LOG_WARN("invalid system function expression: no parameters");
      return RC::INVALID_ARGUMENT;
    }
    
    // 递归处理所有参数
    for (auto &param : sys_func_expr->params()) {
      RC rc = SelectStmt::convert_alias_to_name(param.get(), alias_to_name);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to process parameter of system function");
        return rc;
      }
    }
    return RC::SUCCESS;
  }

  // // 处理未绑定的字段表达式（核心逻辑）
  // if (expr->type() != ExprType::UNBOUND_FIELD) {
  //   LOG_WARN("convert_alias_to_name: invalid expr type: %d. It should be UnboundField.", expr->type());
  //   return RC::INVALID_ARGUMENT;
  // }
  
  auto ub_field_expr = static_cast<UnboundFieldExpr *>(expr);
  
  // 如果字段使用了表别名，替换为真实表名
  if (alias_to_name->find(ub_field_expr->table_name()) != alias_to_name->end()) {
    std::string true_table_name = (*alias_to_name)[ub_field_expr->table_name()];
    LOG_DEBUG("convert alias to name: %s -> %s", ub_field_expr->table_name(), true_table_name.c_str());
    
    // 关键逻辑：
    // 1. 记录原始表别名到 table_alias
    // 2. 将 table_name 设置为真实表名
    // 这确保后续处理使用真实表名，但仍保留别名信息
    ub_field_expr->set_table_alias(ub_field_expr->table_name());
    ub_field_expr->set_table_name(true_table_name);
  }
  return RC::SUCCESS;
}

RC SelectStmt::create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt,
    std::shared_ptr<std::unordered_map<string, string>> name_to_alias,
    std::shared_ptr<std::unordered_map<string, string>> alias_to_name,
    std::shared_ptr<std::vector<string>>                loaded_relation_names)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  if (name_to_alias == nullptr)
    name_to_alias = std::make_shared<std::unordered_map<string, string>>();
  if (alias_to_name == nullptr)
    alias_to_name = std::make_shared<std::unordered_map<string, string>>();
  if (loaded_relation_names == nullptr)
    loaded_relation_names = std::make_shared<std::vector<string>>();
  BinderContext binder_context;

  // collect tables in `from` statement
  vector<Table *>                tables;
  unordered_map<string, Table *> table_map;
  std::vector<std::string>       table_aliases;

  // add table in loaded_relation_name into table_map
  for (auto &rel_name : *loaded_relation_names) {
    Table *table = db->find_table(rel_name.c_str());
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), rel_name.c_str());
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }
    table_map.insert({rel_name, table});
    LOG_DEBUG("add table from name_to_alias extraly(sub-query): %s", rel_name.c_str());
  }

  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    const char *table_name = select_sql.relations[i].relation_name.c_str();
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
    // 当别名为空时也会加入“”
    table_aliases.push_back(select_sql.relations[i].alias_name);
    table_map.insert({table_name, table});
    // 检查 alias 重复
    for (size_t j = i + 1; j < select_sql.relations.size(); j++) {
      if (select_sql.relations[i].alias_name.empty() || select_sql.relations[j].alias_name.empty())
        continue;
      if (select_sql.relations[i].alias_name == select_sql.relations[j].alias_name) {
        LOG_WARN("duplicate alias: %s", select_sql.relations[i].alias_name.c_str());
        return RC::INVALID_ARGUMENT;
      }
    }

    if (!select_sql.relations[i].alias_name.empty()) {
      // 如果有别名，使用别名
      (*alias_to_name)[select_sql.relations[i].alias_name] = string(table_name);
      (*name_to_alias)[table_name]                         = select_sql.relations[i].alias_name;
    }
    // 把table加载入记录中
    loaded_relation_names->push_back(table_name);
  }

  // 处理 select 中的表达式(projection)中带有别名的字段替换为真实表名,列名不会替换
  for (auto &expression : select_sql.expressions) {
    RC rc = convert_alias_to_name(expression.get(), alias_to_name);
    if (!expression->alias().empty())
      LOG_DEBUG("convert alias from %s to %s", expression->name(), expression->alias().c_str());
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to convert alias to name");
      return rc;
    }

    // 如果是 StarExpr，检查是否有别名，如果有报错
    if (expression->type() == ExprType::STAR) {
      StarExpr *star_expr = static_cast<StarExpr *>(expression.get());
      if (!star_expr->alias().empty()) {
        LOG_WARN("alias found in star expression");
        return RC::INVALID_ARGUMENT;
      }
    }
  }

  // 将 conditions 中 **所有** 带有别名的表名替换为真实的表名
  for (auto &condition : select_sql.conditions) {
    if (condition.left_expr != nullptr) {
      RC rc = convert_alias_to_name(condition.left_expr.get(), alias_to_name);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to convert alias to name");
        return rc;
      }
    }
    if (condition.right_expr != nullptr) {
      RC rc = convert_alias_to_name(condition.right_expr.get(), alias_to_name);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to convert alias to name");
        return rc;
      }
    }
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
