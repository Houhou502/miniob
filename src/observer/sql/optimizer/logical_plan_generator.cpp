/* Copyright (c) 2023 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/08/16.
//

#include "sql/optimizer/logical_plan_generator.h"

#include "common/log/log.h"

#include "common/type/attr_type.h"
#include "sql/operator/calc_logical_operator.h"
#include "sql/operator/delete_logical_operator.h"
#include "sql/operator/explain_logical_operator.h"
#include "sql/operator/insert_logical_operator.h"
#include "sql/operator/update_logical_operator.h"
#include "sql/operator/join_logical_operator.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/predicate_logical_operator.h"
#include "sql/operator/project_logical_operator.h"
#include "sql/operator/table_get_logical_operator.h"
#include "sql/operator/group_by_logical_operator.h"
#include "sql/operator/orderby_logical_operator.h"

#include "sql/stmt/calc_stmt.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/explain_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/insert_stmt.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/orderby_stmt.h"
#include "sql/stmt/stmt.h"

#include "sql/expr/expression_iterator.h"

using namespace std;
using namespace common;

RC LogicalPlanGenerator::create(Stmt *stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  RC rc = RC::SUCCESS;
  switch (stmt->type()) {
    case StmtType::CALC: {
      CalcStmt *calc_stmt = static_cast<CalcStmt *>(stmt);

      rc = create_plan(calc_stmt, logical_operator);
    } break;

    case StmtType::SELECT: {
      SelectStmt *select_stmt = static_cast<SelectStmt *>(stmt);

      rc = create_plan(select_stmt, logical_operator);
    } break;

    case StmtType::INSERT: {
      InsertStmt *insert_stmt = static_cast<InsertStmt *>(stmt);

      rc = create_plan(insert_stmt, logical_operator);
    } break;

    case StmtType::DELETE: {
      DeleteStmt *delete_stmt = static_cast<DeleteStmt *>(stmt);

      rc = create_plan(delete_stmt, logical_operator);
    } break;

    case StmtType::UPDATE: {
      UpdateStmt *update_stmt = static_cast<UpdateStmt *>(stmt);

      rc = create_plan(update_stmt, logical_operator);
    } break;

    case StmtType::EXPLAIN: {
      ExplainStmt *explain_stmt = static_cast<ExplainStmt *>(stmt);

      rc = create_plan(explain_stmt, logical_operator);
    } break;
    default: {
      rc = RC::UNIMPLEMENTED;
    }
  }
  return rc;
}

RC LogicalPlanGenerator::create_plan(CalcStmt *calc_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  logical_operator.reset(new CalcLogicalOperator(std::move(calc_stmt->expressions())));
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(SelectStmt *select_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  unique_ptr<LogicalOperator> *last_oper = nullptr;

  unique_ptr<LogicalOperator> table_oper(nullptr);
  last_oper = &table_oper;
  unique_ptr<LogicalOperator> predicate_oper;

  RC rc = create_plan(select_stmt->filter_stmt(), predicate_oper);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to create predicate logical plan. rc=%s", strrc(rc));
    return rc;
  }

  const vector<Table *> &tables = select_stmt->tables();
  for (Table *table : tables) {
    unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, ReadWriteMode::READ_ONLY));
    if (table_oper == nullptr) {
      table_oper = std::move(table_get_oper);
    } else {
      JoinLogicalOperator *join_oper = new JoinLogicalOperator;
      join_oper->add_child(std::move(table_oper));
      join_oper->add_child(std::move(table_get_oper));
      table_oper = unique_ptr<LogicalOperator>(join_oper);
    }
  }

  if (predicate_oper) {
    if (*last_oper) {
      predicate_oper->add_child(std::move(*last_oper));
    }
    last_oper = &predicate_oper;
  }

  unique_ptr<LogicalOperator> group_by_oper;
  rc = create_group_by_plan(select_stmt, group_by_oper);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to create group by logical plan. rc=%s", strrc(rc));
    return rc;
  }

  if (group_by_oper) {
    if (*last_oper) {
      group_by_oper->add_child(std::move(*last_oper));
    }
    last_oper = &group_by_oper;
  }

  // 创建投影算子
    auto project_oper = make_unique<ProjectLogicalOperator>(std::move(select_stmt->query_expressions()));
    if (*last_oper) {
        project_oper->add_child(std::move(*last_oper));
    }

    // 处理ORDER BY
    if (!select_stmt->orderby_stmt().empty()) {
        vector<unique_ptr<OrderByUnit>> orderby_units;
        vector<unique_ptr<Expression>> orderby_exprs;
        
        for (auto &orderby_stmt : select_stmt->orderby_stmt()) {
          //std::cout<<"logical plan orderby "<<endl;
            for (auto &unit : orderby_stmt->get_orderby_units()) {
                orderby_units.emplace_back(make_unique<OrderByUnit>(
                    unit->expr()->copy(),
                    unit->sort_type()
                ));
                //std::cout<<"unit: "<<unit->expr().get()<<" "<<unit->expr()->name()<<" "<<unit->sort_type()<<endl;
            }
        }

        for (auto &orderby_stmt : select_stmt->orderby_stmt()) {
            for (auto &expr_order : orderby_stmt->get_exprs()) {
              //std::cout<<"logical plan orderby expression "<<endl;
              orderby_exprs.emplace_back(expr_order->copy());
              //cout<<"name:"<<expr_order->name()<<endl;
            }
          }

        unique_ptr<LogicalOperator> orderby_oper = make_unique<OrderByLogicalOperator>(
            std::move(orderby_units),
            std::move(orderby_exprs)
        );
        

        // orderby_oper->add_child(std::move(project_oper));
        // logical_operator = std::move(orderby_oper);
        orderby_oper->add_child(std::move(*last_oper));
        *last_oper = std::move(orderby_oper); 
    } 


  logical_operator = std::move(*last_oper);
  return RC::SUCCESS;
}


RC LogicalPlanGenerator::create_plan(FilterStmt *filter_stmt, std::unique_ptr<LogicalOperator> &logical_operator)
{
  RC rc = RC::SUCCESS;
  std::vector<std::unique_ptr<Expression>> cmp_exprs;
  const std::vector<FilterUnit *> &filter_units = filter_stmt->filter_units();

  for (const FilterUnit *filter_unit : filter_units) {
    Expression *left_raw = filter_unit->left();   // 非 owning 指针
    Expression *right_raw = filter_unit->right(); // 非 owning 指针

    if (left_raw == nullptr || right_raw == nullptr) {
      LOG_WARN("null expression in filter unit");
      return RC::INVALID_ARGUMENT;
    }

    std::unique_ptr<Expression> left = left_raw->copy();   // 创建副本
    std::unique_ptr<Expression> right = right_raw->copy(); // 创建副本

    // 尝试做类型统一（插入 CastExpr）
    if (left->value_type() != right->value_type()) {
      int left_to_right_cost = implicit_cast_cost(left->value_type(), right->value_type());
      int right_to_left_cost = implicit_cast_cost(right->value_type(), left->value_type());

      if (left_to_right_cost <= right_to_left_cost && left_to_right_cost != INT32_MAX) {
        ExprType left_type = left->type();
        auto cast_expr = std::make_unique<CastExpr>(std::move(left), right->value_type());

        if (left_type == ExprType::VALUE) {
          Value casted_val;
          rc = cast_expr->try_get_value(casted_val);
          if (rc != RC::SUCCESS) {
            LOG_WARN("Failed to try_get_value for left cast: %s", strrc(rc));
            return rc;
          }
          left = std::make_unique<ValueExpr>(casted_val);
        } else {
          left = std::move(cast_expr);
        }

      } else if (right_to_left_cost != INT32_MAX) {
        ExprType right_type = right->type();
        auto cast_expr = std::make_unique<CastExpr>(std::move(right), left->value_type());

        if (right_type == ExprType::VALUE) {
          Value casted_val;
          rc = cast_expr->try_get_value(casted_val);
          if (rc != RC::SUCCESS) {
            LOG_WARN("Failed to try_get_value for right cast: %s", strrc(rc));
            return rc;
          }
          right = std::make_unique<ValueExpr>(casted_val);
        } else {
          right = std::move(cast_expr);
        }

      } else {
        LOG_WARN("Unsupported implicit cast from %s to %s",
                 attr_type_to_string(left->value_type()),
                 attr_type_to_string(right->value_type()));
        return RC::UNSUPPORTED;
      }
    }

    auto cmp_expr = std::make_unique<ComparisonExpr>(filter_unit->comp(),
                                                     std::move(left),
                                                     std::move(right));
    cmp_exprs.emplace_back(std::move(cmp_expr));
  }

  if (!cmp_exprs.empty()) {
    auto conjunction_expr = std::make_unique<ConjunctionExpr>(ConjunctionExpr::Type::AND, cmp_exprs);
    logical_operator = std::make_unique<PredicateLogicalOperator>(std::move(conjunction_expr));
  } else {
    logical_operator = nullptr; // no filters
  }

  return rc;
}

int LogicalPlanGenerator::implicit_cast_cost(AttrType from, AttrType to)
{
  if (from == to) {
    return 0;  // 无需转换
  }

  // 支持数字类型之间的隐式转换，代价越小越优先选择
  if (from == AttrType::INTS && to == AttrType::FLOATS) {
    return 1;  // int -> float
  } else if (from == AttrType::FLOATS && to == AttrType::INTS) {
    return 2;  // float -> int，稍贵
  }

  return DataType::type_instance(from)->cast_cost(to);
  
}

RC LogicalPlanGenerator::create_plan(InsertStmt *insert_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table        *table = insert_stmt->table();
  vector<Value> values(insert_stmt->values(), insert_stmt->values() + insert_stmt->value_amount());

  InsertLogicalOperator *insert_operator = new InsertLogicalOperator(table, values);
  logical_operator.reset(insert_operator);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(UpdateStmt *update_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table *table = update_stmt->table();
  const char *attribute_name = update_stmt->attribute_name();
  const Value *value = update_stmt->value();
  
  // 1. 创建表扫描算子(READ_WRITE模式)
  unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, ReadWriteMode::READ_WRITE));
  
  // 2. 如果有条件，创建过滤算子
  unique_ptr<LogicalOperator> predicate_oper;
  if (!update_stmt->conditions().empty()) {
    // 创建过滤条件
    FilterStmt *filter_stmt = nullptr;
    RC rc = FilterStmt::create(
        nullptr,  // db 可以为null，因为单表更新
        table,    // 默认表
        nullptr,  // tables map可以为null，单表操作
        update_stmt->conditions().data(),
        static_cast<int>(update_stmt->conditions().size()),
        filter_stmt);
    
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create filter statement. rc=%s", strrc(rc));
      return rc;
    }
    
    // 使用unique_ptr管理FilterStmt生命周期
    unique_ptr<FilterStmt> filter_stmt_guard(filter_stmt);
    
    rc = create_plan(filter_stmt, predicate_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create predicate operator. rc=%s", strrc(rc));
      return rc;
    }
  }
  
  // 3. 创建更新算子
  unique_ptr<LogicalOperator> update_oper(new UpdateLogicalOperator(table, attribute_name, value));
  
  // 4. 构建算子树
  if (predicate_oper) {
    // 如果有过滤条件: 表扫描 -> 过滤 -> 更新
    predicate_oper->add_child(std::move(table_get_oper));
    update_oper->add_child(std::move(predicate_oper));
  } else {
    // 如果没有过滤条件: 表扫描 -> 更新
    update_oper->add_child(std::move(table_get_oper));
  }
  
  logical_operator = std::move(update_oper);
  return RC::SUCCESS;
}


RC LogicalPlanGenerator::create_plan(DeleteStmt *delete_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table                      *table       = delete_stmt->table();
  FilterStmt                 *filter_stmt = delete_stmt->filter_stmt();
  unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, ReadWriteMode::READ_WRITE));

  unique_ptr<LogicalOperator> predicate_oper;

  RC rc = create_plan(filter_stmt, predicate_oper);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  unique_ptr<LogicalOperator> delete_oper(new DeleteLogicalOperator(table));

  if (predicate_oper) {
    predicate_oper->add_child(std::move(table_get_oper));
    delete_oper->add_child(std::move(predicate_oper));
  } else {
    delete_oper->add_child(std::move(table_get_oper));
  }

  logical_operator = std::move(delete_oper);
  return rc;
}

RC LogicalPlanGenerator::create_plan(ExplainStmt *explain_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  unique_ptr<LogicalOperator> child_oper;

  Stmt *child_stmt = explain_stmt->child();

  RC rc = create(child_stmt, child_oper);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create explain's child operator. rc=%s", strrc(rc));
    return rc;
  }

  logical_operator = unique_ptr<LogicalOperator>(new ExplainLogicalOperator);
  logical_operator->add_child(std::move(child_oper));
  return rc;
}

RC LogicalPlanGenerator::create_group_by_plan(SelectStmt *select_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  vector<unique_ptr<Expression>> &group_by_expressions = select_stmt->group_by();
  vector<Expression *> aggregate_expressions;
  vector<unique_ptr<Expression>> &query_expressions = select_stmt->query_expressions();
  function<RC(unique_ptr<Expression>&)> collector = [&](unique_ptr<Expression> &expr) -> RC {
    if (!expr) {
    LOG_WARN("collector: null expression found");
    return RC::SUCCESS;
    }
    RC rc = RC::SUCCESS;
    if (expr->type() == ExprType::AGGREGATION) {
      expr->set_pos(aggregate_expressions.size() + group_by_expressions.size());
      aggregate_expressions.push_back(expr.get());
    }
    rc = ExpressionIterator::iterate_child_expr(*expr, collector);
    return rc;
  };

  function<RC(unique_ptr<Expression>&)> bind_group_by_expr = [&](unique_ptr<Expression> &expr) -> RC {
    if (!expr) {
    LOG_WARN("bind_group_by_expr: null expression found");
    return RC::SUCCESS;
    }

    RC rc = RC::SUCCESS;
    for (size_t i = 0; i < group_by_expressions.size(); i++) {
      auto &group_by = group_by_expressions[i];
      if (expr->type() == ExprType::AGGREGATION) {
        break;
      } else if (expr->equal(*group_by)) {
        expr->set_pos(i);
        continue;
      } else {
        rc = ExpressionIterator::iterate_child_expr(*expr, bind_group_by_expr);
      }
    }
    return rc;
  };

 bool found_unbound_column = false;
  function<RC(unique_ptr<Expression>&)> find_unbound_column = [&](unique_ptr<Expression> &expr) -> RC {
    if (!expr) {
    LOG_WARN("collector: null expression found");
    return RC::SUCCESS;
    }
    RC rc = RC::SUCCESS;
    if (expr->type() == ExprType::AGGREGATION) {
      // do nothing
    } else if (expr->pos() != -1) {
      // do nothing
    } else if (expr->type() == ExprType::FIELD) {
      found_unbound_column = true;
    }else {
      rc = ExpressionIterator::iterate_child_expr(*expr, find_unbound_column);
    }
    return rc;
  };
  

  for (unique_ptr<Expression> &expression : query_expressions) {
    bind_group_by_expr(expression);
  }

  for (unique_ptr<Expression> &expression : query_expressions) {
    find_unbound_column(expression);
  }

  // collect all aggregate expressions
  for (unique_ptr<Expression> &expression : query_expressions) {
    collector(expression);
  }

  if (group_by_expressions.empty() && aggregate_expressions.empty()) {
    // 既没有group by也没有聚合函数，不需要group by
    return RC::SUCCESS;
  }

  if (found_unbound_column) {
    LOG_WARN("column must appear in the GROUP BY clause or must be part of an aggregate function");
    return RC::INVALID_ARGUMENT;
  }

  // 如果只需要聚合，但是没有group by 语句，需要生成一个空的group by 语句

  auto group_by_oper = make_unique<GroupByLogicalOperator>(std::move(group_by_expressions),
                                                           std::move(aggregate_expressions));
  logical_operator = std::move(group_by_oper);
  return RC::SUCCESS;
}
