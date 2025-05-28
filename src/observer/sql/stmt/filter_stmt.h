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

#pragma once

#include <vector>
#include <unordered_map>
#include <string>

#include "sql/expr/expression.h"
#include "sql/parser/parse_defs.h"
#include "sql/stmt/stmt.h"

class Db;
class Table;
class FieldMeta;

class FilterUnit
{
public:
  FilterUnit() = default;
  ~FilterUnit() {
    // FilterUnit 自身不会删除表达式，释放操作由 FilterStmt 统一处理
  }

  void set_comp(CompOp comp) { comp_ = comp; }
  CompOp comp() const { return comp_; }

  void set_left(Expression *expr) { left_ = expr; }
  void set_right(Expression *expr) { right_ = expr; }

  Expression* left() const { return left_; }
  Expression* right() const { return right_; }

private:
  CompOp comp_ = NO_OP;
  Expression *left_ = nullptr;   // 左表达式
  Expression *right_ = nullptr;  // 右表达式
};

/**
 * @brief FilterStmt 用于存储 WHERE 子句对应的过滤表达式。
 *        每个 FilterUnit 表示一个条件表达式，如 A > B
 */
class FilterStmt
{
public:
  FilterStmt() = default;
  virtual ~FilterStmt();

  const std::vector<FilterUnit *> &filter_units() const { return filter_units_; }

  /**
   * 创建 FilterStmt 对象，对应 WHERE 子句
   * @param db 当前数据库指针
   * @param default_table SQL 解析时默认的表
   * @param tables 多表查询时的表名映射
   * @param conditions 条件数组
   * @param condition_num 条件个数
   * @param stmt 创建出的 FilterStmt 指针
   */
  static RC create(Db *db, Table *default_table,
                   std::unordered_map<std::string, Table *> *tables,
                   const ConditionSqlNode *conditions,
                   int condition_num,
                   FilterStmt *&stmt);

  /**
   * 创建 FilterUnit 对象
   */
  static RC create_filter_unit(Db *db, Table *default_table,
                               std::unordered_map<std::string, Table *> *tables,
                               const ConditionSqlNode &condition,
                               FilterUnit *&filter_unit);

private:
  std::vector<FilterUnit *> filter_units_;  // 用于保存所有过滤条件（AND 连接）
};