#pragma once

#include <vector>
#include <unordered_map>
#include "common/sys/rc.h"
#include "sql/parser/parse_defs.h"
#include "sql/stmt/stmt.h"
#include "sql/expr/expression.h"

class Db;
class Table;
class FieldMeta;

class OrderByUnit {
public:
  OrderByUnit(std::unique_ptr<Expression> expr, bool is_asc = true) 
    : expr_(std::move(expr)), is_asc_(is_asc) {}

  ~OrderByUnit() = default;

  void set_sort_type(bool sort_type) { is_asc_ = sort_type; }
  bool sort_type() const { return is_asc_; }
  const std::unique_ptr<Expression>& expr() const { return expr_; }

private:
  std::unique_ptr<Expression> expr_;
  bool is_asc_ = true;
};

class OrderByStmt : public Stmt {
public:
  OrderByStmt() = default;
  ~OrderByStmt() = default;

  StmtType type() const override { return StmtType::ORDERBY; }

  vector<unique_ptr<OrderByUnit>>& get_orderby_units() { 
    return orderby_units_; 
  }

  static RC create(Db* db, Table* default_table, 
      std::unordered_map<string, Table*>* tables,
      const vector<OrderBySqlNode>& orderby_sql_nodes, 
      OrderByStmt*& stmt);
  

private:
  vector<unique_ptr<OrderByUnit>> orderby_units_; // 唯一管理表达式的容器
};