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

class OrderByUnit 
{
public:
  OrderByUnit(Expression *expr , bool is_asc):expr_(expr),is_asc_(is_asc)
  {}

  ~OrderByUnit() = default;

  void set_sort_type(bool sort_type){ is_asc_ = sort_type; }

  bool sort_type() const { return is_asc_; }

  unique_ptr<Expression>& expr(){ return expr_; }

private:
  
  unique_ptr<Expression> expr_;
  //true is asc
  bool is_asc_ = true;
};

class OrderByStmt : Stmt
{
public:
  OrderByStmt() = default;
  virtual ~OrderByStmt() = default;

  StmtType type() const override{ return StmtType::ORDERBY; }

public:

  vector<unique_ptr<OrderByUnit>>& get_orderby_units(){ return orderby_units_; }
  vector<unique_ptr<Expression>>& get_exprs(){ return exprs_; }

  void set_orderby_units(vector<unique_ptr<OrderByUnit >> &&orderby_units){ orderby_units_ = std::move(orderby_units); }
  void set_exprs(vector<unique_ptr<Expression>> &&exprs){ exprs_ = std::move(exprs); }

  

public:
  static RC create(Db *db, Table *default_table, std::unordered_map<string, Table *> *tables,
      const vector<OrderBySqlNode> &orderby_sql_nodes, OrderByStmt *&stmt,
      vector<unique_ptr<Expression>> &&exprs);

private:
  vector<unique_ptr<OrderByUnit >> orderby_units_; //排序列

  ///在 create order by stmt 之前提取 select clause 后的 field_expr (非a gg_expr 中的)和 agg_expr
  vector<unique_ptr<Expression>> exprs_;
};
