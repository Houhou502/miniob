#include "common/log/log.h"
#include "common/lang/string.h"
#include "sql/stmt/orderby_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/stmt/filter_stmt.h"

RC OrderByStmt::create(Db *db, Table *default_table, std::unordered_map<string, Table *> *tables,
      const vector<OrderBySqlNode> &orderby_sql_nodes, OrderByStmt *&stmt,
      vector<unique_ptr<Expression>> &&exprs)
{
  RC rc = RC::SUCCESS;
  stmt = nullptr;

  vector<unique_ptr<OrderByUnit >> tmp_units;
  
  for(auto &node : orderby_sql_nodes){
    tmp_units.emplace_back(make_unique<OrderByUnit>(node.expr,node.is_asc));//这里 order by unit 中的指针是独享的
  }


  stmt = new OrderByStmt();
  stmt->set_orderby_units(std::move(tmp_units));
  stmt->set_exprs(std::move(exprs));

  return rc;
}