#include "sql/stmt/orderby_stmt.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"

RC OrderByStmt::create(Db* db, Table* default_table, 
    std::unordered_map<string, Table*>* tables,
    const vector<OrderBySqlNode>& orderby_sql_nodes, 
    OrderByStmt*& stmt) {
    
    RC rc = RC::SUCCESS;
    stmt = new OrderByStmt();
    
    try {
        for (const auto& orderby_sql_node : orderby_sql_nodes) {
            if (orderby_sql_node.expr == nullptr) {
                rc = RC::INVALID_ARGUMENT;
                LOG_WARN("order by expr is null");
                break;
            }
            
            // 对每个表达式创建 OrderByUnit
            for (auto& expr : *(orderby_sql_node.expr)) {
                // 转移所有权到 OrderByUnit
                auto orderby_unit = std::make_unique<OrderByUnit>(
                    std::move(expr), 
                    orderby_sql_node.is_asc
                );
                stmt->orderby_units_.emplace_back(std::move(orderby_unit));
            }
            
            // 清空原始指针，避免重复释放
            const_cast<OrderBySqlNode&>(orderby_sql_node).expr->clear();
        }
    } catch (...) {
        delete stmt;
        stmt = nullptr;
        rc = RC::NOMEM;
        LOG_WARN("failed to create order by stmt");
    }
    
    if (rc != RC::SUCCESS && stmt != nullptr) {
        delete stmt;
        stmt = nullptr;
    }
    
    return rc;
}