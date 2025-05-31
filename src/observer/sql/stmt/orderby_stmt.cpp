#include "sql/stmt/orderby_stmt.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "storage/table/table.h"

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
                // 检查是否是未绑定的字段表达式
                if (expr->type() == ExprType::UNBOUND_FIELD) {
                    const UnboundFieldExpr* unbound_expr = dynamic_cast<const UnboundFieldExpr*>(expr.get());
                    if (unbound_expr == nullptr) {
                        LOG_WARN("Failed to cast to UnboundFieldExpr");
                        return RC::INTERNAL;
                    }

                    const char* table_name = unbound_expr->table_name();
                    const char* field_name = unbound_expr->field_name();
                    
                    // 确定表
                    Table* table = nullptr;
                    if (table_name != nullptr && strlen(table_name) > 0) {
                        auto iter = tables->find(table_name);
                        if (iter == tables->end()) {
                            LOG_WARN("No such table: %s", table_name);
                            return RC::SCHEMA_TABLE_NOT_EXIST;
                        }
                        table = iter->second;
                    } else {
                        table = default_table;
                    }
                    
                    if (table == nullptr) {
                        LOG_WARN("Failed to find table for field: %s", field_name);
                        return RC::SCHEMA_FIELD_NOT_EXIST;
                    }
                    
                    // 查找字段
                    const FieldMeta* field_meta = table->table_meta().field(field_name);
                    if (field_meta == nullptr) {
                        LOG_WARN("No such field: %s.%s", 
                            table_name ? table_name : "default", 
                            field_name);
                        return RC::SCHEMA_FIELD_NOT_EXIST;
                    }
                    
                    // 创建绑定后的字段表达式
                    Field field(table, field_meta);
                    expr = std::make_unique<FieldExpr>(field);
                }
                
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