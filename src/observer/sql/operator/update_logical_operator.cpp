#include "sql/operator/update_logical_operator.h"

UpdateLogicalOperator::UpdateLogicalOperator(Table *table, const char *attribute_name, const Value *value)
    : table_(table), attribute_name_(attribute_name), value_(value)
{}