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
// Created by Wangyunlai on 2022/07/05.
//

#include "sql/expr/expression.h"
#include "common/type/attr_type.h"
#include "sql/expr/tuple.h"
#include "sql/expr/arithmetic_operator.hpp"
#include "sql/parser/parse_defs.h"
#include "storage/db/db.h"

using namespace std;

RC Expression::get_table_and_field(Db *db,
                       Table *default_table,
                       std::unordered_map<std::string, Table *> *tables,
                       const RelAttrSqlNode &attr,
                       Table *&table,
                       const FieldMeta *&field)
{
  table = nullptr;
  field = nullptr;

  const std::string &relation_name = attr.relation_name;
  const std::string &attribute_name = attr.attribute_name;

  // 1. 查找表
  if (common::is_blank(relation_name.c_str())) {
    // 没有指定表名，使用默认表
    table = default_table;
  } else if (tables != nullptr) {
    // 有多表查询，使用表名映射表
    auto iter = tables->find(relation_name);
    if (iter != tables->end()) {
      table = iter->second;
    }
  }

  // 如果 tables 中找不到，尝试直接从 DB 查找
  if (table == nullptr) {
    table = db->find_table(relation_name.c_str());
  }

  if (table == nullptr) {
    LOG_WARN("No such table: %s", relation_name.c_str());
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // 2. 查找字段
  field = table->table_meta().field(attribute_name.c_str());
  if (field == nullptr) {
    LOG_WARN("No such field in table %s: %s", table->name(), attribute_name.c_str());
    table = nullptr;
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  return RC::SUCCESS;
}

RC Expression::create_expression(Db *db,
                                 Table *default_table,
                                 std::unordered_map<std::string, Table *> *tables,
                                 const Expression *unbound_expr,
                                 Expression *&expr)
{
  expr = nullptr;

  if (unbound_expr == nullptr) {
    LOG_ERROR("Expression is null");
    return RC::INVALID_ARGUMENT;
  }

  switch (unbound_expr->type()) {

    case ExprType::UNBOUND_FIELD: {
      const UnboundFieldExpr *uf = static_cast<const UnboundFieldExpr *>(unbound_expr);

      Table *table = nullptr;
      const FieldMeta *field = nullptr;

      RelAttrSqlNode attr;
      attr.relation_name = uf->table_name();
      attr.attribute_name = uf->field_name();

      RC rc = get_table_and_field(db, default_table, tables, attr, table, field);
      if (rc != RC::SUCCESS) {
        LOG_WARN("Cannot find field: %s.%s", attr.relation_name.c_str(), attr.attribute_name.c_str());
        return rc;
      }

      expr = new FieldExpr(table, field);
      return RC::SUCCESS;
    }

    case ExprType::VALUE: {
      const ValueExpr *val_expr = static_cast<const ValueExpr *>(unbound_expr);
      expr = new ValueExpr(val_expr->get_value());
      return RC::SUCCESS;
    }

    case ExprType::UNBOUND_AGGREGATION: {
      const UnboundAggregateExpr *ua_expr = static_cast<const UnboundAggregateExpr *>(unbound_expr);

      Expression *child_expr = nullptr;
      RC rc = Expression::create_expression(db, default_table, tables, ua_expr->child().get(), child_expr);
      if (rc != RC::SUCCESS) {
        LOG_WARN("Failed to resolve unbound aggregation child");
        return rc;
      }

      AggregateExpr::Type agg_type;
      rc = AggregateExpr::type_from_string(ua_expr->aggregate_name(), agg_type);
      if (rc != RC::SUCCESS) {
        LOG_WARN("Invalid aggregation type: %s", ua_expr->aggregate_name());
        delete child_expr;
        return rc;
      }

      expr = new AggregateExpr(agg_type, child_expr);
      return RC::SUCCESS;
    }

    case ExprType::CAST: {
      const CastExpr *cast_expr = static_cast<const CastExpr *>(unbound_expr);

      Expression *child_expr = nullptr;
      RC rc = Expression::create_expression(db, default_table, tables, cast_expr->child().get(), child_expr);
      if (rc != RC::SUCCESS) {
        return rc;
      }

      expr = new CastExpr(std::unique_ptr<Expression>(child_expr), cast_expr->value_type());
      return RC::SUCCESS;
    }

    case ExprType::COMPARISON: {
      const ComparisonExpr *cmp_expr = static_cast<const ComparisonExpr *>(unbound_expr);

      Expression *left_expr = nullptr;
      RC rc = Expression::create_expression(db, default_table, tables, cmp_expr->left().get(), left_expr);
      if (rc != RC::SUCCESS) {
        return rc;
      }

      Expression *right_expr = nullptr;
      rc = Expression::create_expression(db, default_table, tables, cmp_expr->right().get(), right_expr);
      if (rc != RC::SUCCESS) {
        delete left_expr;
        return rc;
      }

      expr = new ComparisonExpr(cmp_expr->comp(),
                                std::unique_ptr<Expression>(left_expr),
                                std::unique_ptr<Expression>(right_expr));
      return RC::SUCCESS;
    }

    // case ExprType::CONJUNCTION: {
    //   const ConjunctionExpr *conj_expr = static_cast<const ConjunctionExpr *>(unbound_expr);

    //   std::vector<std::unique_ptr<Expression>> children;
    //   for (const auto &child : conj_expr->children()) {
    //     Expression *sub_expr = nullptr;
    //     RC rc = Expression::create_expression(db, default_table, tables, child.get(), sub_expr);
    //     if (rc != RC::SUCCESS) {
    //       return rc;
    //     }
    //     children.emplace_back(sub_expr);
    //   }

    //   expr = new ConjunctionExpr(conj_expr->conjunction_type(), children);
    //   return RC::SUCCESS;
    // }

    case ExprType::ARITHMETIC: {
      const ArithmeticExpr *arith_expr = static_cast<const ArithmeticExpr *>(unbound_expr);

      Expression *left_expr = nullptr;
      RC rc = Expression::create_expression(db, default_table, tables, arith_expr->left().get(), left_expr);
      if (rc != RC::SUCCESS) {
        return rc;
      }

      Expression *right_expr = nullptr;
      if (arith_expr->right()) {
        rc = Expression::create_expression(db, default_table, tables, arith_expr->right().get(), right_expr);
        if (rc != RC::SUCCESS) {
          delete left_expr;
          return rc;
        }
      } else {
      // 一元运算处理：
      // - 仅支持一元负号（ARITH_UNARY_MINUS），其他一元运算符需扩展
        if (arith_expr->arithmetic_type() ==  ArithmeticExpr::Type::NEGATIVE) {
        // 创建右操作数为 0 的 ValueExpr
        Value zero_value;
        zero_value.set_int(0); // 根据实际类型设置（如整数、浮点数）
        right_expr = new ValueExpr(zero_value);
        } else {
          // 不支持的一元运算符，返回错误
          delete left_expr;
          return RC::SQL_SYNTAX;
        }
      }
      

      expr = new ArithmeticExpr(arith_expr->arithmetic_type(),
                                std::unique_ptr<Expression>(left_expr),
                                std::unique_ptr<Expression>(right_expr));
      return RC::SUCCESS;
    }

    default: {
      LOG_WARN("Unsupported expression type: %d", static_cast<int>(unbound_expr->type()));
      return RC::UNIMPLEMENTED;
    }
  }
}

// RC FieldExpr::get_value(const Tuple &tuple, Value &value) const
// {
//   return tuple.find_cell(TupleCellSpec(table_name(), field_name()), value);
// }

RC FieldExpr::get_value(const Tuple &tuple, Value &value) const
{
  // change the table alias
  auto spec = TupleCellSpec(table_name(), field_name());
  if (!table_alias().empty())
    spec.set_table_alias(table_alias());
  return tuple.find_cell(spec, value);
}

bool FieldExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != ExprType::FIELD) {
    return false;
  }
  const auto &other_field_expr = static_cast<const FieldExpr &>(other);
  return table_name() == other_field_expr.table_name() && field_name() == other_field_expr.field_name();
}

// TODO: 在进行表达式计算时，`chunk` 包含了所有列，因此可以通过 `field_id` 获取到对应列。
// 后续可以优化成在 `FieldExpr` 中存储 `chunk` 中某列的位置信息。
RC FieldExpr::get_column(Chunk &chunk, Column &column)
{
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  } else {
    column.reference(chunk.column(field().meta()->field_id()));
  }
  return RC::SUCCESS;
}

bool ValueExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != ExprType::VALUE) {
    return false;
  }
  const auto &other_value_expr = static_cast<const ValueExpr &>(other);
  return value_.compare(other_value_expr.get_value()) == 0;
}

RC ValueExpr::get_value(const Tuple &tuple, Value &value) const
{
  value = value_;
  return RC::SUCCESS;
}

RC ValueExpr::get_column(Chunk &chunk, Column &column)
{
  column.init(value_);
  return RC::SUCCESS;
}

/////////////////////////////////////////////////////////////////////////////////
CastExpr::CastExpr(unique_ptr<Expression> child, AttrType cast_type) : child_(std::move(child)), cast_type_(cast_type)
{}

CastExpr::~CastExpr() {}

RC CastExpr::cast(const Value &value, Value &cast_value) const
{
  RC rc = RC::SUCCESS;
  if (this->value_type() == value.attr_type()) {
    cast_value = value;
    return rc;
  }
  rc = Value::cast_to(value, cast_type_, cast_value);
  return rc;
}

RC CastExpr::get_value(const Tuple &tuple, Value &result) const
{
  Value value;
  RC rc = child_->get_value(tuple, value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, result);
}

RC CastExpr::try_get_value(Value &result) const
{
  Value value;
  RC rc = child_->try_get_value(value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, result);
}

////////////////////////////////////////////////////////////////////////////////

ComparisonExpr::ComparisonExpr(CompOp comp, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : comp_(comp), left_(std::move(left)), right_(std::move(right))
{
}

ComparisonExpr::~ComparisonExpr() {}

RC ComparisonExpr::compare_value(const Value &left, const Value &right, bool &result) const
{
  RC  rc         = RC::SUCCESS;
  int cmp_result = left.compare(right);
  result         = false;

   if (comp_ == LIKE_OP || comp_ == NOT_LIKE_OP) {
    // 确保两个值都是字符串类型
    if (left.attr_type() != AttrType::CHARS || right.attr_type() != AttrType::CHARS) {
      LOG_WARN("LIKE operator can only be used with string values");
      return RC::INTERNAL;
    }
    
    // 调用 LIKE 匹配函数
    std::string str_str = left.get_string();
    const char *str = str_str.c_str();

    std::string pattern_str = right.get_string();
    const char *pattern = pattern_str.c_str();

    bool like_result = like_match(str, pattern);

    result = (comp_ == LIKE_OP) ? like_result : !like_result;

    return RC::SUCCESS;
  }

  switch (comp_) {
    case EQUAL_TO: {
      result = (0 == cmp_result);
    } break;
    case LESS_EQUAL: {
      result = (cmp_result <= 0);
    } break;
    case NOT_EQUAL: {
      result = (cmp_result != 0);
    } break;
    case LESS_THAN: {
      result = (cmp_result < 0);
    } break;
    case GREAT_EQUAL: {
      result = (cmp_result >= 0);
    } break;
    case GREAT_THAN: {
      result = (cmp_result > 0);
    } break;
    default: {
      LOG_WARN("unsupported comparison. %d", comp_);
      rc = RC::INTERNAL;
    } break;
  }

  return rc;
}

bool ComparisonExpr::like_match(const char *str, const char *pattern) const
{
    // 基本情况：如果模式为空，只有当字符串也为空时匹配成功
    if (*pattern == '\0') {
        return *str == '\0';
    }
    // 处理下一个字符是 % 的情况
    if (*pattern == '%') {
        // 跳过连续的 %，因为多个连续的 % 等效于一个 %
        while (*(pattern + 1) == '%') {
            pattern++;
        }
        // % 可以匹配零个或多个字符，因此尝试所有可能的匹配长度
        while (*str != '\0') {
            if (like_match(str, pattern + 1)) {
                return true;
            }
            str++;
        }
        // 尝试 % 匹配零个字符的情况
        return like_match(str, pattern + 1);
    }
    // 处理下一个字符是 _ 的情况
    if (*pattern == '_') {
        // _ 必须匹配一个字符，因此字符串不能为空
        if (*str == '\0') {
            return false;
        }
        // 继续匹配剩余的字符串和模式
        return like_match(str + 1, pattern + 1);
    }
    // 处理普通字符：必须与当前字符匹配，并且剩余部分也匹配
    if (*str != '\0' && *str == *pattern) {
        return like_match(str + 1, pattern + 1);
    }
    // 其他情况：匹配失败
    return false;
}


RC ComparisonExpr::try_get_value(Value &cell) const
{
  if (left_->type() == ExprType::VALUE && right_->type() == ExprType::VALUE) {
    ValueExpr *  left_value_expr  = static_cast<ValueExpr *>(left_.get());
    ValueExpr *  right_value_expr = static_cast<ValueExpr *>(right_.get());
    const Value &left_cell        = left_value_expr->get_value();
    const Value &right_cell       = right_value_expr->get_value();

    bool value = false;
    RC   rc    = compare_value(left_cell, right_cell, value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to compare tuple cells. rc=%s", strrc(rc));
    } else {
      cell.set_boolean(value);
    }
    return rc;
  }

  return RC::INVALID_ARGUMENT;
}

RC ComparisonExpr::get_value(const Tuple &tuple, Value &value) const
{
  Value left_value;
  Value right_value;

  RC rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }

  if (left_value.is_null() || right_value.is_null()) {
    value.set_null(); 
    return RC::SUCCESS;
  }

  bool bool_value = false;

  rc = compare_value(left_value, right_value, bool_value);
  if (rc == RC::SUCCESS) {
    value.set_boolean(bool_value);
  }

  return rc;
}

RC ComparisonExpr::eval(Chunk &chunk, vector<uint8_t> &select)
{
  RC     rc = RC::SUCCESS;
  Column left_column;
  Column right_column;

  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  if (left_column.attr_type() != right_column.attr_type()) {
    LOG_WARN("cannot compare columns with different types");
    return RC::INTERNAL;
  }
  if (left_column.attr_type() == AttrType::INTS) {
    rc = compare_column<int>(left_column, right_column, select);
  } else if (left_column.attr_type() == AttrType::FLOATS) {
    rc = compare_column<float>(left_column, right_column, select);
  } else {
    // TODO: support string compare
    LOG_WARN("unsupported data type %d", left_column.attr_type());
    return RC::INTERNAL;
  }
  return rc;
}

template <typename T>
RC ComparisonExpr::compare_column(const Column &left, const Column &right, vector<uint8_t> &result) const
{
  RC rc = RC::SUCCESS;

  bool left_const  = left.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right.column_type() == Column::Type::CONSTANT_COLUMN;
  if (left_const && right_const) {
    compare_result<T, true, true>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  } else if (left_const && !right_const) {
    compare_result<T, true, false>((T *)left.data(), (T *)right.data(), right.count(), result, comp_);
  } else if (!left_const && right_const) {
    compare_result<T, false, true>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  } else {
    compare_result<T, false, false>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  }
  return rc;
}

////////////////////////////////////////////////////////////////////////////////
ConjunctionExpr::ConjunctionExpr(Type type, vector<unique_ptr<Expression>> &children)
    : conjunction_type_(type), children_(std::move(children))
{}

RC ConjunctionExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    value.set_boolean(true);
    return rc;
  }

  Value tmp_value;
  for (const unique_ptr<Expression> &expr : children_) {
    rc = expr->get_value(tuple, tmp_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value by child expression. rc=%s", strrc(rc));
      return rc;
    }
    bool bool_value = tmp_value.get_boolean();
    if ((conjunction_type_ == Type::AND && !bool_value) || (conjunction_type_ == Type::OR && bool_value)) {
      value.set_boolean(bool_value);
      return rc;
    }
  }

  bool default_value = (conjunction_type_ == Type::AND);
  value.set_boolean(default_value);
  return rc;
}

////////////////////////////////////////////////////////////////////////////////

ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, Expression *left, Expression *right)
    : arithmetic_type_(type), left_(left), right_(right)
{}
ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : arithmetic_type_(type), left_(std::move(left)), right_(std::move(right))
{}

bool ArithmeticExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (type() != other.type()) {
    return false;
  }
  auto &other_arith_expr = static_cast<const ArithmeticExpr &>(other);
  return arithmetic_type_ == other_arith_expr.arithmetic_type() && left_->equal(*other_arith_expr.left_) &&
         right_->equal(*other_arith_expr.right_);
}
AttrType ArithmeticExpr::value_type() const
{
  if (!right_) {
    return left_->value_type();
  }

  if (left_->value_type() == AttrType::INTS && right_->value_type() == AttrType::INTS &&
      arithmetic_type_ != Type::DIV) {
    return AttrType::INTS;
  }

  return AttrType::FLOATS;
}

RC ArithmeticExpr::calc_value(const Value &left_value, const Value &right_value, Value &value) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();
  value.set_type(target_type);

  switch (arithmetic_type_) {
    case Type::ADD: {
      Value::add(left_value, right_value, value);
    } break;

    case Type::SUB: {
      Value::subtract(left_value, right_value, value);
    } break;

    case Type::MUL: {
      Value::multiply(left_value, right_value, value);
    } break;

    case Type::DIV: {
      Value::divide(left_value, right_value, value);
    } break;

    case Type::NEGATIVE: {
      Value::negative(left_value, value);
    } break;

    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported arithmetic type. %d", arithmetic_type_);
    } break;
  }
  return rc;
}

template <bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
RC ArithmeticExpr::execute_calc(
    const Column &left, const Column &right, Column &result, Type type, AttrType attr_type) const
{
  RC rc = RC::SUCCESS;
  switch (type) {
    case Type::ADD: {
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, AddOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, AddOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
    } break;
    case Type::SUB:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, SubtractOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, SubtractOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::MUL:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, MultiplyOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, MultiplyOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::DIV:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, DivideOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, DivideOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::NEGATIVE:
      if (attr_type == AttrType::INTS) {
        unary_operator<LEFT_CONSTANT, int, NegateOperator>((int *)left.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        unary_operator<LEFT_CONSTANT, float, NegateOperator>(
            (float *)left.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    default: rc = RC::UNIMPLEMENTED; break;
  }
  if (rc == RC::SUCCESS) {
    result.set_count(result.capacity());
  }
  return rc;
}
RC ArithmeticExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;

  if (arithmetic_type_ == Type::NEGATIVE) {
    if (!left_) {
      LOG_ERROR("Unary NEGATIVE expression missing left operand");
      return RC::INTERNAL;
    }
    Value operand;
    rc = left_->get_value(tuple, operand);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of NEGATIVE's operand. rc=%s", strrc(rc));
      return rc;
    }
    return calc_value(operand, Value(), value); // pass dummy right
  }

  // Binary expression: ADD, SUB, *, /
  if (!left_ || !right_) {
    LOG_ERROR("Binary ArithmeticExpr missing operand");
    return RC::INTERNAL;
  }

  Value left_value;
  Value right_value;

  rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }

  rc = right_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }

  return calc_value(left_value, right_value, value);
}

RC ArithmeticExpr::get_column(Chunk &chunk, Column &column)
{
  RC rc = RC::SUCCESS;
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
    return rc;
  }
  Column left_column;
  Column right_column;

  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of right expression. rc=%s", strrc(rc));
    return rc;
  }
  return calc_column(left_column, right_column, column);
}

RC ArithmeticExpr::calc_column(const Column &left_column, const Column &right_column, Column &column) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();
  column.init(target_type, left_column.attr_len(), max(left_column.count(), right_column.count()));
  bool left_const  = left_column.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right_column.column_type() == Column::Type::CONSTANT_COLUMN;
  if (left_const && right_const) {
    column.set_column_type(Column::Type::CONSTANT_COLUMN);
    rc = execute_calc<true, true>(left_column, right_column, column, arithmetic_type_, target_type);
  } else if (left_const && !right_const) {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<true, false>(left_column, right_column, column, arithmetic_type_, target_type);
  } else if (!left_const && right_const) {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<false, true>(left_column, right_column, column, arithmetic_type_, target_type);
  } else {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<false, false>(left_column, right_column, column, arithmetic_type_, target_type);
  }
  return rc;
}

RC ArithmeticExpr::try_get_value(Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->try_get_value(left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }

  if (right_) {
    rc = right_->try_get_value(right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }

  return calc_value(left_value, right_value, value);
}

////////////////////////////////////////////////////////////////////////////////

UnboundAggregateExpr::UnboundAggregateExpr(const char *aggregate_name, Expression *child)
    : aggregate_name_(aggregate_name), child_(child)
{}

UnboundAggregateExpr::UnboundAggregateExpr(const char *aggregate_name, unique_ptr<Expression> child)
    : aggregate_name_(aggregate_name), child_(std::move(child))
{}

////////////////////////////////////////////////////////////////////////////////
AggregateExpr::AggregateExpr(Type type, Expression *child) : aggregate_type_(type), child_(child) {}

AggregateExpr::AggregateExpr(Type type, unique_ptr<Expression> child) : aggregate_type_(type), child_(std::move(child))
{}

RC AggregateExpr::get_column(Chunk &chunk, Column &column)
{
  RC rc = RC::SUCCESS;
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  } else {
    rc = RC::INTERNAL;
  }
  return rc;
}

bool AggregateExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != type()) {
    return false;
  }
  const AggregateExpr &other_aggr_expr = static_cast<const AggregateExpr &>(other);
  return aggregate_type_ == other_aggr_expr.aggregate_type() && child_->equal(*other_aggr_expr.child());
}

unique_ptr<Aggregator> AggregateExpr::create_aggregator() const
{
  unique_ptr<Aggregator> aggregator;
  switch (aggregate_type_) {
    case Type::SUM: {
      aggregator = make_unique<SumAggregator>();
      break;
    }
    case Type::MAX: {
      aggregator = make_unique<MaxAggregator>();
      break;
    }
    case Type::MIN: {
      aggregator = make_unique<MinAggregator>();
      break;
    }
     case Type::AVG: {
      aggregator = make_unique<AvgAggregator>();
      break;
    }
    case Type::COUNT: {
      aggregator = make_unique<CountAggregator>();
      break;
    }
    default: {
      ASSERT(false, "unsupported aggregate type");
      break;
    }
  }
  return aggregator;
}

RC AggregateExpr::get_value(const Tuple &tuple, Value &value) const
{
  return tuple.find_cell(TupleCellSpec(name()), value);
}

RC AggregateExpr::type_from_string(const char *type_str, AggregateExpr::Type &type)
{
  RC rc = RC::SUCCESS;
  if (0 == strcasecmp(type_str, "count")) {
    type = Type::COUNT;
  } else if (0 == strcasecmp(type_str, "sum")) {
    type = Type::SUM;
  } else if (0 == strcasecmp(type_str, "avg")) {
    type = Type::AVG;
  } else if (0 == strcasecmp(type_str, "max")) {
    type = Type::MAX;
  } else if (0 == strcasecmp(type_str, "min")) {
    type = Type::MIN;
  } else {
    rc = RC::INVALID_ARGUMENT;
  }
  return rc;
}

////////////////////////////////////////////////////////////////////////////////

AttrType SysFunctionExpr::value_type() const
{
  switch (sys_func_type_) {
    case SysFuncType::DATE_FORMAT: return AttrType::DATES;
    case SysFuncType::LENGTH: return AttrType::CHARS;
    case SysFuncType::ROUND: return AttrType::FLOATS;
    default: return AttrType::UNDEFINED;
  }
  return AttrType::UNDEFINED;
}

RC SysFunctionExpr::check_params() const
{
  switch (sys_func_type_) {
    case SysFuncType::LENGTH: {
      if (params_.size() != 1 && params_[0]->value_type() != this->value_type()) {
        LOG_WARN("LENGTH function must have one parameter, which is chars type");
        return RC::INVALID_ARGUMENT;
      }
      break;
    }

    case SysFuncType::DATE_FORMAT: {
      if (params_.size() != 2 || params_[0]->value_type() != AttrType::DATES || params_[1]->value_type() != AttrType::CHARS) {
        LOG_WARN("DATE_FORMAT function must have two parameters, the first is date and the second is chars");
        return RC::INVALID_ARGUMENT;
      }
      break;
    }

    case SysFuncType::ROUND: {
      if ((params_.size() == 1 || params_.size() != 2) && params_[0]->value_type() != AttrType::FLOATS) {
        LOG_WARN("ROUND function must have one or two parameters, the first is float and the second is int");
        return RC::INVALID_ARGUMENT;
      }
      if (params_.size() == 2 && params_[1]->value_type() != AttrType::INTS) {
        LOG_WARN("ROUND function's second parameter must be int");
        return RC::INVALID_ARGUMENT;
      }
      break;
    }

    default: {
      LOG_WARN("unsupported sys_function type. %d", sys_func_type_);
      return RC::UNIMPLEMENTED;
    }
  }
  return RC::SUCCESS;
}

RC SysFunctionExpr::get_func_length_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;
  Value param;
  rc = params_[0]->get_value(tuple, param);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of first parameter. rc=%s", strrc(rc));
    return rc;
  }
  if (param.attr_type() != AttrType::CHARS) {
    LOG_WARN("LENGTH function's parameter must be CHAR");
    return RC::INVALID_ARGUMENT;
  }
  int len = strlen(param.get_string().c_str());
  value.set_int(len);
  return rc;
}

RC SysFunctionExpr::get_func_round_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;
  if (params_.size() > 1) {
    Value param1;
    Value param2;
    rc = params_[0]->get_value(tuple, param1);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of first parameter. rc=%s", strrc(rc));
      return rc;
    }
    rc = params_[1]->get_value(tuple, param2);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of second parameter. rc=%s", strrc(rc));
      return rc;
    }
    float value1 = param1.get_float();
    int value2 = param2.get_int();
    stringstream ss;
    ss << fixed << setprecision(value2) << value1;
    ss >> value1;
    value.set_float(value1);
  } else {
    Value param1;
    rc = params_[0]->get_value(tuple, param1);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of first parameter. rc=%s", strrc(rc));
      return rc;
    }
    float value1 = param1.get_float();
    stringstream ss;
    ss << fixed << setprecision(0) << value1;
    ss >> value1;
    value.set_float(value1);
  }
  return rc;
}

RC SysFunctionExpr::get_func_date_format_value(const Tuple &tuple, Value &value) const
{
    RC rc = RC::SUCCESS;

    // 定义两个参数变量，用于存储从元组中提取的参数值
    Value param1;
    Value param2;

    // 提取第一个参数（日期数值，格式为YYYYMMDD的整数）
    rc = params_[0]->get_value(tuple, param1);
    if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get value of first parameter. rc=%s", strrc(rc));
        return rc;
    }
    // 提取第二个参数（日期格式字符串，包含%占位符）
    rc = params_[1]->get_value(tuple, param2);
    if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get value of second parameter. rc=%s", strrc(rc));
        return rc;
    }

    // 解析日期数值：假设输入为YYYYMMDD格式的整数
    int         date_val   = param1.get_int(); // 提取整数形式的日期值
    const char *format_str = param2.data();     // 提取格式字符串

    // 拆分日期各部分：通过数值运算分解年/月/日
    int year  = date_val / 10000;        
    int month = (date_val % 10000) / 100; 
    int day   = date_val % 100;         

    std::string result; // 存储格式化后的日期字符串

    // 遍历格式字符串，处理每个字符和占位符
    for (size_t i = 0; format_str[i] != '\0'; ++i) {
        // 检测占位符起始符'%'
        if (format_str[i] == '%' && format_str[i + 1] != '\0') {
            ++i; // 跳过%符号，处理具体格式字符
            switch (format_str[i]) {
                case 'y': {
                    // 两位短年份（如23，取自年份后两位）
                    result += std::to_string(year % 100 / 10) + std::to_string(year % 10);
                    break;
                }
                case 'Y': {
                    // 四位完整年份（如2023）
                    result += std::to_string(year);
                    break;
                }
                case 'm': {
                    // 两位数字月份（补前导零，如01-12）
                    if (month < 10) {
                        result += "0"; // 小于10时补零
                    }
                    result += std::to_string(month);
                    break;
                }
                case 'M': {
                    // 英文月份全称（如January-December）
                    static const char *month_names[] = {"January", "February", "March", "April",
                                                        "May", "June", "July", "August",
                                                        "September", "October", "November", "December"};
                    // 检查月份有效性（1-12）
                    if (month >= 1 && month <= 12) {
                        result += month_names[month - 1]; // 数组下标从0开始
                    } else {
                        return RC::INVALID_ARGUMENT; // 无效月份返回错误
                    }
                    break;
                }
                case 'd': {
                    // 两位数字日期（补前导零，如01-31）
                    if (day < 10) {
                        result += "0"; // 小于10时补零
                    }
                    result += std::to_string(day);
                    break;
                }
                case 'D': {
                    // 带序数词后缀的日期（如1st, 2nd, 11th）
                    result += std::to_string(day); // 先添加数字部分
                    // 处理特殊情况：11-13号统一使用th后缀（避免11st、12nd等错误）
                    if (day >= 11 && day <= 13) {
                        result += "th";
                    } else {
                        // 根据个位数字添加后缀
                        switch (day % 10) {
                            case 1: result += "st"; break; // 1st
                            case 2: result += "nd"; break; // 2nd
                            case 3: result += "rd"; break; // 3rd
                            default: result += "th"; break; // 其他情况（如4th, 5th等）
                        }
                    }
                    break;
                }
                case '%': {
                    // 转义%字符，直接输出%
                    result += "%";
                    break;
                }
                default: {
                    // 非占位符字符直接添加到结果
                    result += std::string(1, format_str[i]);
                    break;
                }
            }
        } else {
            // 非占位符字符直接添加到结果
            result += format_str[i];
        }
    }

    // 设置返回值类型为字符串类型
    value.set_type(AttrType::CHARS);
    // 设置返回值数据（使用C风格字符串和长度，避免字符串末尾'\0'的影响）
    value.set_data(result.c_str(), result.length());
    return rc;
}

//////////////////////////////////////////////////////////////////////////////
// SubqueryExpr::SubqueryExpr(ParsedSqlNode *sub_query_sn) : sub_query_sn_(sub_query_sn) {}

// void SubqueryExpr::set_logical_operator(std::unique_ptr<LogicalOperator> logical_operator)
// {
//   logical_operator_ = std::move(logical_operator);
// }
// void SubqueryExpr::set_physical_operator(std::unique_ptr<PhysicalOperator> physical_operator)
// {
//   physical_operator_ = std::move(physical_operator);
// }


// /**
//  * 检查子查询表达式的有效性
//  * 
//  * 此函数验证子查询是否符合以下规则：
//  * 1. 子查询的SELECT列表中只能包含一个字段表达式或星号表达式
//  * 2. 星号表达式和字段表达式不能同时存在
//  * 3. 如果使用了星号表达式，所有关联表的字段总数必须为1
//  * 
//  * @param db 数据库实例指针
//  * @return RC 返回状态码，表示检查结果
//  */
// RC SubqueryExpr::check_sub_query(Db *db)
// {
//   // 初始化字段表达式和星号表达式指针
//   UnboundFieldExpr *field_expr = nullptr;
//   StarExpr         *star_expr  = nullptr;
  
//   // 遍历子查询选择列表中的所有表达式
//   for (auto &expr : sub_query_sn_->selection.expressions) {
//     // 确保只存在一个字段表达式
//     if (field_expr != nullptr) {
//       LOG_WARN("invalid subquery attributes. It should be only one");
//       return RC::INVALID_ARGUMENT;
//     }
    
//     // 识别并记录字段表达式或星号表达式
//     if (expr->type() == ExprType::UNBOUND_FIELD) {
//       field_expr = static_cast<UnboundFieldExpr *>(expr.get());
//     } else if (expr->type() == ExprType::STAR) {
//       star_expr = static_cast<StarExpr *>(expr.get());
//     }
//   }
  
//   // 验证星号表达式和字段表达式不同时存在
//   if (field_expr != nullptr && star_expr != nullptr) {
//     LOG_WARN("star_expr and unbounded_field_expr cannot be used together in subquery");
//     return RC::INVALID_ARGUMENT;
//   }
  
//   // 如果使用了星号表达式，验证其有效性
//   if (star_expr != nullptr) {
//     int fields_num = 0;
    
//     // 遍历所有关联表，计算总字段数
//     for (size_t j = 0; j < sub_query_sn_->selection.relations.size(); ++j) {
//       const char *table_name = sub_query_sn_->selection.relations[j].relation_name.c_str();
      
//       // 验证表名有效性
//       if (nullptr == table_name) {
//         LOG_WARN("invalid argument. relation name is null. index=%d", j);
//         return RC::INVALID_ARGUMENT;
//       }
      
//       // 验证表是否存在
//       Table *table = db->find_table(table_name);
//       if (nullptr == table) {
//         LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
//         return RC::SCHEMA_TABLE_NOT_EXIST;
//       }
      
//       // 累加表的字段数量
//       fields_num += table->table_meta().field_num();
//     }
    
//     // 确保总字段数为1
//     if (fields_num != 1) {
//       LOG_WARN("invalid subquery attributes");
//       return RC::INVALID_ARGUMENT;
//     }
//   }
  
//   // 所有验证通过
//   return RC::SUCCESS;
// }

// /**
//  * 在事务上下文中执行子查询并获取结果值
//  * 
//  * 此函数执行子查询表达式并返回单个值，遵循以下流程：
//  * 1. 验证查询操作符是否初始化
//  * 2. 打开物理操作符（如果尚未打开）
//  * 3. 获取子查询的下一个元组
//  * 4. 验证元组只包含一个单元格
//  * 5. 提取结果值并处理资源清理
//  * 
//  * @param tuple 输入元组（用于相关子查询）
//  * @param value 输出值（存储子查询结果）
//  * @param trx 当前事务指针
//  * @return RC 返回状态码，表示执行结果
//  */
// RC SubqueryExpr::get_value_with_trx(const Tuple &tuple, Value &value, Trx *trx) const
// {
//   RC rc = RC::SUCCESS;

//   // 验证查询操作符已初始化
//   if (logical_operator_ == nullptr && physical_operator_ == nullptr) {
//     return RC::RECORD_EOF;
//   }

//   if (physical_operator_ == nullptr) {
//     LOG_WARN("physical operator is null");
//     return RC::INVALID_ARGUMENT;
//   }

//   // 设置当前事务上下文
//   trx_ = trx;

//   // 转换为非const引用以便传递给可能修改状态的函数
//   auto *tuple__ = const_cast<Tuple *>(&tuple);
  
//   // 首次执行时初始化物理操作符
//   if (!is_open_) {
//     rc = open_physical_operator(tuple__);
//     if (rc != RC::SUCCESS) {
//       LOG_WARN("failed to open physical operator. rc=%s", strrc(rc));
//       return rc;
//     }
//   }

//   // 执行物理操作获取下一个元组
//   rc = physical_operator_->next();
//   if (rc != RC::SUCCESS) {
//     if (rc != RC::RECORD_EOF) {
//       // 发生错误时关闭操作符并返回错误码
//       close_physical_operator();
//       LOG_PANIC("failed to get next tuple. rc=%s", strrc(rc));
//       return rc;
//     }
    
//     // 处理查询结束情况
//     rc = close_physical_operator();
//     if (rc == RC::SUCCESS) {
//       rc = RC::RECORD_EOF;
//     } else {
//       LOG_PANIC("failed to close physical operator. rc=%s", strrc(rc));
//     }
//     return rc;
//   }
  
//   // 获取当前元组
//   auto tuple_ = physical_operator_->current_tuple();
  
//   // 验证子查询结果格式（必须恰好包含一个字段）
//   if (tuple_->cell_num() > 1) {
//     LOG_WARN("tuple cell count is not 1");
//     close_physical_operator();
//     return RC::INVALID_ARGUMENT;
//   }
  
//   // 处理空结果情况（通常不应该发生，因为上层已验证）
//   if (tuple_->cell_num() == 0) {
//     LOG_WARN("A warn from SubqueryExpr: tuple cell count is 0");
//   }
  
//   // 提取结果值（第一个也是唯一的单元格）
//   tuple_->cell_at(0, value);
//   return rc;
// }

// RC SubqueryExpr::get_value(const Tuple &tuple, Value &value) const 
// { 
//   return get_value_with_trx(tuple, value, nullptr); 
// }


// RC SubqueryExpr::open_physical_operator(Tuple *outer_tuple) const
// {
//   if (physical_operator_ == nullptr) {
//     LOG_WARN("physical operator is null");
//     return RC::INVALID_ARGUMENT;
//   }
//   // 将外层的 tuple 传递给子查询算子
//   physical_operator_->set_outer_tuple(outer_tuple);
//   RC rc = physical_operator_->open(trx_);
//   if (rc != RC::SUCCESS) {
//     LOG_WARN("failed to open physical operator. rc=%s", strrc(rc));
//   } else {
//     is_open_ = true;
//   }
//   return rc;
// }

// RC SubqueryExpr::close_physical_operator() const
// {
//   if (physical_operator_ == nullptr) {
//     LOG_WARN("physical operator is null");
//     return RC::INVALID_ARGUMENT;
//   }
//   RC rc = physical_operator_->close();
//   if (rc != RC::SUCCESS) {
//     LOG_WARN("failed to close physical operator. rc=%s", strrc(rc));
//   } else {
//     is_open_ = false;
//   }
//   return rc;
// }

// ////////////////////////////////////////////////////////////////////////////////
// RC ValueListExpr::get_value(const Tuple &tuple, Value &value) const
// {
//   if (index_ >= values_.size()) {
//     index_ = 0;
//     return RC::RECORD_EOF;
//   }
//   value = values_[index_++];
//   return RC::SUCCESS;
// }