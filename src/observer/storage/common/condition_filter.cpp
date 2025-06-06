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
// Created by Wangyunlai on 2021/5/7.
//

#include "condition_filter.h"
#include "common/log/log.h"
#include "common/type/attr_type.h"
#include "common/value.h"
#include "storage/record/record_manager.h"
#include "storage/table/table.h"
#include "sql/expr//expression.h"
#include <math.h>
#include <stddef.h>

using namespace common;

ConditionFilter::~ConditionFilter() {}

DefaultConditionFilter::DefaultConditionFilter()
{
  left_.is_attr     = false;
  left_.attr_length = 0;
  left_.attr_offset = 0;

  right_.is_attr     = false;
  right_.attr_length = 0;
  right_.attr_offset = 0;
}
DefaultConditionFilter::~DefaultConditionFilter() {}

RC DefaultConditionFilter::init(const ConDesc &left, const ConDesc &right, AttrType attr_type, CompOp comp_op)
{
  if (attr_type <= AttrType::UNDEFINED || attr_type >= AttrType::MAXTYPE) {
    LOG_ERROR("Invalid condition with unsupported attribute type: %d", attr_type);
    return RC::INVALID_ARGUMENT;
  }

  if (comp_op < EQUAL_TO || comp_op >= NO_OP) {
    LOG_ERROR("Invalid condition with unsupported compare operation: %d", comp_op);
    return RC::INVALID_ARGUMENT;
  }

  left_      = left;
  right_     = right;
  attr_type_ = attr_type;
  comp_op_   = comp_op;
  return RC::SUCCESS;
}

RC DefaultConditionFilter::init(Table &table, const ConditionSqlNode &condition)
{
  const TableMeta &table_meta = table.table_meta();
  ConDesc          left;
  ConDesc          right;

  AttrType type_left  = AttrType::UNDEFINED;
  AttrType type_right = AttrType::UNDEFINED;

  Expression *left_expr  = condition.left_expr.get();
  auto        right_expr = condition.right_expr.get();
  if (left_expr->type() == ExprType::VALUE) {
    left.is_attr     = false;
    left.value       = static_cast<ValueExpr *>(left_expr)->get_value();
    type_left        = left.value.attr_type();
    left.attr_length = 0;
    left.attr_offset = 0;
  } else if (left_expr->type() == ExprType::FIELD) {
    left.is_attr                = true;
    const FieldMeta *field_left = table_meta.field(static_cast<FieldExpr *>(left_expr)->field_name());
    if (nullptr == field_left) {
      LOG_WARN("No such field in condition. %s.%s", table.name(), static_cast<FieldExpr *>(left_expr)->field_name());
      return RC::SCHEMA_FIELD_MISSING;
    }
    left.attr_length = field_left->len();
    left.attr_offset = field_left->offset();
    type_left        = field_left->type();
  } else {
    LOG_ERROR("Invalid condition with unsupported expression type: %d", left_expr->type());
    return RC::INVALID_ARGUMENT;
  }

  if (right_expr->type() == ExprType::VALUE) {
    right.is_attr     = false;
    right.value       = static_cast<ValueExpr *>(right_expr)->get_value();
    type_right        = right.value.attr_type();
    right.attr_length = 0;
    right.attr_offset = 0;
  } else if (right_expr->type() == ExprType::FIELD) {
    right.is_attr                = true;
    const FieldMeta *field_right = table_meta.field(static_cast<FieldExpr *>(right_expr)->field_name());
    if (nullptr == field_right) {
      LOG_WARN("No such field in condition. %s.%s", table.name(), static_cast<FieldExpr *>(right_expr)->field_name());
      return RC::SCHEMA_FIELD_MISSING;
    }
    right.attr_length = field_right->len();
    right.attr_offset = field_right->offset();
    type_right        = field_right->type();
  } else {
    LOG_ERROR("Invalid condition with unsupported expression type: %d", right_expr->type());
    return RC::INVALID_ARGUMENT;
  }

  // 校验和转换
  //  if (!field_type_compare_compatible_table[type_left][type_right]) {
  //    // 不能比较的两个字段， 要把信息传给客户端
  //    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  //  }
  // NOTE：这里没有实现不同类型的数据比较，比如整数跟浮点数之间的对比
  // 但是选手们还是要实现。这个功能在预选赛中会出现

  
  if (condition.comp_op == LIKE_OP || condition.comp_op == NOT_LIKE_OP ) {
    // LIKE 操作要求两边都是字符串类型
    if (type_left != AttrType::CHARS || type_right != AttrType::CHARS) {
      LOG_WARN("LIKE operation requires string type on both sides");
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  } 

  if (type_left != type_right) {
    if (type_left == AttrType::FLOATS && type_right == AttrType::INTS) {
      // do nothing
    } else if (type_left == AttrType::INTS && type_right == AttrType::FLOATS) {
      // do nothing
    } else {
      LOG_ERROR("Invalid condition with different attribute types: %d, %d", type_left, type_right);
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  }

  return init(left, right, type_left, condition.comp_op);
}

bool DefaultConditionFilter::filter(const Record &rec) const
{
  Value left_value;
  Value right_value;

  if (left_.is_attr) {  // value
    left_value.set_type(attr_type_);
    left_value.set_data(rec.data() + left_.attr_offset, left_.attr_length);
  } else {
    left_value.set_value(left_.value);
  }

  if (right_.is_attr) {
    right_value.set_type(attr_type_);
    right_value.set_data(rec.data() + right_.attr_offset, right_.attr_length);
  } else {
    right_value.set_value(right_.value);
  }

  // 处理 LIKE 操作
  if (comp_op_ == LIKE_OP || comp_op_ == NOT_LIKE_OP ) {
    bool match_result = like_match(left_value, right_value);
    return (comp_op_ == LIKE_OP) ? match_result : !match_result;
  }

  int cmp_result = left_value.compare(right_value);

  switch (comp_op_) {
    case EQUAL_TO: return 0 == cmp_result;
    case LESS_EQUAL: return cmp_result <= 0;
    case NOT_EQUAL: return cmp_result != 0;
    case LESS_THAN: return cmp_result < 0;
    case GREAT_EQUAL: return cmp_result >= 0;
    case GREAT_THAN: return cmp_result > 0;

    default: break;
  }

  LOG_PANIC("Never should print this.");
  return cmp_result;  // should not go here
}

CompositeConditionFilter::~CompositeConditionFilter()
{
  if (memory_owner_) {
    delete[] filters_;
    filters_ = nullptr;
  }
}

RC CompositeConditionFilter::init(const ConditionFilter *filters[], int filter_num, bool own_memory)
{
  filters_      = filters;
  filter_num_   = filter_num;
  memory_owner_ = own_memory;
  return RC::SUCCESS;
}
RC CompositeConditionFilter::init(const ConditionFilter *filters[], int filter_num)
{
  return init(filters, filter_num, false);
}

RC CompositeConditionFilter::init(Table &table, const ConditionSqlNode *conditions, int condition_num)
{
  if (condition_num == 0) {
    return RC::SUCCESS;
  }
  if (conditions == nullptr) {
    return RC::INVALID_ARGUMENT;
  }

  RC                rc                = RC::SUCCESS;
  ConditionFilter **condition_filters = new ConditionFilter *[condition_num];
  for (int i = 0; i < condition_num; i++) {
    DefaultConditionFilter *default_condition_filter = new DefaultConditionFilter();
    rc                                               = default_condition_filter->init(table, conditions[i]);
    if (rc != RC::SUCCESS) {
      delete default_condition_filter;
      for (int j = i - 1; j >= 0; j--) {
        delete condition_filters[j];
        condition_filters[j] = nullptr;
      }
      delete[] condition_filters;
      condition_filters = nullptr;
      return rc;
    }
    condition_filters[i] = default_condition_filter;
  }
  return init((const ConditionFilter **)condition_filters, condition_num, true);
}

bool CompositeConditionFilter::filter(const Record &rec) const
{
  for (int i = 0; i < filter_num_; i++) {
    if (!filters_[i]->filter(rec)) {
      return false;
    }
  }
  return true;
}

bool DefaultConditionFilter::like_match(const Value &left, const Value &right) const
{
  if (left.attr_type() != AttrType::CHARS || right.attr_type() != AttrType::CHARS) {
    return false;
  }

  
  std::string str_str = left.get_string();
  const char *str = str_str.c_str();

  std::string pattern_str = right.get_string();
  const char *pattern = pattern_str.c_str();

  if (str == nullptr || pattern == nullptr) {
    return false;
  }

  while (*pattern) {
    if (*pattern == '%') {
      pattern++;
      while (*str) {
        if (like_match(Value(str), Value(pattern))) {
          return true;
        }
        str++;
      }
      return *pattern == '\0';
    } 
    else if (*pattern == '_') {
      if (*str == '\0') return false;
      str++;
      pattern++;
    }
    else {
      if (*str != *pattern) return false;
      str++;
      pattern++;
    }
  }
  return *str == '\0';

}