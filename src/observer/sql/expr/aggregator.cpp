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
// Created by Wangyunlai on 2024/05/29.
//

#include "sql/expr/aggregator.h"
#include "common/log/log.h"

RC SumAggregator::accumulate(const Value &value)
{
  if (value.is_null()) {
    return RC::SUCCESS; // 跳过 NULL
  }

  if (value_.attr_type() == AttrType::UNDEFINED || value_.is_null()) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));
  
  Value::add(value, value_, value_);
  return RC::SUCCESS;
}

RC SumAggregator::evaluate(Value& result)
{
  if (value_.attr_type() == AttrType::UNDEFINED || value_.is_null()) {
    result.set_null();
  } else {
    result = value_;
  }

  return RC::SUCCESS;
}


RC MaxAggregator::accumulate(const Value &value)
{
  if (value.is_null()) {
    return RC::SUCCESS;
  }

  if (!has_value_) {
    value_ = value;
    has_value_ = true;
  } else if (value.compare(value_) > 0) {
    value_ = value;
  }

  return RC::SUCCESS;
}

RC MaxAggregator::evaluate(Value &result)
{
  if (!has_value_) {
    result.set_null();
  } else {
    result = value_;
  }
  return RC::SUCCESS;
}


RC MinAggregator::accumulate(const Value &value)
{
  if (value.is_null()) {
    return RC::SUCCESS;
  }

  if (!has_value_) {
    value_ = value;
    has_value_ = true;
  } else if (value.compare(value_) < 0) {
    value_ = value;
  }

  return RC::SUCCESS;
}

RC MinAggregator::evaluate(Value &result)
{
  if (!has_value_) {
    result.set_null();
  } else {
    result = value_;
  }
  return RC::SUCCESS;
}

RC CountAggregator::accumulate(const Value &value)
{
  LOG_DEBUG("count is %ld", count_);
  if (value.is_null()) {
    return RC::SUCCESS;
  }
  count_++;
  return RC::SUCCESS;
}

RC CountAggregator::evaluate(Value &result)
{
  result.set_int(count_);
  return RC::SUCCESS;
}

RC AvgAggregator::accumulate(const Value &value)
{
  if (value.is_null()) {
    return RC::SUCCESS; // 跳过
  }  

  if (sum_.attr_type() == AttrType::UNDEFINED) {
    sum_ = value;
  } else {
    Value::add(value, sum_, sum_);
  }

  count_++;
  return RC::SUCCESS;
}

RC AvgAggregator::evaluate(Value &result)
{
  if (count_ == 0) {
    result.set_null();
    return RC::SUCCESS;
  }

  if (sum_.attr_type() == AttrType::INTS) {
    result.set_float(static_cast<float>(sum_.get_int()) / count_);
  } else if (sum_.attr_type() == AttrType::FLOATS) {
    result.set_float(sum_.get_float() / count_);
  } else {
    result.set_null(); // 不支持的类型，可以抛 INVALID_TYPE
  }

  return RC::SUCCESS;
}