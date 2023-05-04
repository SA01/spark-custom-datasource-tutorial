package com.tutorial.custom.datasource.engine

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType, StringType}

abstract class PushedFilter(val rawFilter: Filter) extends Serializable {
  def filterSqlClause: String
}

object PushedFilter {
  def isFilterOfSupportedType(fieldDataTypeName: String): Boolean = {
    fieldDataTypeName == StringType.typeName ||
      fieldDataTypeName == IntegerType.typeName ||
      fieldDataTypeName == FloatType.typeName ||
      fieldDataTypeName == DoubleType.typeName ||
      fieldDataTypeName == LongType.typeName
  }

  def requireQuotesAroundValues(fieldDataTypeName: String): Boolean = {
    fieldDataTypeName == StringType.typeName
  }
}

case class NoValueFilter(column: String, operator: String, override val rawFilter: Filter) extends PushedFilter(rawFilter) {
  override def filterSqlClause: String = s" ${column} is ${operator} "

  override def toString: String = {
    s"${operator} filter on ${column}"
  }
}

case class FilterWithValue(
                            column: String,
                            operator: String,
                            value: Any,
                            valueInQuotes: Boolean,
                            override val rawFilter: Filter
                          ) extends PushedFilter(rawFilter) {
  override def filterSqlClause: String = {
    if (valueInQuotes) {
      s"${column} ${operator} '${value}' "
    } else {
      s"${column} ${operator} ${value} "
    }
  }
  override def toString: String = {
    s"Filter on ${column} ${operator} ${value} - in quotes: ${valueInQuotes}"
  }
}
