package com.tutorial.custom.datasource.engine

import com.tutorial.custom.datasource.exceptions.TypeNotSupportedException
import org.apache.spark.sql.types.{
  BooleanType, DateType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, TimestampType
}

case class DbColumn(name: String, number: Long, dataType: String, isNullable: Boolean) {
  val sparkColumn: StructField = {
    dataType match {
      case "java.lang.Boolean" => StructField(name = name, dataType = BooleanType, nullable = isNullable)
      case "java.lang.Integer" => StructField(name = name, dataType = IntegerType, nullable = isNullable)
      case "java.lang.Long" => StructField(name = name, dataType = LongType, nullable = isNullable)
      case "java.lang.Float" => StructField(name = name, dataType = FloatType, nullable = isNullable)
      case "java.lang.Double" => StructField(name = name, dataType = DoubleType, nullable = isNullable)
      case "java.math.BigDecimal" => StructField(name = name, dataType = DoubleType, nullable = isNullable)
      case "java.lang.String" => StructField(name = name, dataType = StringType, nullable = isNullable)
      case "java.sql.Timestamp" => StructField(name = name, dataType = TimestampType, nullable = isNullable)
      case "java.sql.Date" => StructField(name = name, dataType = DateType, nullable = isNullable)
      case _ => throw new TypeNotSupportedException(s"Column ${name} of type ${dataType} is not supported, please select the column with appropriate cast")
    }
  }
}
