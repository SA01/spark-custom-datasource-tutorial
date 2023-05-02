package com.tutorial.custom.datasource

import com.tutorial.custom.datasource.engine.{DataPartition, DbConnectorProperties, DriverProvider}
import com.tutorial.custom.datasource.exceptions.{TypeNotSupportedException, ValueCastException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.{
  BooleanType, DateType, DoubleType, FloatType, IntegerType, LongType, StringType, TimestampType
}

import java.lang
import java.sql.Connection

class DataPartitionReader(dbConnectorProperties: DbConnectorProperties, partitionToRead: DataPartition)
  extends PartitionReader[InternalRow] with DriverProvider with Logging {
  private var curIndex: Int = 0
  private lazy val dataRows: Array[Array[Any]] = {
    getRowsForQuery(query = partitionToRead.queryWithAppliedConstraints)
  }

  private def createDbConnection() = {
    val driver = getConfiguredDriver(dbConnectorProperties = dbConnectorProperties)

    val connection = driver.connect(
      dbConnectorProperties.getConnectionUrl,
      dbConnectorProperties.getConnectionProperties
    )

    connection
  }

  private def castToType(fieldName: String, value: Object, typeName: String): Any = {
    val booleanType = BooleanType.typeName
    val integerType = IntegerType.typeName
    val longType = LongType.typeName
    val floatType = FloatType.typeName
    val doubleType = DoubleType.typeName
    val stringType = StringType.typeName
    val timestampType = TimestampType.typeName
    val dateType = DateType.typeName

    try {
      typeName match {
        case `booleanType` => value.asInstanceOf[java.lang.Boolean]
        case `integerType` => value.asInstanceOf[java.lang.Integer]
        case `longType` => value.asInstanceOf[java.lang.Long]
        case `floatType` => value.asInstanceOf[java.lang.Float]
        case `doubleType` => {
          value match {
            case bigDecimal: java.math.BigDecimal => bigDecimal.doubleValue()
            case _ => value.asInstanceOf[lang.Double]
          }
        }
        case `stringType` => org.apache.spark.unsafe.types.UTF8String.fromString(value.asInstanceOf[java.lang.String])
        case `timestampType` => {
          if (value != null) value.asInstanceOf[java.sql.Timestamp].toInstant.toEpochMilli * 1000 else null
        }
        case `dateType` => {
          if (value != null) value.asInstanceOf[java.sql.Date].toLocalDate.toEpochDay.toInt else null
        }
        case _ => throw new TypeNotSupportedException(s"Type ${typeName} is not supported")
      }
    } catch {
      case exception: Exception => {
        val message =
          s"There was an error parsing value `${value}` in field `${fieldName}` to type `${typeName}`: ${exception}"
        log.info(message)
        throw new ValueCastException(message)
      }
    }
  }

  private def getRowsForQuery(query: String): Array[Array[Any]] = {
    var connection: Connection = null
    val resData = scala.collection.mutable.ArrayBuffer[Array[Any]]()
    try {
      connection = createDbConnection()

      val statement = connection.createStatement()

      log.info(s"Running Query to get data: ${query}")
      val results = statement.executeQuery(query)

      while(results.next()) {
        val resultRow = partitionToRead.dbQuery.dataSchema.fields.map(field => {
          castToType(fieldName = field.name, value = results.getObject(field.name), typeName = field.dataType.typeName)
        })
        resData.append(resultRow)
      }

      log.info(s"Fetched ${resData.length} rows")
      resData.toArray

    } catch {
      case exception: Exception => {
        log.error(s"Exception while reading data: ${exception}")
        throw exception
      }
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  override def next(): Boolean = {
    curIndex < dataRows.length
  }

  override def get(): InternalRow = {
    val rowToReturn = dataRows(curIndex)
    curIndex += 1
    InternalRow.apply(rowToReturn: _*)
  }

  override def close(): Unit = {}
}
