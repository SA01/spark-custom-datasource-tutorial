package com.tutorial.custom.datasource

import com.tutorial.custom.datasource.engine.{DataPartition, DbConnectorProperties, DbQuery, DriverProvider, PushedFilter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

import java.sql.Connection

class DataScan(
                dbQuery: DbQuery,
                dbConnectorProperties: DbConnectorProperties,
                providedNumResultPartitions: Option[Long],
                pushedFilters: Iterable[PushedFilter]
              ) extends Scan with Batch with DriverProvider with Logging {
  private lazy val numberOfResultPartitions: Long = {
    providedNumResultPartitions match {
      case Some(numPart) => numPart
      case None => 1
    }
  }

  override def readSchema(): StructType = {
    // TODO: For demonstration
    println("!===================================================! DataScan readSchema called")
    dbQuery.dataSchema
  }

  override def toBatch: Batch = {
    // TODO: For demonstration
    println("!===================================================! DataScan toBatch called")
    this
  }

  override def planInputPartitions(): Array[InputPartition] = {
    // TODO: For demonstration
    println("!===================================================! DataScan planInputPartitions called")
    val partitions = createPartitions()
    log.info(s"Number of partitions planned: ${partitions.length}")
    partitions.foreach(part => log.info(part.toString))

    partitions.map(part => part.asInstanceOf[InputPartition])
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // TODO: For demonstration
    println("!===================================================! DataScan createReaderFactory called")
    new DataPartitionReaderFactory(dbConnectorProperties = dbConnectorProperties)
  }

  private def createPartitions(): Array[DataPartition] = {
    val dataPartitions: Array[DataPartition] = {
      val numRows = getNumberOfQueryRows

      if (numRows <= (numberOfResultPartitions * 2)) {
        Array(DataPartition(startRow = 0, endRow = numRows, dbQuery = dbQuery, pushedFilters = pushedFilters))
      } else {
        val stride = math.ceil((numRows * 1.0) / numberOfResultPartitions).toLong
        val range = (1L to numberOfResultPartitions)
        range.map(partitionNum => {
          val startRow = stride * (partitionNum - 1)
          val endRow = math.min((stride * partitionNum), numRows)
          DataPartition(dbQuery = dbQuery, startRow = startRow, endRow = endRow, pushedFilters = pushedFilters)
        }).toArray
      }
    }

    dataPartitions
  }

  private def getNumberOfQueryRows: Long = {
    var connection: Connection = null
    try {
      val driver = getConfiguredDriver(dbConnectorProperties = dbConnectorProperties)

      connection = driver.connect(
        dbConnectorProperties.getConnectionUrl,
        dbConnectorProperties.getConnectionProperties
      )

      val numRowsQuery = dbQuery.numRowsQuery

      val statement = connection.createStatement()
      val res = statement.executeQuery(numRowsQuery)
      res.next()

      val resultMetadata = res.getMetaData
      val result = res.getLong("num_rows")

      println("column count")
      println(resultMetadata.getColumnCount)

      println(s"result: ${result}")
      result
    } catch {
      case e: Exception => throw e
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }
}
