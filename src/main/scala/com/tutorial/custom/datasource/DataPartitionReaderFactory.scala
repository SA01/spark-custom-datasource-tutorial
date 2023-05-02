package com.tutorial.custom.datasource

import com.tutorial.custom.datasource.engine.{DataPartition, DbConnectorProperties}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class DataPartitionReaderFactory(dbConnectorProperties: DbConnectorProperties) extends PartitionReaderFactory with Logging {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    println("!!==============================================================!! DataPartitionReaderFactory createReader called")
    new DataPartitionReader(
      dbConnectorProperties = dbConnectorProperties,
      partitionToRead = partition.asInstanceOf[DataPartition]
    )
  }
}
