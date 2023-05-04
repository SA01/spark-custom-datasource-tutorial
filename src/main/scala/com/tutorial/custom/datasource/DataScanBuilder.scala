package com.tutorial.custom.datasource

import com.tutorial.custom.datasource.engine.{
  DbConnectorProperties, DbQuery, FilterWithValue, NoValueFilter, PushedFilter
}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters}
import org.apache.spark.sql.sources.{
  EqualTo, Filter, GreaterThan, GreaterThanOrEqual, IsNotNull, IsNull, LessThan, LessThanOrEqual
}

class DataScanBuilder(
                       dbQuery: DbQuery,
                       dbConnectorProperties: DbConnectorProperties,
                       providedNumResultPartitions: Option[Long]
                     ) extends ScanBuilder with SupportsPushDownFilters with Logging {
  private var selectedPushedFilters = scala.collection.mutable.ArrayBuffer[PushedFilter]()
  override def build(): Scan = {
    // TODO: For demonstration
    println("!!====================================================!! DataScanBuilder: build called")
    new DataScan(
      dbQuery = dbQuery,
      dbConnectorProperties = dbConnectorProperties,
      providedNumResultPartitions = providedNumResultPartitions,
      pushedFilters = selectedPushedFilters
    )
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // TODO: For demonstration
    println("!!====================================================!! DataScanBuilder: pushFilters called")
    val filtersToApply = processFilters(filters = filters)
    val filtersToSkip = filters.filter(filter => !filtersToApply.map(_.rawFilter).toArray.contains(filter))

    log.info("Selected Push Filters: ")
    filtersToApply.foreach(filter => log.info(filter.toString))

    log.info("Filters to skip: ")
    filtersToSkip.foreach(filter => log.info(filter.toString))

    selectedPushedFilters.++=(filtersToApply)

    filtersToSkip
  }

  override def pushedFilters(): Array[Filter] = {
    // TODO: For demonstration
    println("!!====================================================!! DataScanBuilder: pushedFilters called")
    selectedPushedFilters.map(filter => filter.rawFilter).toArray
  }

  private def processFilters(filters: Array[Filter]): Iterable[PushedFilter] = {
    filters.map {
      case notNullFilter: IsNotNull => Some(
        NoValueFilter(
          column = notNullFilter.attribute,
          operator = "not null",
          rawFilter = notNullFilter
        )
      )
      case nullFilter: IsNull => Some(
        NoValueFilter(
          column = nullFilter.attribute,
          operator = " null",
          rawFilter = nullFilter
        )
      )
      case greaterThan: GreaterThan => {
        parsePushedFilter(
          attribute = greaterThan.attribute,
          fieldValue = greaterThan.value,
          operator = ">",
          rawFilter = greaterThan
        )
      }
      case greaterThanOrEqual: GreaterThanOrEqual => {
        parsePushedFilter(
          attribute = greaterThanOrEqual.attribute,
          fieldValue = greaterThanOrEqual.value,
          operator = ">=",
          rawFilter = greaterThanOrEqual
        )
      }
      case lessThan: LessThan => {
        parsePushedFilter(
          attribute = lessThan.attribute,
          fieldValue = lessThan.value,
          operator = "<",
          rawFilter = lessThan
        )
      }
      case lessThanOrEqual: LessThanOrEqual => {
        parsePushedFilter(
          attribute = lessThanOrEqual.attribute,
          fieldValue = lessThanOrEqual.value,
          operator = "<=",
          rawFilter = lessThanOrEqual
        )
      }
      case equalTo: EqualTo => {
        parsePushedFilter(
          attribute = equalTo.attribute,
          fieldValue = equalTo.value,
          operator = "=",
          rawFilter = equalTo
        )
      }
      case _ => None
    }
      .filter(item => item.nonEmpty)
      .map(_.get)
  }

  private def parsePushedFilter(
                                 attribute: String,
                                 fieldValue: Any,
                                 operator: String,
                                 rawFilter: Filter
                               ): Option[FilterWithValue] = {
    val fieldInSchema = dbQuery.dataSchema.fields.find(_.name == attribute)
    fieldInSchema match {
      case Some(field) => {
        if (PushedFilter.isFilterOfSupportedType(field.dataType.typeName)) {
          Some(
            FilterWithValue(
              column = attribute,
              operator = operator,
              value = fieldValue,
              valueInQuotes = PushedFilter.requireQuotesAroundValues(field.dataType.typeName),
              rawFilter = rawFilter
            )
          )
        } else {
          None
        }
      }
      case None => None
    }
  }
}
