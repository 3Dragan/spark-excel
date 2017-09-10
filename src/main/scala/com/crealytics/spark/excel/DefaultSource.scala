package com.crealytics.spark.excel

import com.crealytics.spark.excel.utils.{ExcelCreatableRelationProvider, ParameterChecker}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType


class DefaultSource
  extends RelationProvider
  with ExcelCreatableRelationProvider
{

  /**
    * Creates a new relation for retrieving data from an Excel file
    */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]
  ): ExcelRelation =
    createRelation(sqlContext, parameters, null)

  /**
    * Creates a new relation for retrieving data from an Excel file
    */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType
  ): ExcelRelation = {
    ExcelRelation(
      location = ParameterChecker.check(parameters, "path"),
      sheetName = parameters.get("sheetName"),
      useHeader = ParameterChecker.check(parameters, "useHeader").toBoolean,
      treatEmptyValuesAsNulls = parameters.get("treatEmptyValuesAsNulls").fold(true)(_.toBoolean),
      userSchema = Option(schema),
      inferSheetSchema = parameters.get("inferSchema").fold(false)(_.toBoolean),
      addColorColumns = parameters.get("addColorColumns").fold(false)(_.toBoolean),
      startColumn = parameters.get("startColumn").fold(0)(_.toInt),
      endColumn = parameters.get("endColumn").fold(Int.MaxValue)(_.toInt),
      timestampFormat = parameters.get("timestampFormat")
    )(sqlContext)
  }
}
