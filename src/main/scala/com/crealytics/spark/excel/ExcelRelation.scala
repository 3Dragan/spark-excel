package com.crealytics.spark.excel

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

case class ExcelRelation(
                          location: String,
                          sheetName: Option[String],
                          useHeader: Boolean,
                          treatEmptyValuesAsNulls: Boolean,
                          inferSheetSchema: Boolean,
                          addColorColumns: Boolean = true,
                          userSchema: Option[StructType] = None,
                          startColumn: Int = 0,
                          endColumn: Int = Int.MaxValue
                        )
                        (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan {
  private lazy val path = new Path(location)
  private lazy val inputStream = FileSystem.get(path.toUri, sqlContext.sparkContext.hadoopConfiguration).open(path)
  private lazy val extractor = Extractor(useHeader,
    inputStream,
    sheetName,
    startColumn,
    endColumn)
  private lazy val firstRowWithData = extractor.firstRowWithData

  override val schema: StructType = inferSchema

  override def buildScan: RDD[Row] = buildScan(schema.map(_.name).toArray)

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val results = extractor.extract(schema, requiredColumns)
    sqlContext.sparkContext.parallelize(results.map(Row.fromSeq))
  }

  private def parallelize[T: scala.reflect.ClassTag](seq: Seq[T]): RDD[T] = sqlContext.sparkContext.parallelize(seq)

  private def inferSchema: StructType =
    this.userSchema.getOrElse {
      val header = firstRowWithData.zipWithIndex.map {
        case (Some(value), _) if useHeader => value.getStringCellValue
        case (_, index) => s"C$index"
      }
      val baseSchema = if (this.inferSheetSchema) {
        val stringsAndCellTypes = extractor.stringsAndCellTypes
        InferSchema(parallelize(stringsAndCellTypes), header.toArray)
      } else {
        // By default fields are assumed to be StringType
        val schemaFields = header.map { fieldName =>
          StructField(fieldName.toString, StringType, nullable = true)
        }
        StructType(schemaFields)
      }
      if (addColorColumns) {
        header.foldLeft(baseSchema) { (schema, header) => schema.add(s"${header}_color", StringType, nullable = true) }
      } else {
        baseSchema
      }
    }
}
