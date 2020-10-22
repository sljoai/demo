package com.song.cn.spark.scala.mllib

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{BooleanType, NumericType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class FillMissingValueTransformer extends Transformer {
  final val inputCols = new Param[Array[String]](this, "inputCol", "The input column")
  val uid: String = Identifiable.randomUID("MissingValueTransformer")

  override def transformSchema(schema: StructType): StructType = {
    // 检查输入和输出是否复合要求，比如数据类型
    // 返回处理之后的schema
    val inputColNames = $(inputCols)
    val incorrectColumns = inputColNames.flatMap { name =>
      schema(name).dataType match {
        case _: NumericType | BooleanType => None
        case other => Some(s"Data type $other of column $name is not supported.")
      }
    }
    if (incorrectColumns.nonEmpty) {
      throw new IllegalArgumentException(incorrectColumns.mkString("\n"))
    }
    StructType(schema.fields)
  }

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val inputColNames = $(inputCols)
    var rawData = dataset
    for (i <- inputColNames) {
      rawData = rawData.drop(i)
    }
    val allColumnNames = dataset.columns
    // 过滤掉不需要的列名
    val columnNames = allColumnNames.filter {
      !inputColNames.contains(_)
    }
    // 心率的空值填充为60，其他属性的空值填充为0
    var imputedValues: Map[String, Double] = Map()
    for (colName <- columnNames) {
      if (colName == "hr") {
        imputedValues += (colName -> 60.0)
      } else {
        imputedValues += (colName -> 0.0)
      }
    }
    val processData = rawData.na.drop(26, columnNames).na.fill(imputedValues)
    processData.toDF()
  }
}
