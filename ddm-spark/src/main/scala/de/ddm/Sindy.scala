package de.ddm

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{array, array_contains, array_distinct, col, collect_list, collect_set, explode, expr, flatten, lit, monotonically_increasing_id, struct, when}
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}


object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    val dataFrames = inputs.map(file => readData(file, spark))

    val indexedDataFrames = dataFrames.map { df =>
      df.withColumn("rowIndex", monotonically_increasing_id())
    }

    val combinedDF = indexedDataFrames.reduce((df1, df2) => df1.join(df2, Seq("rowIndex"), "outer"))

    val combinedWithoutIndex = combinedDF.drop("rowIndex")

    val columnNames = combinedWithoutIndex.columns

    val finalOutput = combinedWithoutIndex.rdd
      .flatMap(row =>
        columnNames.zipWithIndex.collect {
          case (colName, index) if row.get(index) != null =>
            (row.get(index).toString, Set(colName))
        }
      )
      .reduceByKey(_ ++ _)
      .flatMap { case (_, columnSet) =>
        columnSet.map(col => (col, columnSet - col))
      }
      .groupByKey()
      .mapValues(sets =>
        if (sets.exists(_.isEmpty)) Set.empty[String]
        else sets.reduce(_ intersect _)
      )
      .filter { case (_, values) => values.nonEmpty }
      .map { case (key, values) => s"$key < ${values.mkString(", ")}" } // Format output

    finalOutput.foreach(println)
  }
}
