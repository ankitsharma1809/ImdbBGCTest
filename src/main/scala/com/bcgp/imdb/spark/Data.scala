package com.bcgp.imdb.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import scala.util._
import org.apache.spark.sql.functions.col

object Data extends SparkSessionBuilder {

  val nameBasicsDf = loadData("path_name.basics.tsv_data_files")
  val titleAkasDf = loadData("path_title.akas.tsv_data_files")
  val titleBasicsDf = loadData("path_title.basics.tsv_data_files")
  val titleCrewDf = loadData("path_title.crew.tsv_data_files")
  val titleEpisodeDf = loadData("path_title.episode.tsv_data_files")
  val titlePrincipalsDf = loadData("path_title.principals.tsv_data_files")
  val titleRatingsDf = loadData("path_title.ratings.tsv_data_files")
  
  private def loadData(path: String, delimiter: String = "\t")(implicit spark: SparkSession): DataFrame = {
    Try (
      spark.read.format("csv").
        option("header", "true").
        option("delimiter", delimiter).
        load(path)
      ) match {
      case Success(df) => df
      case Failure(ex) => println(ex.getMessage); spark.emptyDataFrame
    }
  }
}