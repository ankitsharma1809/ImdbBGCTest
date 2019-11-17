package com.bcgp.imdb.spark

import com.bcgp.imdb.spark.Data._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ col, lit, collect_list }
import org.apache.spark.sql.types._

object Model {

  def topNMovies(noOfItems: Int): DataFrame = {

    if(hasData(titleBasicsDf))
    {
    val joineDf = titleBasicsDf
      .drop("isAdult", "startYear", "endYear", "runtimeMinutes", "genres")
      .filter(col("titleType") === "movie")
      .join(titleRatingsDf, Seq("tconst"))

    val avgVotesDf = joineDf
      .withColumn("numOfVotes", col("numVotes").cast(LongType)).drop("numVotes")
      .groupBy("titleType").avg("numOfVotes")
      .withColumnRenamed("avg(numOfVotes)", "averageNumberOfVotes")

    val titleRatingsWithAvgVotes = joineDf.join(avgVotesDf, Seq("titleType"))
      .withColumn("numOfVotes", col("numVotes").cast(LongType)).drop("numVotes", "titleType")

    titleRatingsWithAvgVotes.filter(col("numOfVotes") >= 50)
      .withColumn("avgRatings", col("averageRating").cast(DoubleType))
      .withColumn("ranking", col("numOfVotes") / col("averageNumberOfVotes") * col("avgRatings"))
      .drop("avgRatings", "averageNumberOfVotes")
      .orderBy(col("ranking").desc)
      .limit(noOfItems)
    } else {
      println("No data present for title Basics")
      spark.emptyDataFrame
    }
  }

  def topNMovWithMostCreditedPersons(noOfItems: Int): DataFrame = {
    
    if(hasData(nameBasicsDf))
    {
    val nameBasicLessColDf = nameBasicsDf.drop("nconst", "birthYear", "deathYear", "primaryProfessions")

    topNMovies(noOfItems).join(nameBasicLessColDf, col("knownForTitles").contains(col("tconst")))
      .drop("knownForTitles")
      .groupBy("tconst", "primaryTitle", "ranking")
      .agg(collect_list("primaryName").as("MostlyCredited"))
      .drop("tconst")
      .orderBy(col("ranking").desc)
    } else {
      println("No Data present for Name Basics")
      spark.emptyDataFrame
    }
  }
  
  def topNMovWithAllKnownTitles(noOfItems: Int): DataFrame = {
    
    if(hasData(titleAkasDf))
    {
    val titleAksLessColDf = titleAkasDf.drop("ordering", "region", "language", "types", "attributes", "isOriginalTitle")
    
    topNMovies(noOfItems).join(titleAksLessColDf, col("tconst") === col("titleId"))
      .drop("titleId")
      .groupBy("tconst", "primaryTitle", "ranking")
      .agg(collect_list("title").as("DifferentTitles"))
      .orderBy(col("ranking").desc)
    } else {
      println("No data present for Title Aka")
      spark.emptyDataFrame
    }
  }

}