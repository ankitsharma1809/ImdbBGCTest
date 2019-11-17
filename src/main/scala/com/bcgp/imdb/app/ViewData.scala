package com.bcgp.imdb.app

import org.apache.spark.sql.functions.{ col, lit, collect_list }

import scopt.OptionParser
import com.bcgp.imdb.spark.Model._

case class Config(viewResult: String = "", numerOfItems: Int = 20)

object ViewData extends App {

  val parser = new OptionParser[Config]("ViewData") {
    opt[String]("view") required() action { (x, c) =>
      c.copy(viewResult = x)
    } text("Results to view")
    opt[Int]("numerOfItems") optional() action { (x, c) =>
      c.copy(numerOfItems = x)
    } text("Number of movies to view")
    help("help") text ("""Run example: View --view [metrics] --numerOfItems [n]
    Value of 'metrics' parameter can be one of these -
    TopMovies             - show top n movies with a minimum of 50 votes with ranking.
    MostCreditedPerson    - list the persons who are most often credited for top n movies.
    AllKnownTitles        - list the different titles of the top n movies.
    
    Value of 'n' is optional, by default it will show 20 items.
    """)
  }

  // parser.parse
  parser.parse(args, Config()) map { config =>

    config.viewResult match {
      case "TopMovies"          => topNMovies(config.numerOfItems).show(config.numerOfItems, false)
      case "MostCreditedPerson" => topNMovWithMostCreditedPersons(config.numerOfItems).show(config.numerOfItems, false)
      case "AllKnownTitles"     => topNMovWithAllKnownTitles(config.numerOfItems).show(config.numerOfItems, false)
      case _                    => println(s"Bad argument value passed - [${config.viewResult}], check --help..")
    }
  } getOrElse {
    println("Bad option passed, check --help..")
  }

}