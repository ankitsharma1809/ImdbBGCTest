package com.bcgp.imdb

import org.apache.spark.sql.DataFrame

package object spark {
  def hasData(df: DataFrame): Boolean = {
    df.head(1).isEmpty
  }
}