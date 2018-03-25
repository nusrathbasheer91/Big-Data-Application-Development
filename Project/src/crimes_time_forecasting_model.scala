

package com.cloudera.tsexamples

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

import com.cloudera.sparkts._
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._

import com.cloudera.sparkts.models.ARIMA


def loadObservations(sqlContext: SQLContext, path: String): DataFrame = {
    val rowRdd = sqlContext.sparkContext.textFile(path).map { line =>
      val tokens = line.split('\t')
      val dt = ZonedDateTime.of(tokens(2).toInt, tokens(1).toInt, tokens(0).toInt, 0, 0, 0, 0,
        ZoneId.systemDefault())
      val crimeType = tokens(3)
      val count = tokens(4).toDouble
      Row(Timestamp.from(dt.toInstant), crimeType, count)
    }
    val fields = Seq(
      StructField("timestamp", TimestampType, true),
      StructField("crimeType", StringType, true),
      StructField("count", DoubleType, true)
    )
    val schema = StructType(fields)
    sqlContext.createDataFrame(rowRdd, schema)
  }


    val conf = new SparkConf().setAppName("Spark-TS Ticker Example").setMaster("local")
    conf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val tickerObs = loadObservations(sqlContext, "/user/sl5913/crdata")

	val minDate = tickerObs.selectExpr("min(timestamp)").collect()(0).getTimestamp(0)
   val maxDate = tickerObs.selectExpr("max(timestamp)").collect()(0).getTimestamp(0)
   val zone = ZoneId.systemDefault()

	  val dtIndex = DateTimeIndex.uniformFromInterval(ZonedDateTime.of(minDate.toLocalDateTime, zone),ZonedDateTime.of(maxDate.toLocalDateTime, zone),new DayFrequency(1))

    val tickerTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, tickerObs,
      "timestamp", "crimeType", "count")

	val YEARS=2;
	val dff = tickerTsrdd.mapSeries{vector => {val newVec = new org.apache.spark.mllib.linalg.DenseVector(vector.toArray.map(x => if(x.equals(Double.NaN)) 0 else x))
    val arimaModel = ARIMA.fitModel(1, 0, 0, newVec)
    val forecasted = arimaModel.forecast(newVec, YEARS)
    new org.apache.spark.mllib.linalg.DenseVector(forecasted.toArray.slice(forecasted.size-(YEARS+1), forecasted.size-1))
   }}.toDF("CrimeType","PredictedRate");
   dff.show()
	val crime_with_max_rate_1 = sqlContext.sql("""select CrimeType, PredictedRate from data where PredictedRate in (select max(PredictedRate) from data)""")
  crime_with_max_rate_1.show()
  val crime_with_min_rate_1 = sqlContext.sql("""select CrimeType, PredictedRate from data where PredictedRate in (select min(PredictedRate) from data)""")
