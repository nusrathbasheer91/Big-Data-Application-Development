//Steps to extract required columns of data

val df1 = sc.textFile("CRIMEDETECT/InputFilesMerge/crimeBIG.csv").map(_.split(","))
val df2 = sc.textFile("CRIMEDETECT/InputFilesMerge/taxstats.csv").map(_.split(","))
val df3 = sc.textFile("CRIMEDETECT/InputFilesMerge/2010_Census.csv").map(_.split(","))

val df11 = df1.filter(_.size==9)

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._

val rowRdd1 = df11.map { tokens =>
      Row( tokens(0), tokens(1),tokens(2),tokens(3),tokens(4),tokens(5),tokens(6),tokens(7),tokens(8))
    }
val fieldsdf1 = Seq(
     StructField("zip", StringType, true),
     StructField("date", StringType, true),
     StructField("year", StringType, true),
     StructField("crimecategorynumber", StringType, true),
	 StructField("crimecategorydesc", StringType, true),
	 StructField("statcode", StringType, true),
	 StructField("victimcount", StringType, true),
	 StructField("stationid", StringType, true),
	 StructField("crimeid", StringType, true)
	 )

val schema1 = StructType(fieldsdf1)
val sqldf1 = sqlContext.createDataFrame(rowRdd1,schema1).persist

val df21 = df2.filter(_.size==5)

val rowRdd2 = df2.map { tokens =>
      Row(tokens(0), tokens(1),tokens(2),tokens(3),tokens(4))
    }

val fieldsdf2 = Seq(
     StructField("zip", StringType, true),
     StructField("returns", StringType, true),
     StructField("ca agi", StringType, true),
     StructField("total tax", StringType, true),
     StructField("year", StringType, true)
	 )

val schema2 = StructType(fieldsdf2)
val sqldf2 = sqlContext.createDataFrame(rowRdd2,schema2).persist


val df3 = sc.textFile("CRIMEDETECT/InputFilesMerge/2010_Census.csv").map(_.split(",")).filter(line=> line(0)!="Zip Code").filter(_.size==7)


val rowRdd3 = df3.map { tokens =>     Row(tokens(0),tokens(1),tokens(2),tokens(3),tokens(4),tokens(5),tokens(6))
    }

val fieldsdf3 = Seq(
     StructField("zip", StringType, true),
     StructField("population", StringType, true),
     StructField("median age", StringType, true),
     StructField("totalmales", StringType, true),
     StructField("totalfemales", StringType, true),
     StructField("totalhouseholds", StringType, true),
     StructField("avghouseholdsize", StringType, true)
	 )

val schema3 = StructType(fieldsdf3)
val sqldf3 = sqlContext.createDataFrame(rowRdd3,schema3).persist

val sqljoint11 = sqldf1.join(sqldf2,Seq("zip","year"),"inner")

val sqljoint123 = sqljoint11.join(sqldf3,Seq("zip"),"inner")

sqljoint123.map(x => x.mkString("|")).saveAsTextFile("outputfiles/sqljointBIG")

//sqljoint123.write.format("com.databricks.spark.csv").option("header", "true").save("file.csv")

System.exit(0)
