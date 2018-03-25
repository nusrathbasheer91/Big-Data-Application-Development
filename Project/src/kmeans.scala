import sqlContext.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.functions._

val data = sc.textFile("CRIMEDETECT/InputFilesKmeans")

case class CC1(ZIP: Double, YEAR: Double, DATE: String, CRIME_CAT_NUM: Double, CRIME_CAT_DESC: String, STAT_CODE: Double, VICTIM_COUNT: Double, STAT_ID: String,  CRIME_ID: Double, RETURNS: Double, CA_AGI: Double, TOTAL_TAX: Double, POPULATION: Double, MEDIAN_AGE: Double, TOTAL_MALES: Double, TOTAL_FEMALES: Double, TOTAL_HOUSEHOLDS: Double, AVG_HOUSEHOLD_SIZE: Double)

val allSplit = data.map(line => line.split("""\|"""))

val allData = allSplit.map( p => CC1( p(0).trim.toDouble, p(1).trim.toDouble, p(2).trim.toString, p(3).trim.toDouble, p(4).trim.toString, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toString, p(8).trim.toDouble, p(9).trim.toDouble, p(10).trim.toDouble, p(11).trim.toDouble, p(12).trim.toDouble, p(13).trim.toDouble, p(14).trim.toDouble, p(15).trim.toDouble, p(16).trim.toDouble, p(17).trim.toDouble))

val allDF = allData.toDF()

val rowsRDD = allDF.rdd.map(r => (r.getDouble(0), r.getDouble(1), r.getString(2), r.getDouble(3), r.getString(4), r.getDouble(5), r.getDouble(6), r.getString(7), r.getDouble(8), r.getDouble(9), r.getDouble(10), r.getDouble(11), r.getDouble(12), r.getDouble(13), r.getDouble(14), r.getDouble(15), r.getDouble(16), r.getDouble(17)))
rowsRDD.cache()

val vectors = allDF.rdd.map(r => Vectors.dense( r.getDouble(0), r.getDouble(3), r.getDouble(5), r.getDouble(6) ))
vectors.cache()

val kMeansModel = KMeans.train(vectors, 5, 20)
val predictions = rowsRDD.map{r => (r._9, kMeansModel.predict(Vectors.dense(r._1, r._4, r._6, r._7) ))}
val predDF = predictions.toDF("CRIME_ID", "CLUSTER")

val t = allDF.join(predDF, "CRIME_ID")
t.registerTempTable("resData")
t.write.format("com.databricks.spark.csv").save("OutputKmeansclusterData")

val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)

val t_filt = sqlContext.sql("""select ZIP, YEAR, CRIME_CAT_NUM, CRIME_CAT_DESC, STAT_CODE, VICTIM_COUNT, STAT_ID, RETURNS, CA_AGI, TOTAL_TAX, POPULATION, MEDIAN_AGE, TOTAL_MALES, TOTAL_FEMALES, TOTAL_HOUSEHOLDS, AVG_HOUSEHOLD_SIZE, CLUSTER from resData""")
t_filt.write.format("com.databricks.spark.csv").save("KmeansfiltClusterData")
t_filt.registerTempTable("finalResData")

val cluster_wise_avg_income = sqlContext.sql("""select count(ZIP), avg(CA_AGI), CLUSTER from finalResData group by CLUSTER""")
cluster_wise_avg_income.write.format("com.databricks.spark.csv").save("KmeansclusterAvgData")
cluster_wise_avg_income.registerTempTable("KmeansclusterAvgDataTable")

val crime_cat_desc = sqlContext.sql("""select CRIME_CAT_DESC, count(ZIP) as cat_count from finalResData group by CRIME_CAT_DESC order by cat_count desc""")
crime_cat_desc.write.format("com.databricks.spark.csv").save("KmeanscrimecatData")
crime_cat_desc.registerTempTable("KmeanscrimecatDataTable")


val by_year = sqlContext.sql(""" select YEAR, count(ZIP) as yearwise_count from finalResData group by YEAR order by yearwise_count desc""")
by_year.write.format("com.databricks.spark.csv").save("KmeansyearwisecrimeData")
by_year.registerTempTable("KmeansyearwisecrimeDataTable")

val by_zip = sqlContext.sql("""select ZIP, count(ZIP) as zipwise_count from finalResData group by ZIP order by zipwise_count desc """)
by_zip.write.format("com.databricks.spark.csv").save("KmeanszipwisecrimeData")
by_zip.registerTempTable("KmeanszipwisecrimeDataTable")

System.exit(0)
