import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, date_sub, isnull, lit, monotonically_increasing_id, to_date}

object GenericType2 {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = SparkSession.builder().appName("Generic Type 2").master("local").getOrCreate()

    val properties = new Properties()
    properties.put("user","postgres")
    properties.put("password","root")
    properties.setProperty("driver","org.postgresql.Driver")
    Class.forName("org.postgresql.Driver")

    val jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    val table = "customer"

    val custFull = sc.read.jdbc(jdbc_url,table, properties)

    //custFull.show()
    //custFull.printSchema()

    val custDelta = sc.read.option("infer schema", "true")
      .csv("SparkDataset/customer_delta")
      .toDF("id", "name", "add")

    //custDelta.show()

    val maxKey = sc.read.csv("SparkDataset/max_key.txt").toDF("max_key")
    maxKey.show()


    //fullFile.alias("f")
    //  .join(deltaFile.alias("d"), col("f.cust_id") === col("d.cust_id"))

    val prevDate = date_sub(to_date(current_date(),"dd-MM-yyyy"), 1)
    //println(prevDate)


    val joinDF1 = custFull.as("f")
      .join(custDelta.as("d"), col("f.id") === col("d.id"))
        .select(col("f.id"), col("f.name"),col("f.add") ,col("f.st_dt"))

    joinDF1.show()

    val joinDF2 = custFull.as("f")
      .join(custDelta.as("d"), col("f.id") === col("d.id"))
      .select(col("d.id"), col("d.name"), col("d.add"))

    val joinDF3 = custFull.as("f").join(custDelta.as("d"), col("f.id") === col("d.id"), "left")
      .filter(isnull(col("d.id")))
        .select(col("f.id"), col("f.name"), col("f.add"),col("f.st_dt"), col("f.end_dt"))



    val closeOldDF = joinDF1
      .withColumn("end_dt", prevDate)

    val openNewDF = joinDF2
        .withColumn("st_dt", to_date(current_date(), "dd-MM-yyyy"))
        .withColumn("end_dt", to_date(lit("31-12-4099"),"dd-MM-yyyy"))

    closeOldDF.show()

    val mergedDF = closeOldDF.union(openNewDF).sort("id")


    val finalDF = mergedDF.union(joinDF3).sort("id")
    //finalDF.show()

    //mergedDF.write.mode("Overwrite")jdbc(jdbc_url, table, properties)
    //mergedDF.write.mode("Append")jdbc(jdbc_url, table, properties)

    //newDF.withColumn("sk", monotonically_increasing_id() + 9766578).show()

    custFull.as("f")
      .join(custDelta.as("d"), col("f.id") === col("d.id"))
      .select(col("f.id"), col("f.name"),col("f.add") ,col("f.st_dt")).where("(d.add != f.add) or (d.name != f.name)").show()


  }

}
