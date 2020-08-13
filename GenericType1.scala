import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,isnull,coalesce}

import org.apache.log4j.{Level, Logger}

object GenericType1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.INFO)

    val properties = new Properties()
    properties.put("user","postgres")
    properties.put("password","root")
    properties.setProperty("driver","org.postgresql.Driver")
    Class.forName("org.postgresql.Driver")

    val jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    val table = "cust_info"

    val sc = SparkSession.builder().appName("Join Datasets Example").master("local").getOrCreate()

    val deltaFile = sc.read.option("header", "false")
      .option("infer schema", "true")
      .csv("SparkDataset/cus_info_delta")
      .toDF("cust_id", "cust_name", "cust_add", "cust_prod")

    deltaFile.show()
    //deltaFile.printSchema()

    val fullFile = sc.read.jdbc(jdbc_url,table, properties).toDF()
    //fullFile.printSchema()
    fullFile.show()

    deltaFile.createOrReplaceTempView("delta")
    fullFile.createOrReplaceTempView("full")


    val joinDF1 = fullFile.alias("f")
      .join(deltaFile.alias("d"), col("f.cust_id") === col("d.cust_id"))
      .select(col("d.cust_id"), col("d.cust_name"), col("d.cust_add"), col("d.cust_prod"))

    //joinDF1.show()

    val joinDF2 =
      fullFile.alias("f")
      .join(deltaFile.alias("d"), col("f.cust_id") === col("d.cust_id"),"left")
      .filter(isnull(col("d.cust_id")))
      .select(col("f.cust_id"), col("f.cust_name"), col("f.cust_add"), col("f.cust_prod"))

    //joinDF2.show()

    val mergedDF = joinDF1.union(joinDF2)

    mergedDF.show()

    //mergedDF.coalesce(1)
    //  .write
    //    .option("header","true")
     // .text("C:\\Users\\chandra_prakash\\Documents\\Scala\\SparkOut\\cus_info_full.txt")
    //    .csv("C:\\Users\\chandra_prakash\\Documents\\Scala\\SparkOut\\cus_info_full.csv")


  }

}
