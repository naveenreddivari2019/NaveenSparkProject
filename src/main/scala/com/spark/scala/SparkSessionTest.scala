package com.spark.scala

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import java.sql.Date
import java.text.SimpleDateFormat

object SparkSessionTest {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Test").master("local[*]")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/naveen_db").enableHiveSupport().getOrCreate()
    var schema1 = new StructType()
      .add("id", IntegerType, true)
      .add("DOB", DateType, true)
      .add("Gender", StringType, true)
      .add("marital_status", StringType, true)
      .add("smoking_status", StringType, true)
      .add("city", StringType, true)

    case class Patients(id: BigInt, DateOfBirth: String, Gender: String, marital_status: String, smoking_status: String, city: String)

    case class Employee(id: Int, name: String)

    val rdd = spark.read.schema(schema1).csv("""C:\Local Disc(D)\Hadoop\TCS\Integrated usecase\Integrated usecase\Patients.csv""")
    //val rdd = spark.read.parquet("""C:\Local Disc(D)\Hadoop\TCS\Integrated usecase\parquet1""")
    // val rdd = spark.read.format("parquet").load("""C:\Local Disc(D)\Hadoop\TCS\Integrated usecase\parquetPartition""")

    //val encoder = org.apache.spark.sql.catalyst.encoders.ExpressionEncoder[Patients]
    //val encoder = org.apache.spark.sql.Encoders.product[Patients]
    //val df = spark.read.csv("""C:\Local Disc(D)\Hadoop\TCS\Integrated usecase\Integrated usecase\Patients.csv""") //.as(encoder)
    //val ds1 = df.as[Patients]

    //rdd.write.mode(SaveMode.Overwrite).partitionBy("city").parquet("""C:\Local Disc(D)\Hadoop\TCS\Integrated usecase\parquetPartition""")

    /*    val v=Array("A","B","C","B")
    val rdd=spark.sparkContext.parallelize(v, 2)
    val r=rdd.map(e=>(e,1)).reduceByKey((x,y)=>x+y)
    r.persist(StorageLevel.MEMORY_ONLY_SER)
    r.sortByKey(true).foreach(e=>print(e))*/

    import spark.implicits._

    val patient_DF = spark.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("dateFormat", "yyyy-MM-dd").schema(schema1).load("""C:\Local Disc(D)\Hadoop\TCS\Integrated usecase\Integrated usecase\patient_DF\part-00000.csv""")
    println(patient_DF.schema)
    patient_DF.show()
    val epoch = System.currentTimeMillis
    val curDate = new Date(epoch)
    val dtFormat = new SimpleDateFormat("yyyy-MM-dd")
    val usecase4_result = patient_DF.alias("p").withColumn("age", when(col("DOB")!== "", {
      col("id")+100

    }).otherwise(null))
    println("usecase4_result ==> "+usecase4_result.printSchema())

    //usecase4_result.coalesce(1).write.mode(SaveMode.Overwrite).csv("/output/UseCase_Result")
    //usecase4_result.coalesce(1).write.format("csv").mode(SaveMode.Overwrite).saveAsTable("naveen_db.Patients_Data")

    /*    val rdd1 = spark.sparkContext.makeRDD(Array(("A", "1"), ("B", "2"), ("C", "3")), 2)
        val rdd2 = spark.sparkContext.makeRDD(Array(("A", "a"),("A", "55"), ("C", "c"), ("D", "d")), 2)

        val join_r=rdd1.join(rdd2)
        join_r.collect().foreach(println)

        val cog_r=rdd1.cogroup(rdd2)
        cog_r.collect().foreach(println)*/

    //stop spark session
    spark.stop()
    //Fengcheng
  }

}