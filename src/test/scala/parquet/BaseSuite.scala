package parquet

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}


class BaseSuite extends FunSuite with BeforeAndAfterAll{

  private var sc: SparkContext = _
  private var spark: SparkSession = _
  private var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder().appName(this.getClass.getName).master("local").getOrCreate()
    sc = spark.sparkContext
    sqlContext = spark.sqlContext


  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }



  test("Test row to file"){
    val rdd: RDD[Row] = sc.parallelize(
      Seq(
        Row("one", 1, 2.2),
        Row("two", 3, 4.4)
      )
    )

    val schema = StructType(Seq(
      StructField(name = "name", dataType = StringType, nullable = false),
      StructField(name = "val1", dataType = IntegerType, nullable = false),
      StructField(name = "val2", dataType = DoubleType, nullable = false)
    ))

    val df = spark.createDataFrame(rdd, schema)

    df.show()

    df.write.parquet("data")

//    val newDF = sqlContext.read.parquet("data")
//
//    newDF.show()


  }

}
