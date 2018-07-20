package tendai.spark.test

/**
  * Created by dev on 7/3/18.
  */
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class SparkTestBase extends WordSpec with Matchers with BeforeAndAfterAll {

  var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {

    super.beforeAll()

    val sparkConf = new SparkConf()
      .setAppName("unit test")
        .set("spark.ui.port", "4040")
      .set("spark.master", "local[2]")

    sparkSession = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

  }

  override def afterAll(): Unit = {

    //sparkSession.close()

  }

}
