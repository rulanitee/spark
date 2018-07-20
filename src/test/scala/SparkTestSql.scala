package tendai.spark.test
import org.apache.spark.sql.functions._

/**
  * Created by dev on 7/3/18.
  */
class SparkTestSql extends SparkTestBase{

  "testing student age calculation" in {

    //given student data with a date of birth field
    val studentRDD = sparkSession
      .sparkContext
      .parallelize(SparkTestDataStudent.students)

    //calculate the student age
    val studentDF = sparkSession.createDataFrame(studentRDD)
      .select(
        col("studentId"),
        col("name"),
        col("surname"),
        col("dateOfBirth"),
        col("address"))
      .withColumn("age", year(current_date) - year(to_date(col("dateOfBirth"))))

    //when student if filtered as Frank
    val student_frank = studentDF.select(col("age")).filter(col("name").equalTo("Frank")).collect()

    assert(student_frank(0).get(0) == 20 , s"The age should be 20, but it was calculated as ${student_frank(0).get(0)}")

  }

}
