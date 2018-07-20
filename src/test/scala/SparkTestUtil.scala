package tendai.spark.test

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

/**
  * Created by dev on 7/12/18.
  */
object SparkTestUtil {

  def getStudentDF(sparkSession: SparkSession): DataFrame = {

    //given student data with a date of birth field
    val studentRDD = sparkSession
      .sparkContext
      .parallelize(SparkTestDataStudent.students)

    //get the student data as df
    sparkSession.createDataFrame(studentRDD)
      .select(col("studentId"), col("name"), col("surname"), col("dateOfBirth"), col("address"))

  }

  def getNewStudentDF(sparkSession: SparkSession): DataFrame = {

    //given student data with a date of birth field
    val studentRDD = sparkSession
      .sparkContext
      .parallelize(SparkTestDataStudent.studentsChanged)

    //get the student data as df
    sparkSession.createDataFrame(studentRDD)
      .select(col("studentId"), col("name"), col("surname"), col("dateOfBirth"), col("address"))

  }

}
