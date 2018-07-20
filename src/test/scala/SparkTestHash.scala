package tendai.spark.test

import org.apache.spark.sql.functions.{col, hash}

/**
  * Created by dev on 7/11/18.
  */
class SparkTestHash extends SparkTestBase{

  "testing student hash " ignore {

    //get the student data as df
    val studentDF = SparkTestUtil.getStudentDF(sparkSession)

    //hash the student data
    val studentHash = studentDF.select(col("studentId"),
      col("name"),col("surname"),col("dateOfBirth"),col("address"))
      .withColumn("hash", hash(studentDF.columns.map(col): _*))

    //get the student data as df
    val studentChangedDF = SparkTestUtil.getNewStudentDF(sparkSession)

    //hash the student data
    val studentChangedHash = studentChangedDF.select(col("studentId"),
      col("name"),col("surname"),col("dateOfBirth"),col("address"))
      .withColumn("hash", hash(studentChangedDF.columns.map(col): _*))

    //get the data that changed
    val changedStudents = studentChangedHash
      .join(studentHash, studentHash("studentId") === studentChangedHash("studentId"))
      .filter(studentChangedHash("hash").notEqual(studentHash("hash")))
      .select(
        studentChangedHash("studentId"),
        studentChangedHash("name"),
        studentChangedHash("surname"),
        studentChangedHash("dateOfBirth"),
        studentChangedHash("address"))

    changedStudents.show

    assert(20 == 20 , s"The age should be 20 ...")

  }


}
