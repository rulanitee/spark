package tendai.spark.test
/**
  *
  * Created by dev on 7/12/18.
  */
class SparkTestNewData extends SparkTestBase{

  "testing new students " ignore {

    //get the student data as df
    val studentDF = SparkTestUtil.getStudentDF(sparkSession)

    //get the new records
    val newStudentsDF = SparkTestUtil.getNewStudentDF(sparkSession)

    //get new data
    val newStudents = newStudentsDF
      .join(studentDF, newStudentsDF("studentId") === studentDF("studentId"), "LEFT")
      .filter(studentDF("studentId").isNull)
      .select(
        newStudentsDF("studentId"),
        newStudentsDF("name"),
        newStudentsDF("surname"),
        newStudentsDF("dateOfBirth"),
        newStudentsDF("address"))

    newStudents.show

    assert(20 == 20, s"The age should be 20 ...")

  }

}
