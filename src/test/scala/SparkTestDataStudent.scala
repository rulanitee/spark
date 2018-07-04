/**
  * Created by dev on 7/4/18.
  */
case class Student(studentId: String, name: String, surname: String, dateOfBirth: String, address: String)

object SparkTestDataStudent {

  val students: Seq[Student] = Seq(
    new Student("001", "Frank", "Loo", "1998-05-24", "Water loo"),
    new Student("002", "Mike", "Koo", "1998-11-30", "Sandton"),
    new Student("003", "Charles", "Ling", "1998-06-12", "Randburg"),
    new Student("004", "John", "Doe", "1999-01-01", "Pretoria"))

}
