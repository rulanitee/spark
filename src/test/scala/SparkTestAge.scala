import org.joda.time._

/**
  * Created by dev on 7/4/18.
  */
class SparkTestAge extends SparkTestBase {

  "age" should {
    "be calculated" in {
      //given a date of birth
      val dateOfBirth: DateTime = new DateTime(1998, 5, 24, 0, 0, 0, 0)
      //should be able to get age based on the current date
      val today: DateTime = DateTime.now
      //age
      val age = Years.yearsBetween(dateOfBirth, today).getYears

      assert(age == 20, s"The age should be 20, but it was calculated as $age")

    }
  }

}
