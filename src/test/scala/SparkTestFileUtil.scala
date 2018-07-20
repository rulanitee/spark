package tendai.spark.test
import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source

/**
  * Created by dev on 7/12/18.
  */
object SparkTestFileUtil {

  def writeToFile(fileName: String, contents : String) = {
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(contents)
    bw.close()
  }

  def readFromFile(fileName: String): Unit ={
    Source.fromFile(fileName).mkString
  }


  def getTestResourceAsFile(resourceName: String): File = {

    val path = SparkTestFileUtil.getClass.getResource(resourceName)

    val file = new File(path.getPath)

    file
  }

}
