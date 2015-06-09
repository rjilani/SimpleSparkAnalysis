/**
 * Created by rjilani on 6/3/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleSparkTest {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("C:\\Projects\\scala_project\\Data\\ml-100k\\u.user") //Location of the data file
      .map(line => line.split(","))
      .map(userRecord => (userRecord(0),
      userRecord(1), userRecord(2),userRecord(3),userRecord(4)))

   // data.collect().foreach(println)

    println(s"Number of Records in Movie file ${data.count()} \n")

    //Number of unique professions in the data file
    val uniqueProfessions = data.map {case (id, age, gender, profession,zipcode) => profession}.distinct().count()

    println(s"Number of unique professionals $uniqueProfessions \n")

    //Group users by profession and sort them by descending order
    val usersByProfession = data
      .map{ case (id, age, gender, profession,zipcode) => (profession, 1) }
      .reduceByKey(_ + _)
      .sortBy(-_._2)

    println("Users group by professions")

    usersByProfession.collect().foreach(println)

    println("\n")
    //Group users by zip code and sort them by descending order
    val usersByZipCode = data
      .map{ case (id, age, gender, profession,zipcode) => (zipcode, 1) }
      .reduceByKey(_ + _)
      .sortBy(-_._2)

    println(s"Users group by Zip Codes")

    usersByZipCode.collect().foreach(println)

    println("\n")

    //Group users by Gender and sort them by descending order
    val usersByGender = data
      .map{ case (id, age, gender, profession,zipcode) => (gender, 1) }
      .reduceByKey(_ + _)
      .sortBy(-_._2)

    println(s"Users group by Gender")

    usersByGender.collect().foreach(println)

    println("\n")


    sc.stop()
  }
}
