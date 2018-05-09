package IRS990Filings.revenue_calculator

import java.io.{File, PrintWriter}
import scala.collection.Map
import scala.xml.{Elem, XML}

import org.joda.time.DateTime
import org.joda.time.Days

object Helper {
  
  /*
   * Reading XML file over the HTTP.
  */
   def getFileContent(httpURI: String): Elem = {
     try {
      val response: Elem = XML.load(httpURI)
      if(!response.label.equals("Error"))
          response
      else
          null
     } catch {
       case (ex : Exception) => null
     }
	}
   
   /*
    * American fiscal year is from October to September and the data is stored based on the
    * filing date. It's not clear that revenue belongs to which year. Here total revenue is
    * divided based on tax duration.
   */
  def divideRevenue(startDate: DateTime, endDate: DateTime, revenue: Double): List[(Int, Double)] =  {
      var revenueParts: List[(Int, Double)] = List()
      val revenuePerDay: Double = revenue / (Days.daysBetween(startDate, endDate).getDays)
      var date = startDate 
      while (date.getYear <= endDate.getYear) {
          revenueParts = {
              if(date.getYear == startDate.getYear && date.getYear == endDate.getYear)
                  revenueParts :+ (date.getYear, revenue)          
              else if(date.getYear == startDate.getYear)
                  revenueParts :+ (date.getYear, (date.dayOfYear.getMaximumValue - date.getDayOfYear) * revenuePerDay)
              else if (date.getYear == endDate.getYear)
                  revenueParts :+ (date.getYear, endDate.getDayOfYear * revenuePerDay)          
              else 
                  revenueParts :+ (date.getYear, date.dayOfYear.getMaximumValue * revenuePerDay)
          }
          date = date.plusYears(1)
      }
      revenueParts
  }
  
  /*
   * Storing Intermediate and final results.
  */
  def saveMultiKeyCollection(filePath: String, collection: Map[(Int, String), Double]) = writeToFile(filePath, collection.map(tuple => s"${tuple._1._1},${tuple._1._2},${tuple._2}").toArray)
  
  def saveSingleKey(filePath: String, collection: Map[_, _]) = writeToFile(filePath, collection.map(tuple => s"${tuple._1},${tuple._2}").toArray)
  
  def saveSingleResult(filePath: String, result: Any) = {
     val writer: PrintWriter = new PrintWriter(new File(filePath))
     println(result)
     writer.close
  }
  
  def writeToFile(filePath: String, collection : Array[String]) {
    val writer: PrintWriter = new PrintWriter(new File(filePath))
    for(item <- collection)
      println(item)
    writer.close
  }
}