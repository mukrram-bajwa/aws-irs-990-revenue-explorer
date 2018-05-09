package IRS990Filings.revenue_calculator

import scala.xml.Elem

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.LongAccumulator
import org.joda.time.DateTime
import IRS990Filings.revenue_calculator.util.{Constants, FormParser}

/**
 * @author Mukrram Ur Rahman
 */
object App {

	def main(args : Array[String]) {

		val sparkSession: SparkSession = SparkSession.builder
				.appName(Constants.APP_NAME).getOrCreate
		val sc: SparkContext = sparkSession.sparkContext
    val sqlContext: SQLContext = new SQLContext(sc)

		val dataFrame: DataFrame = sqlContext.read.option("header", "true")   // Use first line of all files as header
		    .option("inferSchema", "true")                                    // Automatically infer data types
			  .csv(Constants.getS3IndexFileURI: _*)                             // Reading all index files

    val totalFiles: LongAccumulator = sc.longAccumulator("totalFiles")
    val successfullyProcessedFiles: LongAccumulator = sc.longAccumulator("successfullyProcessedFiles")
			  
		implicit val rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]
		
		val stats: RDD[Row] = dataFrame.flatMap{fileInfo =>                   // Converting XML files to RDD[Row[year, stateCode, revenue]]
		    var statRows: List[Row] = List.empty
		    val formType: String = fileInfo.getAs[String]("RETURN_TYPE")
		    val objectID: Long = fileInfo.getAs[Long]("OBJECT_ID")
				try {
				    val fileContent: Elem = Helper.getFileContent(Constants.getS3XMLFileURI(objectID))
				    if(fileContent != null) {
				        totalFiles.add(1)
				        val schemaVersion: String = FormParser.getSchemaVersion(fileContent)
				        val stateCode: String = FormParser.getStateCode(fileContent, schemaVersion)
				        val dates: (DateTime, DateTime) = FormParser.getDate(fileContent, schemaVersion)
				        val totalRevenue: Double = FormParser.getRevenue(fileContent, schemaVersion, formType)
				        val revenueParts: List[(Int, Double)] = Helper.divideRevenue(dates._1, dates._2, totalRevenue)
				        for(revenuePart <- revenueParts)
				          statRows = statRows :+ Row.fromSeq(Seq(revenuePart._1, stateCode,revenuePart._2))
				        successfullyProcessedFiles.add(1)
				    }
				} catch {
				    case (ex: Exception) => println(ex.getMessage)
				}
				statRows
		}.rdd

		var schema: StructType = StructType(Seq(StructField("year", IntegerType, true),
      StructField("stateCode", StringType, true),
      StructField("revenue", DoubleType, true)))
    
    val statsDataFrame: DataFrame = sqlContext.createDataFrame(stats, schema)        // Converted RDD to DataFrame to use columns by names rather by index, which is error-prone.
		
		val aggeregatedStateDF: DataFrame = statsDataFrame.groupBy("year", "stateCode")  // Calculating total revenue by State for each year. 
		    .sum("revenue").persist
		
		val aggeregatedDF: DataFrame = aggeregatedStateDF.groupBy("year").sum("sum(revenue)").drop("stateCode") // Calculating total revenue for each year. 

		/*
		 * Collecting the aggregated results on the master, It is safe because only few hundred
		 * records left after aggregation by State and year.
		*/
		
		val stateRevenueMap: Map[(Int, String), Double] = aggeregatedStateDF.collect
		    .map(row => (row.getAs[Int]("year"), row.getAs[String]("stateCode")) -> row.getAs[Double]("sum(revenue)")).toMap

		//Calculating yearly revenue growths   
		val annualRevenueGrowths: Map[(Int, String), Double] = stateRevenueMap.keys.flatMap{ key =>
		  if(stateRevenueMap.keys.toSeq.contains((key._1 - 1, key._2))) {
		    val currentYearRevenue: Double = stateRevenueMap(key)
		    val previousYearRevenue: Double = stateRevenueMap((key._1 - 1, key._2))
		    val annualRevenueGrowth: Double = ((currentYearRevenue - previousYearRevenue) / previousYearRevenue ) * 100
		    Seq(key -> annualRevenueGrowth)
		  } else
		      Seq()
		}.toMap
		
		var totalAnnualRevenueGrowthsByState: Map[String, Double] = Map.empty
		var annualRevenueGrowthsNationally: Map[Int, Double] = Map.empty
		
		/*
		 * Calculating average yearly revenue growths by state 
		 * and also nationally.
		*/
		
		annualRevenueGrowths.foreach{ tuple => 
		  totalAnnualRevenueGrowthsByState = {
  		  if(totalAnnualRevenueGrowthsByState.contains(tuple._1._2))
  		    totalAnnualRevenueGrowthsByState + (tuple._1._2 -> (totalAnnualRevenueGrowthsByState(tuple._1._2) + tuple._2))
  		  else
  		    totalAnnualRevenueGrowthsByState + (tuple._1._2 -> tuple._2)
		  }
		  annualRevenueGrowthsNationally = {
		    if(annualRevenueGrowthsNationally.contains(tuple._1._1))
		      annualRevenueGrowthsNationally + (tuple._1._1 -> (annualRevenueGrowthsNationally(tuple._1._1) + tuple._2))
		    else
		      annualRevenueGrowthsNationally + (tuple._1._1 -> tuple._2)
		  }
		}
		
		var avgAnnualRevenueGrowthsByState: Map[String, Double] = totalAnnualRevenueGrowthsByState.map(tuple => (tuple._1, tuple._2 / Constants.TOTAL_YEARS))
		var avgAnnualRevenueGrowthsByNationally: Double = annualRevenueGrowthsNationally.values.sum / Constants.TOTAL_YEARS
		
		
    println(totalFiles.value)
    println(successfullyProcessedFiles.value)
    
    /*
     * Persisting annual revenue Nationally & By State to HDFS.
    */
    aggeregatedStateDF.coalesce(1).write.csv(args(0).concat("/annualRevenueByState.csv"))
    aggeregatedDF.coalesce(1).write.csv(args(0).concat("/annualRevenueNationally.csv"))
    
    // Persisting some intermediate as well as final results
    
    Helper.saveMultiKeyCollection(args(0).concat("/annualRevenueGrowthByState.csv"), annualRevenueGrowths)
    Helper.saveSingleKey(args(0).concat("/annualRevenueGrowthNationally.csv"), annualRevenueGrowthsNationally)
    
    Helper.saveSingleKey(args(0).concat("/avgAnnualRevenueGrowthByState.csv"), avgAnnualRevenueGrowthsByState)
    Helper.saveSingleResult(args(0).concat("/avgAnnualRevenueGrowthNationally.csv"), avgAnnualRevenueGrowthsByNationally)
	}
}
