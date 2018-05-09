package IRS990Filings.revenue_calculator.util

object Constants {
  
  val APP_NAME: String = "IRS990Filings_Revenue_Calculator"
  val START_YEAR: Int = 2011
  val END_YEAR: Int = 2018
  val TOTAL_YEARS: Int = (END_YEAR -START_YEAR) + 1
  val S3_BUCKET_NAME: String = "irs-form-990"
  val AWS_REGION: String = "us-east-1"
  
  def getS3IndexFileURI: Array[String] = {
    val s3IndexURIs: Array[String] = new Array[String]((END_YEAR-START_YEAR) + 1)
    for(year <- START_YEAR to END_YEAR)
      s3IndexURIs(year - START_YEAR) = s"s3n://${S3_BUCKET_NAME}/index_${year}.csv"
    s3IndexURIs
  }
  def getS3XMLFileURI(objectID: Long) = s"https://s3.amazonaws.com/${S3_BUCKET_NAME}/${objectID}_public.xml"
}