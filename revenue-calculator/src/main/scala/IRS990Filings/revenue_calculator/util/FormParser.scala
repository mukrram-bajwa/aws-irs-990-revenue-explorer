package IRS990Filings.revenue_calculator.util

import scala.xml.{Elem, NodeSeq}
import org.joda.time.DateTime

/*
 * The 990 database is built from XML files supplied from the IRS. These filings are based on 
 * schemas supplied in .xsd format, which is a special form of XML used for describing other XML 
 * documents. Some of these schemas from TY 2013 through TY 2016 are available on the IRS website 
 * while others not. So, some of this parser's knowledge is based on the schema extractor module 
 * developed before writing to this part to better understand the data schema.
*/
object FormParser {
  
    /*
     * Gets the schema Version of the XML document
		*/  
    def getSchemaVersion(fileContent: Elem): String = fileContent.attributes.get("returnVersion").get.head.toString	    
    
		def getStateCode(fileContent: Elem, schemaVersion: String): String = {
		    val address: NodeSeq = fileContent \ "ReturnHeader" \ "Filer" \ "USAddress"
		    schemaVersion match {
		        case "2016v3.0" | "2015v3.0" | "2014v6.0" | "2014v5.0" | "2016v3.1" | "2015v2.1" | "2015v2.0" =>  (address \ "StateAbbreviationCd").text
		        case _ => (address \ "State").text
		    }
		}
		
		def getDate(fileContent: Elem, schemaVersion: String): (DateTime, DateTime) = {
		  val header: NodeSeq = fileContent \ "ReturnHeader"
		    schemaVersion match {
		        case "2016v3.0" | "2015v3.0" | "2014v6.0" | "2014v5.0" | "2016v3.1" | "2013v4.0" | "2015v2.1" | "2013v3.1" | "2015v2.0" | "2013v3.0" => 
		          (DateTime.parse((header \ "TaxPeriodBeginDt").text), DateTime.parse((header \ "TaxPeriodEndDt").text))
		        case _ => (DateTime.parse((header \ "TaxPeriodBeginDate").text), DateTime.parse((header \ "TaxPeriodEndDate").text))
		    }
		}
		
		def getRevenue(fileContent: Elem, schemaVersion: String, formType: String): Double = {  
		  try {
			  val data: NodeSeq = fileContent \ "ReturnData"
			  schemaVersion match {
			      case "2015v2.1" | "2015v2.0" | "2014v6.0" | "2013v4.0" | "2014v5.0" | "2013v3.1" | "2013v3.0" | "2016v3.0" | "2015v3.0" | "2016v3.1" => {
			          formType match {
			            case "990" | "990O"      => (data \ "IRS990" \ "CYTotalRevenueAmt").text.toDouble
			            case "990EO" | "990EZ"   => (data \ "IRS990EZ" \ "TotalRevenueAmt").text.toDouble
			            case _                   => 0
			          }
			      }
			      case _ => {
			          formType match {
			            case "990" | "990O"      => (data \ "IRS990" \ "TotalRevenueCurrentYear").text.toDouble
			            case "990EO" | "990EZ"   => (data \ "IRS990EZ" \ "TotalRevenue").text.toDouble
			            case _                   => 0
			          }
			      }
			    }
		  }  catch {
		        case (ex: Exception) => 0
		  }
		}
}