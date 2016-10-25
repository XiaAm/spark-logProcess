package com.spark.demo1

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object SparkLogProcess {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("log processing"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert a RDD to a DataFrame
    import sqlContext.implicits._
    
    // 0. parse the input
    // get the path of the folder in which stores the log .gz file
    val folder_log = args(0)
    // get the path of the folder in which stores the output
    val folder_output = args(1)
    // get the path of decoded blob file
    val path_decoded_blob = args(2)
    
    // 1st Part, load data
    // read all the log files into RDD
    val path_log = folder_log + "*.gz"
    val FElogAllKinds_rdd = sc.textFile(path_log)
    val FElog_necessary_rdd = FElogAllKinds_rdd.filter(x => (x.contains("<name>OTA_HotelAvailRS</name>")&&x.contains("id=\"OTA2011B_APD\"")&&x.contains("EchoToken=\"MultiSingle\""))||(x.contains("<name>OTA_HotelAvailRQ</name>")&&x.contains("id=\"OTA2011B_APD\"")&&x.contains("EchoToken=\"MultiSingle\""))||(x.contains("HBTLCR")) )
    //availability request
    val FElog_isAvailRQ_rdd = FElog_necessary_rdd.filter(x=>x.contains("<name>OTA_HotelAvailRQ</name>"))
    //availability response
    val FElog_isAvailRS_rdd = FElog_necessary_rdd.filter(x=>x.contains("<name>OTA_HotelAvailRS</name>"))
    //booking response 
    val FElog_isHBTLCR_rdd = FElog_necessary_rdd.filter(x=>x.contains("HBTLCR"))
    
    
    // 2nd Part, use the DCX id of availability request to distinguish whether a response is single or multi
    val FElog_singleAvailRQ_rdd = FElog_isAvailRQ_rdd.filter(x=>countSubString(x, "<HotelRef")==1).filter(x=>countSubString(x, "HotelCode=")==1)
    val FElog_multiAvailRQ_rdd = FElog_isAvailRQ_rdd.subtract(FElog_singleAvailRQ_rdd)    
    // get the DCX id of singleAvailRQ, Array((9HVHZRP#TFEB12JI#3WT7#ZT91,278-1),...)
    val DCX_singleAvailRQ = FElog_singleAvailRQ_rdd.map(x => (getDCX(x)._2, getSeqNb(x))).collect()
    // use these DCX id to separate single availability RS from multi
    val FElog_singleAvailRS_rdd = FElog_isAvailRS_rdd.filter(x => DCX_singleAvailRQ contains Tuple2(getDCX(x)._2, getSeqNb(x)) )
    val FElog_multiAvailRS_rdd = FElog_isAvailRS_rdd.subtract(FElog_singleAvailRS_rdd)
    
    
    // 3rd Part, parse the log and store the result in different files
    FElog_singleAvailRQ_rdd.map(x => parseAvailRQ(x)).map(x=>x+",isSingleRQ").coalesce(1).saveAsTextFile(folder_output+"/singleAvailRQ")
    FElog_multiAvailRQ_rdd.map(x=>parseAvailRQ(x)).map(x=>x+",isNotSingleRQ").coalesce(1).saveAsTextFile(folder_output+"/multiAvailRQ")
    //10% of single avail RS are not complete (I remove only the uncomplete room stay), 2% of multi avail RS do not have complete HotelStays (I remove the whole multi avail RS)
    //some room stay does not have booking code because their availability status is ClosedOut
    FElog_singleAvailRS_rdd.filter(x=>x.contains("<RoomStays")).filter(x=>x.contains("AvailabilityStatus=\"AvailableForSale\"")).flatMap(x => parseSingleAvail(x)).distinct().coalesce(1).saveAsTextFile(folder_output+"/singleAvailRS")
    FElog_multiAvailRS_rdd.filter(x=>x.contains("<HotelStay")).filter(x=>x.contains("</HotelStays>")).filter(x=>x.contains("TimeSpan")).flatMap(x => parseMultiSingle(x)).distinct().coalesce(1).saveAsTextFile(folder_output+"/multiSingleRS")
    
    
    // 4rd Part, combine HBTLCR and Blob, store it in csv format
    val evrBlobJson_rdd = sc.textFile(path_decoded_blob)
    val evrBlobJson = sqlContext.read.json(evrBlobJson_rdd.map(x => x.replace("\"\"", "\"")))
    val df_evrBlobJson_unNamed = evrBlobJson.select("chain_code","image.confirmation_number","property_city","property_code","image.originator.amadeus_office_id","image.distrib_history_status.transaction_type", "image.roomstay.booking_code", "image.roomstay.product.code", "image.roomstay.rate_plan.code")
    val df_evrBlobJson = df_evrBlobJson_unNamed.toDF("chain_code", "confirmation_number", "property_city", "property_code", "office_id", "transaction_type", "booking_code", "product_code", "rate_code")
    val df_FElog_isHBTLCR = FElog_isHBTLCR_rdd.map(x => (getDCX(x)._2, getCCandConfirmationNumber(x)._1, getCCandConfirmationNumber(x)._2, getSAPFields(x)._1, getSAPFields(x)._2, getSAPFields(x)._3, getSignValue_FElog(x), get_generateTime(x))).distinct().map({case Tuple8(val1:String, val2:String, val3:String, val4:String, val5:String, val6:String, val7:String, val8:String) => (val1, val2, val3, val4, val5, val6, val7, val8) }).toDF("DCXid", "chain_code", "confirmation_number", "SAPname", "SAPtype", "SAPproduct", "SIGNvalue", "generateTime")    
    val df_reservation = df_evrBlobJson.join(df_FElog_isHBTLCR, df_evrBlobJson("confirmation_number")===df_FElog_isHBTLCR("confirmation_number")).filter("transaction_type=\"sell   \"").select(df_FElog_isHBTLCR("DCXid"), df_FElog_isHBTLCR("chain_code"), df_evrBlobJson("property_city"), df_evrBlobJson("property_code"), df_evrBlobJson("office_id"), df_evrBlobJson("booking_code"), df_evrBlobJson("product_code"), df_evrBlobJson("rate_code"), df_FElog_isHBTLCR("SAPname"), df_FElog_isHBTLCR("SAPtype"), df_FElog_isHBTLCR("SAPproduct"), df_FElog_isHBTLCR("SIGNvalue"), df_FElog_isHBTLCR("generateTime"))
    // save it
    df_reservation.rdd.map(x=>x.getString(0)+","+x.getString(1)+","+x.getString(2)+","+x.getString(3)+","+x.getString(4)+","+x.getString(5)+","+x.getString(6)+","+x.getString(7)+","+x.getString(8)+","+x.getString(9)+","+x.getString(10)+","+x.getString(11)+","+x.getString(12)).coalesce(1).saveAsTextFile(folder_output+"/df_reserv")
  }
  
  // all the following functions are user defined functions for log parsing.
  
  //////////////////////  1. common functions for parsing singleAvailRQ, multiAvailRQ, availRS, HBTLCR
  def countSubString(str: String, substr: String): Int = {
    return substr.r.findAllMatchIn(str).length
  }
  def getLength(log: String): Int = {
    val pattern1 = "Len=[0-9]*".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        return s.substring(4, s.length).toInt
      }
      case None => {
        return 0
      }
    }
  }
  def hasDCX(log: String): (Int, String) = {
    //3 if it contains complete DCX info, 1 if it contains only the SimpleInfo, 0 if there is no PFX found
    val pattern1 = "\\[PFX:\\s[0-9a-zA-Z#$:\\-\\/]*\\]".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        val nbOfTwopoint = countSubString(s, ":")
        return (nbOfTwopoint, s)
      }
      case None => {
        return (0, "NoDCXFound")
      }
    }
  }
  def getDCX(log: String): (String, String) = {
    //s is the DCXid
    val i = hasDCX(log)._1
    val s = hasDCX(log)._2
    i match {
      case 0 => {
        return ("noSimpleInfo", "noDCXid")
      }
      case 1 => {
        val s_array = s.split(":")
        val simpleInfo = s_array(1).trim
        return (simpleInfo.substring(0, simpleInfo.length() - 1), "noDCXid") //-1 to remove the ] at the end
      }
      case 3 => {
        val s_array = s.split(":")
        val simpleInfo = s_array(1).trim
        val DCXid = s_array(2).trim
        return (simpleInfo, DCXid)
      }
      case default => {
        return ("not0_1_3", "not0_1_3")
      }
    }
  }
  def getSeqNb(log: String): String = {
    //get the sequence number after DCXid
    val t = hasDCX(log)
    val n = t._1
    val strPFX = t._2
    if (n.toInt != 3) {
      return "NoSeqNbFound"
    } else {
      val s_array = strPFX.split(":")
      val res = s_array.last.substring(0, s_array.last.length() - 1)
      return res
    }
  }
  /* these functions are used to parse APD report logs, I don't use APD report log any more.
  def getDifferentSessionExceptMultiSingle(log: String): (String, String, Array[String]) = {
    //http://hdpdoc/doku.php?id=teams:hda:projects:search_engine:reporting
    //basicFields: the basic fields from SingleAvail to 
    //property: property session
    //roomStays: roomStays session
    val array1 = log.split("\\]\\s1\\|") //split log to two part: DCX info and content(already remove "1|" in the head) 
    val log_content = array1(1)

    val s_array = log_content.split("\\|")
    val basicFields = s_array(0)
    val property = s_array(1)
    val roomStays = s_array.slice(2, s_array.length)
    return (basicFields, property, roomStays)
  }
  def parseBasicFields(basicFields: String): (String, String) = {
    //MultiSingle,20160726-144601,0.552017,GSP1S2115,,1A,,1,160730,2,distribution,QQN-QQP-QQR-QQS-RAC,10
    val fieldNames = Array("serviceName", "Transaction date", "Response time", "Office id",
      "ATID", "Channel", "SubChannel", "LOS", "CheckIn date", "Occupancy", "Providers",
      "Requested rate codes", "Number of Propertyies")

    val fieldUsed1 = "serviceName"
    val fieldUsed2 = "Transaction date"

    val array1 = basicFields.split(",")
    return (array1(fieldNames.indexOf(fieldUsed1)), array1(fieldNames.indexOf(fieldUsed2)))
  }
  def getOfficeId(log: String): String = {
    val array1 = log.split("\\]\\s1\\|") //split log to two part: DCX info and content(already remove "1|" in the head) 
    val content = array1(1)
    val contentArray = content.split("\\|") //according to Document, 1st is main fields, 2nd is property session, from 3rd, all is RoomStay session
    val mainFields = contentArray(0)
    val property = contentArray(1)
    val roomStays = contentArray.slice(2, contentArray.length)

    val officeId = mainFields.split(",")(3)

    return officeId
  }
  def getServiceName(log: String): String = {
    val pattern1 = "(SingleAvail|Pricing|MultiAvail|MultiSingle)(\\-sampling|\\-crawling)?".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        return s
      }
      case None => {
        return "noServiceNameFound"
      }
    }
  }
  def getService(log: String): String = {
    return log.substring(170, 200)
  }
  * */
  
  //function for HBTLCR FE log
  //getDCX is the same as the one for APD Report
  def getCCandConfirmationNumber(log: String): (String, String) = {
    //chain_code is 2 or 3 combinations of number and letter, confirmation_number has at least 8 combinations of number and letter
    val pattern1 = "RCI[+][0-9A-Z]{2,3}[:][0-9A-Z]{8,}[:]".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        //"""RCI+HL:3274614036:2"""
        val s_array = s.split(":")
        return (s_array(0).split("\\+")(1), s_array(1))
      }
      case None => {
        return ("NoCCFound", "NoConfirmationNumberFound")
      }
    }
  }
  def get_generateTime(log: String): String = {
    //get generation time of the log and transform its format to yyyy-MM-dd HH:mm:ss
    val pattern1 = "[0-9]{4}\\/[0-9]{2}\\/[0-9]{2}\\s[0-9]{2}\\:[0-9]{2}\\:[0-9]{2}".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        return s.substring(0, 4) + "-" + s.substring(5, 7) + "-" + s.substring(8, 19)
      }
      case None => {
        return "1970-01-01 12:00:00"
      }
    }
  }

  def getServiceName_FElog(log: String): String = {
    val pattern1 = "<name>[A-Za-z0-9\\_]*<\\/name><\\/Service>".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        return s
      }
      case None => {
        return "noServiceNameFound"
      }
    }
  }
  def getServiceId_FElog(log: String): String = {
    val pattern1 = "\\sid=\"[A-Z0-9\\_]*\"><name>".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        return s
      }
      case None => {
        return "noServiceIdFound"
      }
    }
  }

  def getOffice_id(log: String): String = {
    val pattern1 = "<OFFICEID VALUE=\"[A-Za-z0-9\\-]*\"".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        return s.substring(17, s.length() - 1)
      }
      case None => {
        return "noOfficeIdFound"
      }
    }
  }
  def getOrganization(log: String): String = {
    val pattern1 = "<ORGANIZATION VALUE=\"[A-Za-z0-9\\-]*\"".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        return s.substring(21, s.length() - 1)
      }
      case None => {
        return "noOrganizationFound"
      }
    }
  }
  def getCountry(log: String): String = {
    val pattern1 = "<COUNTRY VALUE=\"[A-Za-z0-9\\-]*\"".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        return s.substring(16, s.length() - 1)
      }
      case None => {
        return "noCountryFound"
      }
    }
  }
  
    //                                              2. SAP information
  //add new field mentioned by Alex on Sep.07 
  def getSAPFields(log: String): (String, String, String) = {
    //SAP fields are: NAME, TYPE, PRODUCT
    val pattern1 = "<SAP\\s[A-Za-z0-9\\s\"=\\_\\-$#\\/]*\\/>".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        val pattern2 = "NAME=\"[A-Za-z0-9\\_\\-$#\\/]*\"".r
        val strName = pattern2.findFirstIn(s)
        var name = "noSAPNameFound"
        var sapType = "noSAPTypeFound"
        var sapProduct = "noSAPProductFound"
        strName match {
          case Some(s2) => {
            name = s2.substring(6, s2.length() - 1)
          }
          case None => {
            name = "noSAPNameFound"
          }
        }

        val pattern3 = "TYPE=\"[A-Za-z0-9\\_\\-$#\\/]*\"".r
        val strType = pattern3.findFirstIn(s)
        strType match {
          case Some(s3) => {
            sapType = s3.substring(6, s3.length() - 1)
          }
          case None => {
            sapType = "noSAPTypeFound"
          }
        }

        val pattern4 = "PRODUCT=\"[A-Za-z0-9\\_\\-$#\\/]*\"".r
        val strProduct = pattern4.findFirstIn(s)
        strProduct match {
          case Some(s4) => {
            sapProduct = s4.substring(9, s4.length() - 1)
          }
          case None => {
            sapProduct = "noSAPProductFound"
          }
        }
        return (name, sapType, sapProduct)
      }
      case None => {
        return ("noSAPNameFound", "noSAPTypeFound", "noSAPProductFound")
      }
    }
  }
  def getSignValue_FElog(log: String): String = {
    val pattern1 = "<SIGN VALUE=\"[A-Za-z0-9\\_\\-$#\\/]*\"\\/>".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        return s.substring(13, s.length() - 3)
      }
      case None => {
        return "noSignValueFound"
      }
    }
  }
  
  
  //////////////////////   3.  functions for availRS (multi)
  def getEchoToken_FElog(log: String): String = {
    val pattern1 = "\\sEchoToken=\"[A-Za-z0-9\\_]*\"\\s".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        return s
      }
      case None => {
        return "noEchoTokenFound"
      }
    }
  }
  def get_checkin_date(log: String): String = {
    val pattern1 = "Start=\"[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}\"".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        return s.substring(7, s.length() - 1)
      }
      case None => {
        return "noCheckinDateFound"
      }
    }
  }
  def get_CI_day(log: String): String = {
    val pattern1 = "StartDateWindow DOW=\"[A-Za-z]{3}\"".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        return s.substring(21, s.length() - 1)
      }
      case None => {
        return "noCheckinDayFound"
      }
    }
  }
  def get_checkout_date(log: String): String = {
    val pattern1 = "End=\"[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}\"".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        return s.substring(5, s.length() - 1)
      }
      case None => {
        return "noCheckoutDateFound"
      }
    }
  }
  def get_CO_day(log: String): String = {
    val pattern1 = "EndDateWindow DOW=\"[A-Za-z]{3}\"".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        return s.substring(19, s.length() - 1)
      }
      case None => {
        return "noCheckoutDayFound"
      }
    }
  }
  /*
  def getLengthOfStay(dateBegin:String, dateEnd:String):String = {
    //use python do this, after the data is in Json format
  }
  */
  //transform a MultiSingle AvailRS OTA2011B_APD to serveral HotelStay
  def transToHotelStay(log: String): Array[String] = {
    val res = log.split("<\\/HotelStays>")(0)
    return res.split("<\\/HotelStay>") //split the log by the end of <HotelStay>...</HotelStay>, the last one is useless
  }
  //get from one HotelStay
  def get_city_code(hotelStay: String): String = {
    val pattern1 = "HotelCityCode=\"[A-Z]{3}\"".r
    val str = pattern1.findFirstIn(hotelStay)
    str match {
      case Some(s) => {
        return s.substring(15, s.length() - 1)
      }
      case None => {
        return "noCityCodeFound"
      }
    }
  }
  def get_property_id(hotelStay: String): String = {
    val pattern1 = "HotelCode=\"[A-Z0-9]*\"".r
    val str = pattern1.findFirstIn(hotelStay)
    str match {
      case Some(s) => {
        return s.substring(11, s.length() - 1)
      }
      case None => {
        return "noPropertyIdFound"
      }
    }
  }
  def get_chain_code(hotelStay: String): String = {
    val pattern1 = "ChainCode=\"[A-Z]{2}\"".r
    val str = pattern1.findFirstIn(hotelStay)
    str match {
      case Some(s) => {
        return s.substring(11, s.length() - 1)
      }
      case None => {
        return "noChainCodeFound"
      }
    }
  }
  def parseMultiSingle(log: String): Array[String] = {
    val DCXid = getDCX(log)._2
    val seqNb = getSeqNb(log)
    val office_id = getOffice_id(log)
    val organization = getOrganization(log)
    val country = getCountry(log)
    val hotelStayArray = transToHotelStay(log)
    val city_code_array = hotelStayArray.map { x => get_city_code(x) }
    val property_id_array = hotelStayArray.map { x => get_property_id(x) }
    val chaine_code_array = hotelStayArray.map { x => get_chain_code(x) }
    val checkin_date = get_checkin_date(log)
    val CI_day = get_CI_day(log) //day of week of checkin
    val checkout_date = get_checkout_date(log)
    val CO_day = get_CO_day(log)

    val sapFields: (String, String, String) = getSAPFields(log)
    val sapName = sapFields._1
    val sapType = sapFields._2
    val sapProduct = sapFields._3
    val signValue = getSignValue_FElog(log)
    val generateTime = get_generateTime(log)

    var res = new Array[String](hotelStayArray.length)
    for (n <- 0 to hotelStayArray.length - 1) {
      res.update(n, DCXid + "," + seqNb + "," + office_id + "," + organization + "," + country + "," + city_code_array(n) + "," + property_id_array(n)
        + "," + chaine_code_array(n) + "," + checkin_date + "," + CI_day + "," + checkout_date + "," + CO_day + "," +
        sapName + "," + sapType + "," + sapProduct + "," + signValue + "," + generateTime)
    }
    return res
  }
  
  
  //                                  4. single avail RS
  // parse single availRS (EchoToken is still MultiSingle)
  def transToRoomStay(log: String): Array[String] = {
    val res = log.split("<RoomStays")(1)
    val roomStay_array = res.split("<\\/RoomStay>")
    if (roomStay_array.last.contains("<\\/RoomStay>")) {
      return roomStay_array
    } else {
      return roomStay_array.dropRight(1)
    }
    //split the log by the end of <HotelStay>...</HotelStay>, the last one is useless
  }
  def get_RateCode(roomStay: String): String = {
    val pattern1 = "RatePlanCode=\"[A-Z0-9]*\"".r
    val str = pattern1.findFirstIn(roomStay)
    str match {
      case Some(s) => {
        return s.substring(14, s.length() - 1)
      }
      case None => {
        return "noRateCodeFound"
      }
    }
  }
  //retrieve booking from a <RoomStay>...</RoomStay>
  def get_booking_code(roomStay: String): String = {
    val pattern1 = "BookingCode=\"[a-zA-Z0-9\\.\\#\\*\\:]*\"".r
    val str = pattern1.findFirstIn(roomStay)
    str match {
      case Some(s) => {
        return s.substring(13, s.length() - 1)
      }
      case None => {
        return "noBookingCodeFound"
      }
    }
  }
  def get_RoomType(roomStay: String): String = {
    //this room type is return by provider, it may contain dirty char like \s, \*
    val pattern1 = "RoomType=\"[A-Z0-9\\*\\s]*\"".r
    val str = pattern1.findFirstIn(roomStay)
    str match {
      case Some(s) => {
        return s.substring(10, s.length() - 1)
      }
      case None => {
        return "noRoomTypeFound"
      }
    }
  }
  def get_RoomTypeCode(roomStay: String): String = {
    val pattern1 = "RoomTypeCode=\"[A-Z0-9\\*]*\"".r
    val str = pattern1.findFirstIn(roomStay)
    str match {
      case Some(s) => {
        return s.substring(14, s.length() - 1)
      }
      case None => {
        return "noRoomTypeFound"
      }
    }
  }
  def parseSingleAvail(log: String): Array[String] = {
    // retrieve booking code, RoomType, RoomTypeCode from single availRS
    // single availRS is actually availRS with EchoToken='MultiSingle', should be judge from its 
    // correponsding availRQ
    val DCXid = getDCX(log)._2
    val seqNb = getSeqNb(log)
    val office_id = getOffice_id(log)
    val organization = getOrganization(log)
    val country = getCountry(log)
    val city_code = get_city_code(log)
    val property_id = get_property_id(log)
    val chain_code = get_chain_code(log)
    val checkin_date = get_checkin_date(log)
    val CI_day = get_CI_day(log) //day of week of checkin
    val checkout_date = get_checkout_date(log)
    val CO_day = get_CO_day(log)

    val sapFields: (String, String, String) = getSAPFields(log)
    val sapName = sapFields._1
    val sapType = sapFields._2
    val sapProduct = sapFields._3
    val signValue = getSignValue_FElog(log)

    val roomStayArray = transToRoomStay(log)
    val bookingCode_array = roomStayArray.map { x => get_booking_code(x) }
    val roomType_array = roomStayArray.map { x => get_RoomType(x) }
    val roomTypeCode_array = roomStayArray.map { x => get_RoomTypeCode(x) }
    val rateCode_array = roomStayArray.map { x => get_RateCode(x) }
    val generateTime = get_generateTime(log)
    var res = new Array[String](roomStayArray.length)
    for (n <- 0 to roomStayArray.length - 1) {
      res.update(n, DCXid + "," + seqNb + "," + office_id + "," + organization + "," + country + "," + city_code + "," +
        property_id + "," + chain_code + "," + checkin_date + "," + CI_day + "," + checkout_date + "," + CO_day + "," +
        sapName + "," + sapType + "," + sapProduct + "," + signValue + "," +
        bookingCode_array(n) + "," + roomType_array(n) + "," + roomTypeCode_array(n) + "," + rateCode_array(n)
        + "," + generateTime)
    }
    return res
  }

  //////////////////////
  
  
  //////////////////////         5. functions for availRQ
  // parse the AvailRQ
  def getGuestCount(log: String): String = {
    val pattern1 = "Count=\"[0-9]{1,2}\"\\/>".r
    val str = pattern1.findFirstIn(log)
    str match {
      case Some(s) => {
        return s.substring(7, s.length() - 3)
      }
      case None => {
        return "-1"
      }
    }
  }

  def parseAvailRQ(log: String): String = {
    val DCXid = getDCX(log)._2
    val seqNb = getSeqNb(log)
    val guestCount = getGuestCount(log)
    //val isSingleAvailRQ = if(countSubString(log, "HotelRef")==1) "isSingleRQ" else "isNotSingleRQ"
    //not used because single/multi is not judged here
    val office_id = getOffice_id(log)
    val organization = getOrganization(log)
    val country = getCountry(log)
    val sapFields: (String, String, String) = getSAPFields(log)
    val sapName = sapFields._1
    val sapType = sapFields._2
    val sapProduct = sapFields._3
    val signValue = getSignValue_FElog(log)
    val generateTime = get_generateTime(log)

    val res = DCXid + "," + seqNb + "," + guestCount + "," + office_id + "," + organization + "," + country + "," +
      sapName + "," + sapType + "," + sapProduct + "," + signValue + "," + generateTime
    return res
  }
  //////////////////////
  
}
