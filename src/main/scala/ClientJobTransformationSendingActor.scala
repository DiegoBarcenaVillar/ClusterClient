import akka.actor.Actor
import akka.actor.ActorPath
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
import akka.pattern.Patterns
import akka.util.Timeout
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import spray.json._
import DefaultJsonProtocol._

class ClientJobTransformationSendingActor extends Actor {

  val constants : Constants = new Constants()

  val receptionistPorts = constants.receptionistPorts

  var sparkSession :SparkSession = SparkSession.builder()
    .master("local[2]")
    .getOrCreate()

  val initialContacts = Set(
    ActorPath.fromString("akka.tcp://ClusterSystem@127.0.0.1:".concat(receptionistPorts(0))
      .concat("/system/receptionist"))
    , ActorPath.fromString("akka.tcp://ClusterSystem@127.0.0.1:".concat(receptionistPorts(1))
      .concat("/system/receptionist"))
    , ActorPath.fromString("akka.tcp://ClusterSystem@127.0.0.1:".concat(receptionistPorts(2))
      .concat("/system/receptionist"))
  )

  val settings = ClusterClientSettings(context.system)
    .withInitialContacts(initialContacts)

  val client = context.system.actorOf(ClusterClient.props(settings), "demo-client")

  var columns : Map[String,String] = _

  implicit val timeout = Timeout(5 seconds)


  def receive = {

    case msg:String =>{
      println(s"Client saw result: $msg")
      println("")
      processStringJson(msg)
    }

    case SendString(query) =>
      processSend(query)

    case SendInt(counter) =>
      processSend ("PING")

    case columns : Array[(String,String)] =>
      processArrayOfTuples(columns)

    case message : JsValue =>
      processJSValue(message)
  }

  def processSend (text:String ) :  Unit= {

    val result = Patterns.ask(client,ClusterClient.Send("/user/clusterListener", text, localAffinity = false), timeout)

    result.onComplete {
      case Success(transformationResult) => {
        self ! transformationResult
      }
      case Failure(t) => println("An error has occured: " + t.getMessage)
    }
  }

  def processStringJson(source:String):Unit = {

    val listOfRows = source.split(";")

    listOfRows.foreach(t => println(t.parseJson))

    val itemJson = listOfRows.map(t => t.parseJson.convertTo[Map[JsValue, JsValue]])
    var i : Int = 0
    var header : String = ""
    var row : String = " "

    while(i<itemJson.size)
    {
      if(i==0)
      {
        itemJson(i).foreach(
          x =>
          {
            header += x._1 + " "
            row += x._2 + "   "
          }
        )
        println(header)
        println(row)
      }
      else
      {
        row = ""
        itemJson(i).foreach(
          x =>
          {
            row += x._2 + "  "
          }
        )
        println(row)
      }
      i = i+1
    }
  }

  def processArrayOfTuples(columns : Array[(String,String)]) : Unit = {

    columns.foreach( t =>
        System.out.println(t._1.concat(" ").concat(t._2) )
    )
    this.columns = columns.toMap[String,String]
  }

  def processArrayOfSring(rows : Array[String]) : Unit = {

    rows.foreach( t => {
          System.out.println("**Row Beginning**" )
          System.out.println(t.toString)
          System.out.println("**Row End**" )
      }
    )

    var lstStr = scala.collection.mutable.MutableList[String]()
    var lstCol = scala.collection.mutable.MutableList[String]()

    val lstRow = new java.util.ArrayList[Row]()
    var isFirstRow = true

    rows.map(t => t.parseJson.convertTo[scala.collection.immutable.Map[JsValue, JsValue]])
      .foreach(t=> {
        System.out.println("**Row Beginning**")

        var str = new StringBuilder()
        t.keySet.foreach(s => {
          System.out.println(s + " " + t.get(s).mkString)
          str ++= t.get(s).mkString.concat(",")
          if(isFirstRow) {
            lstCol += s.convertTo[String]
          }
        })

        isFirstRow = false
        lstStr += str.substring(0,str.size-1).mkString

        System.out.println("**Row End**")
      })

    System.out.println("**List Beginning**")

    lstStr.foreach(u=>{
      var i:Int = 0

      val b = u.split(",").map(t=>{

        val columnName: String = lstCol.get(i).get
        i += 1
        val columnType: String = this.columns.get(columnName).get
        getValueFromString(t, columnType)
      }).toList
      lstRow.add(Row.apply(b:_*))
    })

    var fields = Array[StructField]()

    lstCol.foreach(t=>{
      val fieldDataTypeDT: DataType = getDataTypeFromString(columns.get(t).getOrElse("NONE"))
      fields =  fields :+ new StructField(t, fieldDataTypeDT, true)
    })

    val struct = new StructType(fields)

    var df : DataFrame = null

    try {
      df = sparkSession.createDataFrame(lstRow, struct)
    }catch{
      case ex:Exception => ex.printStackTrace()
    }

    try {
      System.out.println(df.show())
    }catch{
      case ex:Exception => ex.printStackTrace()
    }

    System.out.println("**List end**")
  }

  def getValueFromString (fieldDataTypeStr : String, columnDataTypeStr : String): Any = {

    import java.sql.Timestamp

    columnDataTypeStr match {
      case "StringType" => return fieldDataTypeStr
      case "BinaryType" => return fieldDataTypeStr
      case "BooleanType" => return fieldDataTypeStr.toBoolean
      case "DateType" => return getDateValue(fieldDataTypeStr)
      case "TimestampType" => return Timestamp.valueOf(fieldDataTypeStr)
      case "DoubleType" => return fieldDataTypeStr.toDouble
      case "FloatType" => return fieldDataTypeStr.toFloat
      case "ByteType" => return fieldDataTypeStr.toByte
      case "IntegerType" => return fieldDataTypeStr.toInt
      case "LongType" => return fieldDataTypeStr.toLong
      case "ShortType" => return fieldDataTypeStr.toShort
      case _ => return "NONE"
    }
  }

  def getDateValue(fieldDataTypeStr : String) = {

    import java.text.SimpleDateFormat
    import java.sql.Date
    val format = new SimpleDateFormat("dd-MM-yyyy")
    val parsed = format.parse(fieldDataTypeStr)
    val sqlDate = new Date(parsed.getTime)
    sqlDate
  }

  def getDataTypeFromString (columnDataTypeStr : String): DataType = {
    columnDataTypeStr match{
      case "StringType"			         => return DataTypes.StringType
      case "BinaryType"             => return DataTypes.StringType
      case "BooleanType"            => return DataTypes.BooleanType
      case "DateType"               => return DataTypes.DateType
      case "TimestampType"          => return DataTypes.TimestampType
      case "DoubleType"             => return DataTypes.DoubleType
      case "FloatType"              => return DataTypes.FloatType
      case "ByteType"               => return DataTypes.ByteType
      case "IntegerType"            => return DataTypes.IntegerType
      case "LongType"               => return DataTypes.LongType
      case "ShortType"              => return DataTypes.ShortType
      case _ =>  return DataTypes.StringType
    }
  }

  def processJSValue(message : JsValue): Unit = {

    System.out.println("***" + message.compactPrint + "***")
    System.out.println("***" + message.isInstanceOf[Array[(String,String)]] + "***")
    System.out.println("***" + message.isInstanceOf[Array[String]] + "***")

    try {
      processArrayOfSring(message.convertTo[Array[String]])
    }catch{
      case de: spray.json.DeserializationException => processArrayOfTuples(message.convertTo[Array[(String,String)]])
      case _ =>
    }
  }
}
