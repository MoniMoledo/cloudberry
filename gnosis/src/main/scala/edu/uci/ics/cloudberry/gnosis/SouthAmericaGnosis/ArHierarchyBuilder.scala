//package edu.uci.ics.cloudberry.gnosis.SouthAmericaGnosis
//
//import java.io._
//import java.lang.Exception
//
//import com.vividsolutions.jts.geom.Geometry
//import edu.uci.ics.cloudberry.gnosis.BrGeoJSONIndex
//import edu.uci.ics.cloudberry.util.Profile._
//import org.wololo.geojson.Feature
//import org.wololo.jts2geojson.GeoJSONWriter
//
//import scala.collection.JavaConverters._
//import scala.collection.mutable
//import scala.collection.mutable.ArrayBuffer
//
//object BrHierarchyBuilder {
//
//
//  val CountryJsonPath = "CountryPath"
//  val StateJsonPath = "StatePath"
//  val CityJsonPath = "CityPath"
//
//  val usage =
//    """
//      |Usage: BrHierarchyBuilder  -country /path/to/country.json -state /path/to/state.json -city /path/to/city.json
//      |It will generate the spatial relationship between , country, state, and cities.
//    """.stripMargin
//
//  def parseOption(map: OptionMap, list: List[String]) {
//    list match {
//      case Nil =>
//      case "-h" :: tail => System.err.println(usage); System.exit(0)
//      case "-country" :: value :: tail => parseOption(map += (CountryJsonPath -> value), tail)
//      case "-state" :: value :: tail => parseOption(map += (StateJsonPath -> value), tail)
//      case "-city" :: value :: tail => parseOption(map += (CityJsonPath -> value), tail)
//      case option :: tail => System.err.println("unknown option:" + option); System.err.println(usage); System.exit(1);
//    }
//  }
//
//  type OptionMap = mutable.Map[String, Any]
//
//  val countryIndex = new BrGeoJSONIndex()
//  val stateIndex = new BrGeoJSONIndex()
//  val cityIndex = new BrGeoJSONIndex()
//  val cityToStateMap = mutable.Map.empty[Int, String]
//
//  def main(args: Array[String]) = {
//    val config: OptionMap = mutable.Map.empty[String, Any]
//    parseOption(config, args.toList)
//    profile("loadShape")(loadShapes(config))
//    profile("parseRelation")(parseRelation)
//    profile("writeRelation")(writeRelation)
//  }
//
//  def loadShapes(config: OptionMap): Unit = {
//    Seq((StateJsonPath, stateIndex),
//      (CountryJsonPath, countryIndex),
//      (CityJsonPath, cityIndex)).foreach {
//      case (key: String, index: BrGeoJSONIndex) => config.get(key) match {
//        case Some(path: String) =>
//          BrGeoGnosis.loadShape(new File(path), index)(RawBrEntityBuilder.apply)
//        case _ => System.err.print(usage); throw new IllegalArgumentException(s"$key is missing")
//      }
//    }
//  }
//
//
//  def parseRelation(): Unit = {
//    profile("parse city") {
//      cityIndex.entities.foreach(entity => {
//        try{
//        val city = entity.asInstanceOf[BrCityEntity]
//        val stateName = findState(city)
//        cityToStateMap += city.cityID -> stateName
//        }
//        catch {
//          case e:ClassCastException =>
//        }
//      })
//    }
//  }
//
//  def findState(city: BrCityEntity): String = {
//    val state = stateIndex.entities.find(_.asInstanceOf[BrStateEntity].stateID == city.stateID)
//    if(state.isDefined)
//      return state.get.name
//    else return ""
//  }
//
//  def writeRelation(): Unit = {
//    writeGeoProperty("BrCountry.json", annotate(countryIndex.entities))
//    writeGeoProperty("BrState.json", annotate(stateIndex.entities))
//    writeGeoProperty("BrCity.json", annotate(cityIndex.entities))
//  }
//
//  def annotate(entities: Seq[IBrGeoJSONEntity]): Seq[String] = {
//    val Unknown = "Unknown"
//    //def getStateName(stateID: Int): String = stateIndex.entities.asInstanceOf[ArrayBuffer[BrStateEntity]].find(_.stateID == stateID).map(_.name).getOrElse(Unknown)
////    def getCountyName(stateID: Int, countyID: Int): String =
////      countyIndex.entities.map(_.asInstanceOf[USCountyEntity]).
////        find(county => county.stateID == stateID && county.countyID == countyID).map(_.name).getOrElse(Unknown)
//    entities.map(entity => entity match {
//      case e: BrCountryEntity =>
//        writeGeoJsonFeature(e.geometry, e.toPropertyMap)
//      case e: BrStateEntity =>
//        writeGeoJsonFeature(e.geometry, e.toPropertyMap)
//      case e: BrCityEntity =>
//        val sttName: String = cityToStateMap.get(e.cityID).getOrElse("")
//        writeGeoJsonFeature(e.geometry, e.copy(geoID = e.cityID.toString,
//                  countryID = e.countryID, countryName = e.countryName,
//                  stateID = e.stateID,
//                  stateName = sttName).toPropertyMap)
//    })
//  }
//
//
//  val writer = new GeoJSONWriter()
//
//  //TODO convert to stream to save a big chunk of memory
//  def writeGeoJsonFeature(geometry: Geometry, propertyMap: Map[String, AnyRef]): String = {
//    new Feature(writer.write(geometry), propertyMap.asJava).toString
//  }
//
//  def writeGeoProperty(filePath: String, geojsonString: Seq[String]): Unit = {
//    val file = new File(filePath)
//    val bw = new BufferedWriter(new FileWriter(file))
//    bw.write("{ \"type\": \"FeatureCollection\",")
//    bw.newLine()
//    bw.write("\"features\": [")
//    bw.newLine()
//    bw.write(geojsonString.head.toString)
//    geojsonString.tail.foreach {
//      json =>
//        bw.newLine()
//        bw.write("," + json.toString)
//    }
//    bw.newLine()
//    bw.write(']')
//    bw.newLine()
//    bw.write('}')
//    bw.newLine()
//    bw.close()
//  }
//
//}
//
//object RawBrEntityBuilder {
//  val GEO_ID = "IBGE:GEOCODIGO"
//  val COUNTRYID = 59470
//  val COUNTRYNAME = "Brazil"
//  val NAME = "name"
//
//  val PENDING = "pending"
//
//  val CITYID = "GEOID"
//
//  def apply(map: Map[String, AnyRef], geometry: Geometry): IBrGeoJSONEntity = {
//    map.get("admin_level").get match {
//      case "8" =>
//        BrCityEntity(
//          geoID = map.get(GEO_ID).getOrElse("").asInstanceOf[String],
//          countryID = COUNTRYID,
//          countryName = COUNTRYNAME,
//          stateID =  map.get(GEO_ID).getOrElse("00").asInstanceOf[String].substring(0,2).toInt,
//          stateName = PENDING,
//          cityID = map.get(GEO_ID).getOrElse("00").asInstanceOf[String].toInt,
//          name = map.get(NAME).get.asInstanceOf[String],
//          geometry
//        )
//      case "4" => BrStateEntity(
//            geoID = map.get(GEO_ID).get.asInstanceOf[String],
//            countryID = COUNTRYID,
//            countryName = COUNTRYNAME,
//            stateID = map.get(GEO_ID).get.asInstanceOf[String].toInt,
//            name = map.get(NAME).get.asInstanceOf[String],
//            geometry
//          )
//      case "2" => BrCountryEntity(
//            geoID = COUNTRYID.toString,
//            countryID = COUNTRYID,
//            name = COUNTRYNAME,
//            geometry
//          )
//        }
//    }
//}
