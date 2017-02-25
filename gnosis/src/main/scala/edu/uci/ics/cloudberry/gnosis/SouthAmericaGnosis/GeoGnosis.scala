//package edu.uci.ics.cloudberry.gnosis.SouthAmericaGnosis
//
///**
//  * Created by Monique on 10/11/2016.
//  */
//import java.io.{File, FilenameFilter}
//
//import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry}
//import play.api.libs.json.{JsObject, Json, Writes}
//
//import scala.collection.mutable
//
//class BrGeoGnosis(levelGeoPathMap: Map[TypeLevel, File]) extends IBrGnosis {
//
//  import BrGeoGnosis._
//
//  val levelShapeMap: Map[TypeLevel, BrGeoJSONIndex] = load(levelGeoPathMap)
//
//  def load(shapeMap: Map[TypeLevel, File]): Map[TypeLevel, BrGeoJSONIndex] = {
//    BrOrderedLevels.map(level => {
//      val index = new BrGeoJSONIndex()
//      loadShape(shapeMap.get(level).get, index)(IBrGeoJSONEntity.apply)
//      level -> index
//    }).toMap
//  }
//
//  lazy val country: Seq[BrCountryEntity] = {
//    levelShapeMap.get(BrCountryLevel).get.entities.map(_.asInstanceOf[BrCountryEntity])
//  }
//
//  lazy val states: Seq[BrStateEntity] = {
//    levelShapeMap.get(BrStateLevel).get.entities.map(_.asInstanceOf[BrStateEntity])
//  }
//
//  lazy val cities: Seq[BrCityEntity] = {
//    levelShapeMap.get(BrCityLevel).get.entities.map(_.asInstanceOf[BrCityEntity])
//  }
//
//  lazy val cityByNameList: Map[String, List[BrCityEntity]] = {
//    val map = mutable.Map.empty[String, List[BrCityEntity]]
//    cities.foreach(city => map += (city.name -> (city :: (map.getOrElse(city.name, Nil)))))
//    map.toMap
//  }
//  lazy val stateByIdList: Map[Int, List[BrStateEntity]] = {
//    val map = mutable.Map.empty[Int, List[BrStateEntity]]
//    states.foreach(state => map += (state.stateID -> (state :: (map.getOrElse(state.stateID, Nil)))))
//    map.toMap
//  }
//
//  lazy val countryShapes: IGeoIndex = levelShapeMap.get(BrCountryLevel).get
//  lazy val stateShapes: IGeoIndex = levelShapeMap.get(BrStateLevel).get
//  lazy val cityShapes: IGeoIndex = levelShapeMap.get(BrCityLevel).get
//
//  // used in geo tag
////  def tagNeighborhood(cityName: String, rectangle: Rectangle): Option[BrGeoTagInfo] = {
////    val box = new Envelope(rectangle.swLog, rectangle.neLog, rectangle.swLat, rectangle.neLat)
////    cityByNameList.get(cityName).flatMap(list => list.find(_.geometry.getEnvelopeInternal.covers(box)).map(BrGeoTagInfo(_)))
////  }
//
//  // used in geo tag
//  override def tagPoint(longitude: Double, latitude: Double): Option[BrGeoTagInfo] = {
//    val box = new Envelope(new Coordinate(longitude, latitude))
//    val state = stateShapes.search(box).headOption.map(entity => BrGeoTagInfo(entity.asInstanceOf[BrStateEntity]))
//    return state
//  }
//
//  // used in geo tag
//  def tagCity(cityName: String): Option[BrGeoTagInfo] = {
//    cityByNameList.get(cityName).flatMap(
//      list => list.find(_.countryID == 59470).map(BrGeoTagInfo(_)))
//  }
//
//}
//
//object BrGeoGnosis {
//
//  case class BrGeoTagInfo(countryID: Int, countryName: String,
//                          stateID: Int, stateName: String,
//                          cityID: Option[Int], cityName: Option[String]) extends IGeoTagInfo{
//    override def toString: String = Json.toJson(this).asInstanceOf[JsObject].toString()
//  }
//
//  object BrGeoTagInfo {
//    implicit val writer: Writes[BrGeoTagInfo] = Json.writes[BrGeoTagInfo]
//
//    def apply(entity: IBrGeoJSONEntity): BrGeoTagInfo = {
//      entity match {
//        case country: BrCountryEntity => BrGeoTagInfo(countryID = country.countryID, countryName = country.name,
//                                                      stateID = 0, stateName = "",
//                                                      None, None)
//        case state: BrStateEntity => BrGeoTagInfo(countryID = state.countryID, countryName = state.countryName,
//                                                  stateID = state.stateID, stateName = state.name,
//                                                  None, None)
//        case city: BrCityEntity => BrGeoTagInfo(countryID = city.countryID, countryName = city.countryName,
//                                                stateID = city.stateID, stateName = city.stateName,
//                                                Some(city.cityID), Some(city.name))
//
//      }
//    }
//}
//  def loadShape(file: File, index: BrGeoJSONIndex)(builder: (Map[String, AnyRef], Geometry) => IBrGeoJSONEntity): Unit = {
//    if (file.isDirectory) {
//      file.listFiles(new FilenameFilter {
//        override def accept(dir: File, name: String): Boolean = name.endsWith(".json")
//      }).foreach { file =>
//        loadShape(file, index)(builder)
//      }
//    } else {
//      val textJson = loadSmallJSONFile(file)
//      index.loadShape(textJson)(builder)
//    }
//  }
//  val StateAbbr2FullNameMap: Map[String, String] = Map(
//    "AC" -> "Acre",
//    "RO" -> "Rondônia",
//    "RS" -> "Rio Grande do Sul",
//    "AL" -> "Alagoas",
//    "SE" -> "Sergipe",
//    "CE" -> "Ceará",
//    "PB" -> "Paraíba",
//    "RN" -> "Rio Grande do Norte",
//    "ES" -> "Espiríto santo",
//    "MS" -> "Mato Grosso do Sul",
//    "PR" -> "Paraná",
//    "PE" -> "Pernambuco",
//    "BA" -> "Bahia",
//    "PI" -> "Piauí",
//    "RJ" -> "Rio de Janeiro",
//    "DF" -> "Distrito Federal",
//    "MT" -> "Mato Grosso",
//    "PA" -> "Pará",
//    "AM" -> "Amazonas",
//    "MG" -> "Minas Gerais",
//    "SC" -> "Santa Catarina",
//    "SP" -> "São Paulo",
//    "TO" -> "Tocantins",
//    "GO" -> "Goiânia",
//    "AP" -> "Amapá",
//    "MA" -> "Maranhão",
//    "RR" -> "Roraima"
//  )
//}
