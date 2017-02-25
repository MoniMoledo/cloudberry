//package edu.uci.ics.cloudberry.gnosis.SouthAmericaGnosis
//
//import com.vividsolutions.jts.geom.{Envelope, Geometry}
//import com.vividsolutions.jts.index.strtree.STRtree
//import org.wololo.geojson.{Feature, FeatureCollection, GeoJSONFactory}
//import org.wololo.jts2geojson.GeoJSONReader
//
//import scala.collection.JavaConverters._
//import scala.collection.mutable.ArrayBuffer
//
//
//class GeoJSONIndex() extends IGeoIndex {
//
//  //generic, find a way for this to work    ////////////////////////////really need this ?
//  private val index: STRtree = new STRtree()
//  val entities: ArrayBuffer[IBrGeoJSONEntity] = new ArrayBuffer[IBrGeoJSONEntity]()
//
//  /**
//    * Load GeoJson. It may get called many times, but once calls search() methods it can not load any more.
//    *
//    * @param geoJsonString
//    */
//  def loadShape(geoJsonString: String)(implicit builder: (Map[String, AnyRef], Geometry) => IBrGeoJSONEntity): Unit = {
//    val geoJSONReader = new GeoJSONReader()
//    val featureCollection: FeatureCollection = GeoJSONFactory.create(geoJsonString).asInstanceOf[FeatureCollection]
//    featureCollection.getFeatures.foreach { f: Feature =>
//      val geometry: Geometry = geoJSONReader.read(f.getGeometry)
//      entities += builder(f.getProperties.asScala.toMap, geometry)
//      index.insert(geometry.getEnvelopeInternal, entities.size - 1)
//    }
//  }
//
//  override def search(geometry: Geometry): Seq[IBrGeoJSONEntity] = {
//    search(geometry.getEnvelopeInternal).filter(_.geometry.intersects(geometry))
//  }
//
//  override def search(envelope: Envelope): Seq[IBrGeoJSONEntity] = {
//    index.query(envelope).asScala.map(item => entities(item.asInstanceOf[Int]))
//  }
//}