package task2

import java.text.SimpleDateFormat
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter
import com.esri.core.geometry.Point
import org.joda.time.{DateTime, Duration}
import task2.GeoJsonProtocol._

case class Trip(
                 pickupTime: DateTime,
                 dropoffTime: DateTime,
                 pickupLoc: Point,
                 dropoffLoc: Point)

object RunGeoTime extends Serializable {

  val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)

  val taxipath1="/taxidata/green/green_tripdata*"
  //val taxipath2="/taxidata/yellow/yellow_tripdata_2015-1*"
  val geopath="/bo_data/nyc-bo.geojson"
  val outputpath="/dschen/Q2"


  //test
  //val taxipath1="/Users/whhy/Downloads/project/green_taxi.csv"
  //val taxipath2="/Users/whhy/Downloads/project/yellow_taxi.csv"
  //val geopath="/Users/whhy/Downloads/project/nyc_bo.geojson"
  //var outputpath="/Users/whhy/Downloads/out2"
  //test


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("GeoSearch_v2.2_YeP3")
    //.setMaster("local[*]")   //test
    val sc = new SparkContext(conf)

    val safeParse1 = safe(parse1)
    //val safeParse2 = safe(parse2)
    val taxiRaw = sc.textFile(taxipath1)
    //val taxiRaw2= sc.textFile(taxipath2)
    val taxiParsed = taxiRaw.map(safeParse1)
    //val taxiParsed2 = taxiRaw2.map(safeParse2)
    //test
    //test
    //val taxiParsed = taxiParsed1.union(taxiParsed2)
    taxiParsed.cache()
    println("      ----------------data is ready!----------------")
    //val taxiBad = taxiParsed.collect({
    //  case t if t.isRight => t.right.get
    //})
    //taxiBad.collect().foreach(println)

    val taxiGood = taxiParsed.collect({
      case t if t.isLeft => t.left.get
    })
    taxiGood.cache()
    taxiParsed.unpersist()

    def hours(trip: Trip): Long = {
      val d = new Duration(trip.pickupTime, trip.dropoffTime)
      d.getStandardHours
    }

    //taxiGood.values.map(hours).countByValue().toList.sorted.foreach(println)

    val taxiClean = taxiGood.filter {
      case (lic, trip) => {
        val hrs = hours(trip)
        0 <= hrs && hrs < 3
      }
    }
    taxiClean.cache()
    taxiGood.unpersist()


    val taxiSelect=taxiGood.filter{
      case (lic,trip) =>{
        val a=trip.pickupTime.hourOfDay().get()
        a>2 && a<6
      }
    }


    println("      ----------------time data is ready!----------------")
    val geojson = scala.io.Source.fromFile(geopath).mkString
    val features = geojson.parseJson.convertTo[FeatureCollection]

    val areaSortedFeatures = features.sortBy(f => {
      val borough = f("boroughCode").convertTo[Int]
      (borough, -f.geometry.area2D())
    })

    val bFeatures = sc.broadcast(areaSortedFeatures)
    println("      ----------------geojson is ready!----------------")
    def borough(trip: Trip): Option[String] = {
      val feature: Option[Feature] = bFeatures.value.find(f => {
        f.geometry.contains(trip.dropoffLoc)   //dropoff or pickup
      })
      feature.map(f => {
        f("borough").convertTo[String]
      })
    }

    def id(trip: Trip) = {
      val feature: Option[Feature] = bFeatures.value.find(f => {
        f.geometry.contains(trip.pickupLoc)   //dropoff or pickup
      })
      feature.map(f => {
        f.getid
      })
    }

    //taxiClean.values.map(borough).countByValue().foreach(println)
    //taxiClean.values.map(id).countByValue().foreach(println)

    def hasZero(trip: Trip): Boolean = {
      val zero = new Point(0.0, 0.0)
      (zero.equals(trip.pickupLoc) || zero.equals(trip.dropoffLoc))
    }

    val taxiDone = taxiSelect.filter {
      case (lic, trip) => !hasZero(trip)
    }.cache()
    taxiClean.unpersist()
    println("      ----------------geo data is ready!----------------")
    val result=taxiDone.values.map(id).countByValue()
    println("      ----------------task is finishing!----------------")
    val resultRDD=sc.parallelize(result.toSeq)
    resultRDD.coalesce(1).saveAsTextFile(outputpath)
  }

  def point(longitude: String, latitude: String): Point = {
    new Point(longitude.toDouble, latitude.toDouble)
  }

  def parse1(line: String): (String, Trip) = {
    val fields = line.split(',')
    val license = fields(0)
    val pickupTime = new DateTime(formatter.parse(fields(1)))
    val dropoffTime = new DateTime(formatter.parse(fields(2)))
    val pickupLoc = point(fields(5), fields(6))
    val dropoffLoc = point(fields(7), fields(8))
    val trip = Trip(pickupTime, dropoffTime, pickupLoc, dropoffLoc)
    (license, trip)
  }

  def parse2(line: String): (String, Trip) = {
    val fields = line.split(',')
    val license = fields(0)
    val pickupTime = new DateTime(formatter.parse(fields(1)))
    val dropoffTime = new DateTime(formatter.parse(fields(2)))
    val pickupLoc = point(fields(5), fields(6))
    val dropoffLoc = point(fields(9), fields(10))
    val trip = Trip(pickupTime, dropoffTime, pickupLoc, dropoffLoc)
    (license, trip)
  }


  def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
    new Function[S, Either[T, (S, Exception)]] with Serializable {
      def apply(s: S): Either[T, (S, Exception)] = {
        try {
          Left(f(s))
        } catch {
          case e: Exception => Right((s, e))
        }
      }
    }
  }


}



