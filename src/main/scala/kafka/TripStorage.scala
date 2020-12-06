package kafka
import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
class TripStorage {

  var startDate:String=""
  var startStation:Any =0
  var endDate:String =""
  var endStation:Any=0
  var durationSec:Any=0
  var isMember:Any=0

  def this(start_date: Option[String], start_station_code:Option[Any], end_date: Option[String],
           end_station_code:Option[Any] , duration_sec:Option[Any], is_member: Option[Any])
  {
    this()
    this.startDate = start_date.getOrElse("")
    this.startStation = start_station_code.getOrElse(0)
    this.endDate = end_date.getOrElse("")
    this.endStation = end_station_code.getOrElse(0)
    this.durationSec = duration_sec.getOrElse(0)
    this.isMember = is_member.getOrElse(0)
  }

  def getTripRecord: String = this.startDate+","+this.startStation+","+this.endDate+","+this.endStation+
    ","+this.durationSec+","+this.isMember
}
