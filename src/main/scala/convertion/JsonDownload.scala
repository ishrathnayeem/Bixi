package convertion

import java.io.{File, PrintWriter}
import scala.io.{BufferedSource, Source}

class JsonDownload {

  val systemInformation: BufferedSource = Source.fromURL("https://api-core.bixi.com/gbfs/en/system_information.json")
  val systemInfo: String = systemInformation.mkString
  val systemInfoWriter = new PrintWriter(new File("/Users/ishrathnayeem/" +
    "MY MAC/Study/Big Data/MCIT/Final Project/JSON/json created/system_info.json"))
  systemInfoWriter.write(systemInfo)
  systemInfoWriter.close()

  val stationInformation: BufferedSource = Source.fromURL("https://api-core.bixi.com/gbfs/en/station_information.json")
  val stationInfo: String = stationInformation.mkString
  val stationInfoWriter = new PrintWriter(new File("/Users/ishrathnayeem/MY MAC/Study/Big Data/MCIT/" +
    "Final Project/JSON/json created/station_info.json"))
  stationInfoWriter.write(stationInfo)
  stationInfoWriter.close()
}