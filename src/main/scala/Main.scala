import convertion.{JsonDownload, JsonToCsv}
import hivetable.HiveTable

object Main extends App {
  new JsonDownload
  new HiveTable
  new JsonToCsv
}
