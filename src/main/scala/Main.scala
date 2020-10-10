import convertion.{JsonDownload, JsonToCsv}
import hive.table.HiveTable

object Main extends App {
  new JsonDownload
  new JsonToCsv
  new HiveTable
}
