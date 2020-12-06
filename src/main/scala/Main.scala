import convertion.{JsonDownload, JsonToCsv}
import hivetable.HiveTable
import org.apache.log4j.{Level, Logger}

object Main extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  new JsonDownload
  new HiveTable
  new JsonToCsv
}
