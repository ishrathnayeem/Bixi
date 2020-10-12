package hivetable

import configuration.Config

class HiveTable extends Config {

  stmt execute """DROP TABLE IF EXISTS s19909_bixi_feed_ishrath.enriched_station_system"""
  stmt execute
    """CREATE TABLE s19909_bixi_feed_ishrath.enriched_station_system (
      |station_id                     INT,
      |external_id                    STRING,
      |station_name                   STRING,
      |short_name                     STRING,
      |lat                            STRING,
      |lon                            STRING,
      |rental_methods                 STRING,
      |rental_methods_1               STRING,
      |capacity                       INT,
      |electric_bike_surcharge_waiver BOOLEAN,
      |is_charging                    BOOLEAN,
      |eightd_has_key_dispenser       BOOLEAN,
      |has_kiosk                      BOOLEAN,
      |last_updated                   STRING,
      |ttl                            INT,
      |system_id                      STRING,
      |language                       STRING,
      |name                           STRING,
      |operator                       STRING,
      |url                            STRING,
      |purchase_url                   STRING,
      |start_date                     TIMESTAMP,
      |phone_number                   STRING,
      |email                          STRING,
      |license_url                    STRING,
      |timezone                       STRING
      |)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS TEXTFILE
      |LOCATION '/user/fall2019/ishrath/enriched_station_system'
      |TBLPROPERTIES (
      | "skip.header.line.count" = "1",
      |"serialization.null.format" = "")""".stripMargin

  println("enriched_station_system TABLE was CREATED")
}
