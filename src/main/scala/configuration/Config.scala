package configuration

import java.sql.{Connection, DriverManager, Statement}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait Config {

    val conf = new Configuration()
    conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop/etc/cloudera/core-site.xml"))
    conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop/etc/cloudera/hdfs-site.xml"))
    val hadoop: FileSystem = FileSystem.get(conf)

    val driverName: String = "org.apache.hive.jdbc.HiveDriver"
    Class.forName(driverName)
    val connection: Connection = DriverManager
      .getConnection("jdbc:hive2://quickstart.cloudera:10000/fall2019_ishrath;user=ishrathnayeem;password=Faheemnayeem1.")
    val stmt: Statement = connection.createStatement()
}
