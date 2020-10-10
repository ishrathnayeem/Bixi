name := "Bixi_Project"

version := "0.1"
scalaVersion := "2.11.0"

libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.16.2"
resolvers += "Cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos/"

val hadoopVersion = "2.7.3"
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common",
  "org.apache.hadoop" % "hadoop-hdfs",
).map(_ % hadoopVersion)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.3"