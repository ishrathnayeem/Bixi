name := "Bixi_Project"

version := "0.1"
scalaVersion := "2.12.5"

libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.16.2"
resolvers += "Cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-mllib" % "3.0.0",

  "org.apache.hadoop" % "hadoop-common" % "2.6.0",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0",

  "com.typesafe" % "config" % "1.3.2",
  "org.apache.kafka" % "kafka-clients" % "2.4.0",
  "org.apache.kafka" % "connect-json" % "2.4.0",
  "org.apache.kafka" % "connect-runtime" % "2.4.0",
  "org.apache.kafka" % "kafka-streams" % "2.4.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.4.0",


  "io.confluent" % "kafka-json-serializer" % "5.0.1",
  "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1" artifacts Artifact("javax.ws.rs-api", "jar", "jar") // this is a workaround for https://github.com/jax-rs/api/issues/571
)
resolvers ++= Seq (
  Opts.resolver.mavenLocalFile,
  "Confluent" at "https://packages.confluent.io/maven"

)