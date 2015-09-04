name := "short-text-classification"

version := "1.0"

libraryDependencies ++= Seq(
  "org.apache.flink" % "flink-scala" % "0.9.1",
  "org.apache.flink" % "flink-streaming-scala" % "0.9.1",
  "org.apache.flink" % "flink-clients" % "0.9.1",
  "org.slf4j" % "slf4j-log4j12" % "1.2",
  "com.github.scopt" %% "scopt" % "3.3.0"
)

resolvers += Resolver.sonatypeRepo("public")