import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

name := "CcoOnSpark"

version := "0.2"

scalaVersion := "2.11.11"

val sparkVersion = "2.1.1"

val mahoutVersion = "0.13.0"

libraryDependencies ++= Seq(
  "log4j" % "log4j" % "1.2.17",
  // Mahout's Spark code
  "commons-io" % "commons-io" % "2.5" % "provided",
  "com.typesafe" % "config" % "1.3.0",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.mahout" % "mahout-math-scala_2.10" % mahoutVersion
    exclude("com.github.scopt", "scopt_2.10")
    exclude("org.spire-math", "spire_2.10")
    exclude("org.scalanlp", "breeze_2.10")
    exclude("org.spire-math", "spire-macros_2.10")
    exclude("org.apache.spark", "spark-mllib_2.10")
    exclude("org.json4s", "json4s-ast_2.10")
    exclude("org.json4s", "json4s-core_2.10")
    exclude("org.json4s", "json4s-native_2.10")
    exclude("org.scalanlp", "breeze-macros_2.10")
    exclude("com.esotericsoftware.kryo", "kryo")
    exclude("com.twitter", "chill_2.10"),
  "org.apache.mahout" % "mahout-spark_2.10" % mahoutVersion
    exclude("com.github.scopt", "scopt_2.10")
    exclude("org.spire-math", "spire_2.10")
    exclude("org.scalanlp", "breeze_2.10")
    exclude("org.spire-math", "spire-macros_2.10")
    exclude("org.apache.spark", "spark-mllib_2.10")
    exclude("org.json4s", "json4s-ast_2.10")
    exclude("org.json4s", "json4s-core_2.10")
    exclude("org.json4s", "json4s-native_2.10")
    exclude("com.twitter", "chill_2.10")
    exclude("org.scalanlp", "breeze-macros_2.10")
    exclude("com.esotericsoftware.kryo", "kryo")
    exclude("org.apache.spark", "spark-launcher_2.10")
    exclude("org.apache.spark", "spark-unsafe_2.10")
    exclude("org.apache.spark", "spark-tags_2.10")
    exclude("org.apache.spark", "spark-core_2.10")
    exclude("org.apache.spark", "spark-network-common_2.10")
    exclude("org.apache.spark", "spark-streaming_2.10")
    exclude("org.apache.spark", "spark-graphx_2.10")
    exclude("org.apache.spark", "spark-catalyst_2.10")
    exclude("org.apache.spark", "spark-sql_2.10"),
  "org.apache.mahout"  % "mahout-math" % mahoutVersion,
  "org.apache.mahout"  % "mahout-hdfs" % mahoutVersion
    exclude("com.thoughtworks.xstream", "xstream")
    exclude("org.apache.hadoop", "hadoop-client")
)

libraryDependencies += "joda-time" % "joda-time" % "2.10.1"

resolvers += "Temp Scala 2.11 build of Mahout" at "https://github.com/actionml/mahout_2.11/raw/mvn-repo/"

resolvers += Resolver.mavenLocal

SbtScalariform.scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(DanglingCloseParenthesis, Prevent)
  .setPreference(MultilineScaladocCommentsStartOnFirstLine, true)

assemblyMergeStrategy in assembly := {
  case "plugin.properties" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "package-info.class" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "UnusedStubClass.class" =>
    MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}