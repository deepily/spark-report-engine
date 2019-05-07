//name := "WIOA WPMA Report"
//
//version := "1.0.0"
//
//scalaVersion := "2.11.12"
//
//// https://mvnrepository.com/artifact/org.apache.spark/spark-core
//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
//libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.336"

name := "WIOA WPMA Report" // this *is* available for output w/in commonSettings artifactName block!
organization := "gov.doleta.wips"

// trying to access version w/in commonSettings artifactName block produces "... / That / That / That version )..."  Arg!
//version := "1.0.0" 

// so use std assignment and get on with it!
val version = "1.0.0"
val sparkVersion = "2.4.0"

lazy val commonSettings = Seq(

  scalaVersion := "2.11.12",

  artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact ) =>

    //artifact.name + "-" + sys.env.get("BUILD_NUMBER").getOrElse(version) + "." + artifact.extension
    artifact.name + "-" + sys.env.getOrElse( "BUILD_NUMBER", version ).toString + "." + artifact.extension

  }
)

lazy val commonDependencies = Seq(
  //libraryDependencies += "mysql" %% "mysql-connector-java" % "6.0.4",
  libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0",
  libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0",
  libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.336"
)

lazy val aggregatedCommons = commonDependencies ++ commonSettings

lazy val app = project
  .in(file("."))
  .settings(aggregatedCommons)