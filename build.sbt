lazy val commonSettings = Seq(
  name := "py2j-arrow",
  version := "0.0.1-SNAPSHOT",
  scalaVersion := "2.12.10",
  crossScalaVersions := Seq("2.12.10", "2.11.12"),
  organization := "com.azavea",
  scalacOptions ++= Seq(
    "-deprecation",
    "-unchecked",
    "-language:implicitConversions",
    "-language:reflectiveCalls",
    "-language:higherKinds",
    "-language:postfixOps",
    "-language:existentials",
    "-feature"
  ),
  libraryDependencies ++= Seq(
    "org.apache.arrow" % "arrow-vector"         % "0.15.0",
    "org.typelevel"   %% "spire"                % "0.16.2",
    "org.slf4j"        % "slf4j-api"            % "1.7.28",
    "org.slf4j"        % "slf4j-log4j12"        % "1.7.28",
    "org.nd4j"         % "nd4j-native-platform" % "1.0.0-beta5",
    "org.nd4j"         % "nd4j-arrow"           % "1.0.0-beta5",
    "org.scalatest"   %% "scalatest"            % "3.0.8" % Test
  ),
  headerLicense := Some(HeaderLicense.ALv2("2019", "Azavea")),
  fork := true,
  Test / fork := true,
  Test / parallelExecution := false,
  Test / testOptions += Tests.Argument("-oDF"),
  // ThisBuild / useCoursier := false,
  shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
)

lazy val root = Project("py2j-arrow", file("."))
  .aggregate(py4, pyj)
  .settings(commonSettings: _*)

lazy val py4 = project
  .settings(commonSettings)
  .settings(libraryDependencies += "net.sf.py4j" % "py4j" % "0.10.8.1")

lazy val pyj = project
  .settings(commonSettings)
  .settings(javaOptions += s"-Djava.library.path=/usr/local/lib:/usr/local/lib/python3.7/site-packages/jep")
  // You also need to run pip install jep
  .settings(libraryDependencies += "black.ninia" % "jep" % "3.9.0")
