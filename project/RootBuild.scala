import _root_.sbt.Keys._
import sbt._
import sbt.Classpaths.publishTask
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import scala.sys.process._
import scala.util.Properties.{ envOrNone => env }
import scala.collection.JavaConversions._


object RootBuild extends Build {

  //////// Project definitions/configs ///////
  val OBSELETE_HADOOP_VERSION = "1.0.4"
  val DEFAULT_HADOOP_VERSION = "2.2.0"

  val SPARK_VERSION = "1.6.0-adatao-1.7.0"

  val YARN_ENABLED = env("SPARK_YARN").getOrElse("true").toBoolean

  // Target JVM version
  val SCALAC_JVM_VERSION = "jvm-1.8"
  val JAVAC_JVM_VERSION = "1.8"
  val theScalaVersion = "2.10.3"
        val majorScalaVersion = theScalaVersion.split(".[0-9]+$")(0)
  val targetDir = "target/scala-" + majorScalaVersion // to help mvn and sbt share the same target dir

  val rootOrganization = "io"
  val projectName = "ddf"
  val rootProjectName = projectName
  val rootVersion = "1.4.14-SNAPSHOT"
  //val rootVersion = if(YARN_ENABLED) {
  //  "1.2-adatao"
  //} else {
  //  "1.2-mesos"
  //}

  val projectOrganization = rootOrganization + "." + projectName

  val coreProjectName = "ddf_core"
  val coreVersion = rootVersion
  val coreJarName = coreProjectName.toLowerCase + "_" + theScalaVersion + "-" + coreVersion + ".jar"
  val coreTestJarName = coreProjectName + "-" + coreVersion + "-tests.jar"

  val sparkProjectName = "ddf_spark"
  val sparkVersion = rootVersion

  val s3ProjectName = "ddf_s3"
  val s3Version = rootVersion

  val hdfsProjectName = "ddf_hdfs"
  val hdfsVersion = rootVersion
  
  val testProjectName = "ddf_test"
  val testVersion = rootVersion

//  val sparkVersion = if(YARN_ENABLED) {
//    rootVersion
//  } else {
//    rootVersion + "-mesos"
//  }
  val sparkJarName = sparkProjectName.toLowerCase + "_" + theScalaVersion + "-" + rootVersion + ".jar"
  val sparkTestJarName = sparkProjectName.toLowerCase + "_" + theScalaVersion + "-" + rootVersion + "-tests.jar"
  

  val examplesProjectName = projectName + "_examples"
  val examplesVersion = rootVersion
  val examplesJarName = examplesProjectName + "-" + rootVersion + ".jar"
  val examplesTestJarName = examplesProjectName + "-" + rootVersion + "-tests.jar"


  // lazy val root = Project("root", file("."), settings = rootSettings) aggregate(core, spark, examples)
  lazy val root = Project("root", file("."), settings = rootSettings) aggregate(core, spark, s3, hdfs, examples, test_ddf)
  lazy val core = Project("core", file("core"), settings = coreSettings)
  lazy val test_ddf = Project("ddf-test", file("ddf-test"), settings = testSettings) dependsOn (core)
  lazy val spark = Project("spark", file("spark"), settings = sparkSettings) dependsOn (test_ddf % "test") dependsOn (core, s3, hdfs)
  lazy val examples = Project("examples", file("examples"), settings = examplesSettings) dependsOn (spark) dependsOn (core)
  lazy val s3 = Project("s3", file("s3"), settings = s3Settings) dependsOn (core)
  lazy val hdfs = Project("hdfs", file("hdfs"), settings = hdfsSettings) dependsOn(core)
  // A configuration to set an alternative publishLocalConfiguration
  lazy val MavenCompile = config("m2r") extend(Compile)
  lazy val publishLocalBoth = TaskKey[Unit]("publish-local", "publish local for m2 and ivy")


  //////// Variables/flags ////////

  // Hadoop version to build against. For example, "0.20.2", "0.20.205.0", or
  // "1.0.4" for Apache releases, or "0.20.2-cdh3u5" for Cloudera Hadoop.
  val HADOOP_VERSION = "1.0.4"
  val HADOOP_MAJOR_VERSION = "0"

  // For Hadoop 2 versions such as "2.0.0-mr1-cdh4.1.1", set the HADOOP_MAJOR_VERSION to "2"
  //val HADOOP_VERSION = "2.0.0-mr1-cdh4.1.1"
  //val HADOOP_MAJOR_VERSION = "2"

  val slf4jVersion = "1.7.2"
  val excludeAvro = ExclusionRule(organization = "org.apache.avro" , name = "avro-ipc")
  val excludeJacksonCore = ExclusionRule(organization = "org.codehaus.jackson", name = "jackson-core-asl")
  val excludeJacksonMapper = ExclusionRule(organization = "org.codehaus.jackson", name = "jackson-mapper-asl")
  val excludeNetty = ExclusionRule(organization = "org.jboss.netty", name = "netty")
  val excludeScala = ExclusionRule(organization = "org.scala-lang", name = "scala-library")
  val excludeGuava = ExclusionRule(organization = "com.google.guava", name = "guava-parent")
  val excludeAsm = ExclusionRule(organization = "asm", name = "asm")
  val excludeSpark = ExclusionRule(organization = "org.apache.spark", name = "spark-core_2.10")
  val excludeEverthing = ExclusionRule(organization = "*", name = "*")
  val excludeEverythingHackForMakePom = ExclusionRule(organization = "_MAKE_POM_EXCLUDE_ALL_", name = "_MAKE_POM_EXCLUDE_ALL_")

  // We define this explicitly rather than via unmanagedJars, so that make-pom will generate it in pom.xml as well
  // org % package % version

  val rforge = Seq(
    "net.rforge" % "REngine" % "2.1.1.compiled",
    "net.rforge" % "Rserve" % "1.8.2.compiled"
  )

  val scalaArtifacts = Seq("jline", "scala-compiler", "scala-library", "scala-reflect")
  val scalaDependencies = scalaArtifacts.map( artifactId => "org.scala-lang" % artifactId % theScalaVersion)

  val spark_dependencies = Seq(
    "io.ddf" % "ddf_hdfs_2.10" % rootVersion exclude("javax.servlet", "servlet-api"),
    "com.databricks" % "spark-csv_2.10" % "1.4.0",
    "com.databricks" % "spark-avro_2.10" % "2.0.1",
    "commons-configuration" % "commons-configuration" % "1.6",
    "com.google.code.gson"% "gson" % "2.2.2",
    "com.novocode" % "junit-interface" % "0.10" % "test",
    "net.sf" % "jsqlparser" % "0.9.8.8",
    "org.jblas" % "jblas" % "1.2.3", // for fast linear algebra
    //"org.apache.derby" % "derby" % "10.4.2.0",
   // "org.apache.spark" % "spark-streaming_2.10" % SPARK_VERSION excludeAll(excludeSpark),
    "org.apache.spark" % "spark-core_2.10" % SPARK_VERSION  exclude("net.java.dev.jets3t", "jets3t") exclude("com.google.protobuf", "protobuf-java") exclude ("com.google.code.findbugs", "jsr305")
      exclude("org.jboss.netty", "netty") exclude("org.mortbay.jetty", "jetty"),
    //"org.apache.spark" % "spark-repl_2.10" % SPARK_VERSION excludeAll(excludeSpark) exclude("com.google.protobuf", "protobuf-java") exclude("io.netty", "netty-all") exclude("org.jboss.netty", "netty"),
    "org.apache.spark" % "spark-mllib_2.10" % SPARK_VERSION excludeAll(excludeSpark) exclude("io.netty", "netty-all"),
    "org.apache.spark" % "spark-sql_2.10" % SPARK_VERSION exclude("io.netty", "netty-all")
      exclude("org.jboss.netty", "netty") exclude("org.mortbay.jetty", "jetty"),
    "org.apache.spark" % "spark-hive_2.10" % SPARK_VERSION exclude("io.netty", "netty-all") exclude ("com.google.code.findbugs", "jsr305")
      exclude("org.jboss.netty", "netty") exclude("org.mortbay.jetty", "jetty") exclude("org.mortbay.jetty", "servlet-api"),
    //"org.apache.spark" % "spark-yarn_2.10" % SPARK_VERSION exclude("io.netty", "netty-all")
    "com.google.protobuf" % "protobuf-java" % "2.5.0",
    "org.apache.hadoop" % "hadoop-aws" % "2.7.2" exclude("com.amazonaws", "aws-java-sdk") exclude("com.fasterxml.jackson.core", "jackson-annotations"),
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
  )
  
  val s3_dependencies = Seq(
    "com.amazonaws" % "aws-java-sdk" % "1.10.8",
    "org.apache.hadoop" % "hadoop-common" % "2.7.2" exclude("org.mortbay.jetty", "servlet-api") exclude("commons-httpclient", "commons-httpclient") exclude ("org.apache.httpcomponents", "httpcore"),
   "org.apache.httpcomponents" % "httpcore" % "4.4.1"
  )

  val hdfs_dependencies = Seq(
    "org.apache.hadoop" % "hadoop-hdfs" % "2.2.0"
  )

  val test_dependencies = Seq(
    "org.pegdown" % "pegdown" % "1.6.0",
    "org.scalatest" % "scalatest_2.10" % "2.1.5"
  )

  /////// Common/Shared project settings ///////
  def commonSettings = Defaults.defaultSettings ++ Seq(
    organization := projectOrganization,
    version := rootVersion,
    scalaVersion := theScalaVersion,
    scalacOptions := Seq("-unchecked", "-optimize", "-deprecation"),
    retrieveManaged := true, // Do create a lib_managed, so we have one place for all the dependency jars to copy to slaves, if needed
    retrievePattern := "[type]s/[artifact](-[revision])(-[classifier]).[ext]",
    transitiveClassifiers in Scope.GlobalScope := Seq("sources"),

    // Fork new JVMs for tests and set Java options for those
    fork in Test := true,
    parallelExecution in ThisBuild := false,
    javaOptions in Test ++= Seq("-Xmx2g"),

    // Only allow one test at a time, even across projects, since they run in the same JVM
    concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),

    conflictManager := ConflictManager.strict,

    // This goes first for fastest resolution. We need this for rforge. 
    // Now, sometimes missing .jars in ~/.m2 can lead to sbt compile errors.
    // In that case, clean up the ~/.m2 local repository using bin/clean-m2-repository.sh
    
    resolvers ++= Seq(
      "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
      //"Local ivy Repository" at "file://"+Path.userHome.absolutePath+"/.ivy2/local",
      "Adatao Mvnrepos Snapshots" at "https://raw.github.com/adatao/mvnrepos/master/snapshots",
      "Adatao Mvnrepos Releases" at "https://raw.github.com/adatao/mvnrepos/master/releases",
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
    ),


    publishMavenStyle := true, // generate pom.xml with "sbt make-pom"

    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "commons-configuration" % "commons-configuration" % "1.6",
      "com.google.guava" % "guava" % "14.0.1",
      "com.google.code.gson"% "gson" % "2.2.2",
      "org.scalatest" % "scalatest_2.10" % "2.1.5" % "test",
      "org.scalacheck"   %% "scalacheck" % "1.11.3" % "test",
      "com.novocode" % "junit-interface" % "0.10" % "test",	
      "org.jblas" % "jblas" % "1.2.3", // for fast linear algebra
      "com.googlecode.matrix-toolkits-java" % "mtj" % "0.9.14",
      "net.sf" % "jsqlparser" % "0.9.8.8", 
      "commons-io" % "commons-io" % "1.3.2",
      "org.easymock" % "easymock" % "3.1" % "test",
      "mysql" % "mysql-connector-java" % "5.1.25",
      "org.python" % "jython-standalone" % "2.7.0",
      "joda-time" % "joda-time" % "2.8.1",
      "org.joda" % "joda-convert" % "1.7"
    ),


    otherResolvers := Seq(Resolver.file("dotM2", file(Path.userHome + "/.m2/repository"))),

    publishLocalConfiguration in MavenCompile <<= (packagedArtifacts, deliverLocal, ivyLoggingLevel) map {
      (arts, _, level) => new PublishConfiguration(None, "dotM2", arts, Seq(), level)
    },
    publishMavenStyle in MavenCompile := true,
    publishLocal in MavenCompile <<= publishTask(publishLocalConfiguration in MavenCompile, deliverLocal),
    publishLocalBoth <<= Seq(publishLocal in MavenCompile, publishLocal).dependOn,
    publishArtifact in (Compile, packageDoc) := false,

    dependencyOverrides += "commons-lang" % "commons-lang" % "2.6",
    dependencyOverrides += "it.unimi.dsi" % "fastutil" % "6.4.4",
    dependencyOverrides += "log4j" % "log4j" % "1.2.17",
    dependencyOverrides += "org.slf4j" % "slf4j-api" % slf4jVersion,
    dependencyOverrides += "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
    dependencyOverrides += "commons-io" % "commons-io" % "2.4", //tachyon 0.2.1
    dependencyOverrides += "org.apache.httpcomponents" % "httpclient" % "4.4.1", //libthrift
    dependencyOverrides += "com.google.guava" % "guava" % "14.0.1", //spark-core
    dependencyOverrides += "org.codehaus.jackson" % "jackson-core-asl" % "1.8.8",
    dependencyOverrides += "org.codehaus.jackson" % "jackson-mapper-asl" % "1.8.8",
    dependencyOverrides += "org.codehaus.jackson" % "jackson-xc" % "1.8.8",
    dependencyOverrides += "org.codehaus.jackson" % "jackson-jaxrs" % "1.8.8",
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4",
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.4.4",
    dependencyOverrides += "com.thoughtworks.paranamer" % "paranamer" % "2.4.1", //net.liftweb conflict with avro
    dependencyOverrides += "org.xerial.snappy" % "snappy-java" % "1.0.5", //spark-core conflicts with avro
    dependencyOverrides += "org.apache.httpcomponents" % "httpcore" % "4.4.1",
    dependencyOverrides += "org.apache.avro" % "avro-ipc" % "1.7.4",
    dependencyOverrides += "org.apache.avro" % "avro" % "1.7.4",
    dependencyOverrides += "org.apache.zookeeper" % "zookeeper" % "3.4.5",
    dependencyOverrides += "org.scala-lang" % "scala-compiler" % "2.10.3",
    dependencyOverrides += "io.netty" % "netty" % "3.6.6.Final",
    dependencyOverrides += "org.ow2.asm" % "asm" % "4.0", //org.datanucleus#datanucleus-enhancer's
    dependencyOverrides += "asm" % "asm" % "3.2",
    dependencyOverrides += "commons-codec" % "commons-codec" % "1.4",
    dependencyOverrides += "org.scala-lang" % "scala-actors" % "2.10.1",
    dependencyOverrides += "org.scala-lang" % "scala-library" %"2.10.3",
    dependencyOverrides += "org.scala-lang" % "scala-reflect" %"2.10.3",
    dependencyOverrides += "com.sun.jersey" % "jersey-core" % "1.9",
    dependencyOverrides += "javax.xml.bind" % "jaxb-api" % "2.2.2",
    dependencyOverrides += "commons-collections" % "commons-collections" % "3.2.1",
    dependencyOverrides += "commons-logging" % "commons-logging" % "1.1.3",
    dependencyOverrides += "commons-net" % "commons-net" % "3.1",
    dependencyOverrides += "org.mockito" % "mockito-all" % "1.8.5",
    dependencyOverrides += "org.apache.commons" % "commons-math3" % "3.1.1",
    dependencyOverrides += "commons-httpclient" % "commons-httpclient" % "3.1",
    dependencyOverrides += "com.sun.jersey" % "jersey-json" % "1.9",
    dependencyOverrides += "com.sun.jersey" % "jersey-server" % "1.9",
    dependencyOverrides += "org.scalamacros" % "quasiquotes_2.10" % "2.0.0",
    dependencyOverrides += "commons-httpclient" % "commons-httpclient" % "3.1",
    dependencyOverrides += "org.apache.avro" % "avro-mapred" % "1.7.6",
    dependencyOverrides += "commons-logging" % "commons-logging" % "1.1.3",
    dependencyOverrides += "net.java.dev.jets3t" % "jets3t" % "0.7.1",
    dependencyOverrides += "com.google.code.gson"% "gson" % "2.3.1",
    dependencyOverrides += "com.sun.xml.bind" % "jaxb-impl" % "2.2.7",
    dependencyOverrides += "jline" % "jline" % "2.12",
    dependencyOverrides += "joda-time" % "joda-time" % "2.8.1",
    dependencyOverrides += "io.dropwizard.metrics" % "metrics-core" % "3.1.2",
    dependencyOverrides += "io.dropwizard.metrics" % "metrics-json" % "3.1.2",
    dependencyOverrides += "io.dropwizard.metrics" % "metrics-jvm" % "3.1.2",
    dependencyOverrides += "org.apache.commons" % "commons-lang3" % "3.1",
      pomExtra := (
      <!--
      **************************************************************************************************
      IMPORTANT: This file is generated by "sbt make-pom" (bin/make-poms.sh). Edits will be overwritten!
      **************************************************************************************************
      -->
        <parent>
          <groupId>{rootOrganization}</groupId>
          <artifactId>{rootProjectName}</artifactId>
          <version>{rootVersion}</version>
        </parent>
        <build>
          <directory>${{basedir}}/{targetDir}</directory>
          <plugins>
            <plugin>
              <!-- Let SureFire know where the jars are -->
              <groupId>org.apache.maven.plugins</groupId>
              <artifactId>maven-surefire-plugin</artifactId>
              <version>2.15</version>
              <configuration>
                <reuseForks>false</reuseForks>
	<enableAssertions>false</enableAssertions>
        <environmentVariables>
		 <RSERVER_JAR>${{basedir}}/{targetDir}/*.jar,${{basedir}}/{targetDir}/lib/*</RSERVER_JAR>
	</environmentVariables> 
                <systemPropertyVariables>
                  <spark.serializer>org.apache.spark.serializer.KryoSerializer</spark.serializer>
                  <spark.kryo.registrator>io.ddf.spark.content.KryoRegistrator</spark.kryo.registrator>
                  <spark.ui.port>8085</spark.ui.port>
                  <log4j.configuration>ddf-local-log4j.properties</log4j.configuration>
                  <derby.stream.error.file>${{basedir}}/target/derby.log</derby.stream.error.file>
                </systemPropertyVariables>
                <additionalClasspathElements>
                  <additionalClasspathElement>${{basedir}}/conf/</additionalClasspathElement>
                  <additionalClasspathElement>${{basedir}}/conf/local/</additionalClasspathElement>
                  <additionalClasspathElement>${{basedir}}/../lib_managed/jars/*</additionalClasspathElement>
                  </additionalClasspathElements>
                <includes>
                  <include>**/*.java</include>
                </includes>
              </configuration>
            </plugin>
            <plugin>
              <groupId>org.apache.maven.plugins</groupId>
              <artifactId>maven-assembly-plugin</artifactId>
              <version>2.2.2</version>
              <configuration>
                <descriptors>
                  <descriptor>assembly.xml</descriptor>
                </descriptors>
              </configuration>
            </plugin>
            <plugin>
              <groupId>org.apache.maven.plugins</groupId>
              <artifactId>maven-jar-plugin</artifactId>
              <version>2.2</version>
              <executions>
                <execution>
                  <goals><goal>test-jar</goal></goals>
                </execution>
              </executions>
            </plugin>
	    <plugin>
		 <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-dependency-plugin</artifactId>
        <version>2.10</version>
           </plugin>
            <plugin>
              <groupId>net.alchim31.maven</groupId>
              <artifactId>scala-maven-plugin</artifactId>
              <version>3.2.0</version>
              <configuration>
                <recompileMode>incremental</recompileMode>
              </configuration>
            </plugin>
            <plugin>
              <groupId>org.apache.maven.plugins</groupId>
              <artifactId>maven-checkstyle-plugin</artifactId>
              <version>2.6</version>
              <configuration>
                <configLocation>${{basedir}}/../src/main/resources/sun_checks.xml</configLocation>
                <propertyExpansion>checkstyle.conf.dir=${{basedir}}/../src/main/resources</propertyExpansion>
                <outputFileFormat>xml</outputFileFormat>
              </configuration>
            </plugin>
            
          </plugins>
        </build>
        <profiles>

          <profile>
            <id>local</id>
            <activation><property><name>!dist</name></property>
		<activeByDefault>true</activeByDefault>
	    </activation>
            <build>
              <directory>${{basedir}}/{targetDir}</directory>
              <plugins>
                <plugin>
                  <groupId>org.apache.maven.plugins</groupId>
                  <artifactId>maven-surefire-plugin</artifactId>
                  <version>2.15</version>
                  <configuration>
                    <additionalClasspathElements>
                      <additionalClasspathElement>${{basedir}}/conf/local</additionalClasspathElement>
                    </additionalClasspathElements>
                  </configuration>
                </plugin>
              </plugins>
            </build>
          </profile>

          <profile>
            <id>distributed</id>
            <activation><property><name>dist</name></property></activation>
            <build>
              <directory>${{basedir}}/{targetDir}</directory>
              <plugins>
                <plugin>
                  <!-- Let SureFire know where the jars are -->
                  <groupId>org.apache.maven.plugins</groupId>
                  <artifactId>maven-surefire-plugin</artifactId>
                  <version>2.15</version>
                  <configuration>
                    <additionalClasspathElements>
                      <additionalClasspathElement>${{basedir}}/../lib_managed/jars/*</additionalClasspathElement>
                      <additionalClasspathElement>${{basedir}}/conf/distributed/</additionalClasspathElement>
                      <additionalClasspathElement>${{HADOOP_HOME}}/conf/</additionalClasspathElement>
                      <additionalClasspathElement>${{HIVE_HOME}}/conf/</additionalClasspathElement>
                    </additionalClasspathElements>
                  </configuration>
                </plugin>
              </plugins>
            </build>
          </profile>

          <profile>
            <id>nospark</id>
            <activation><property><name>nospark</name></property></activation>
            <build>
              <directory>${{basedir}}/{targetDir}</directory>
              <plugins>
                <plugin>
                  <!-- Let SureFire know where the jars are -->
                  <groupId>org.apache.maven.plugins</groupId>
                  <artifactId>maven-surefire-plugin</artifactId>
                  <version>2.15</version>
                  <configuration>
                    <additionalClasspathElements>
                      <additionalClasspathElement>${{basedir}}/conf/local</additionalClasspathElement>
                    </additionalClasspathElements>
                    <includes><include>**</include></includes>
                    <excludes><exclude>**/spark/**</exclude></excludes>
                  </configuration>
                </plugin>
              </plugins>
            </build>
          </profile>

          <profile>
            <id>package</id>
            <activation><property><name>package</name></property></activation>
            <build>
              <directory>${{basedir}}/{targetDir}</directory>
              <plugins>
                <plugin>
                  <!-- Let SureFire know where the jars are -->
                  <groupId>org.apache.maven.plugins</groupId>
                  <artifactId>maven-surefire-plugin</artifactId>
                  <version>2.15</version>
                  <configuration>
                    <additionalClasspathElements>
                      <additionalClasspathElement>${{basedir}}/conf/local</additionalClasspathElement>
                    </additionalClasspathElements>
                    <includes><include>**/${{path}}/**</include></includes>
                  </configuration>
                </plugin>
              </plugins>
            </build>
          </profile>
        </profiles>
      )

  ) // end of commonSettings


  /////// Individual project settings //////
  def rootSettings = commonSettings ++ Seq(publish := {})

  def coreSettings = commonSettings ++ Seq(
    name := coreProjectName,
    //javaOptions in Test <+= baseDirectory map {dir => "-Dspark.classpath=" + dir + "/../lib_managed/jars/*"},
    // Add post-compile activities: touch the maven timestamp files so mvn doesn't have to compile again
    compile in Compile <<= compile in Compile andFinally { List("sh", "-c", "touch core/" + targetDir + "/*timestamp") },
    libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.7.2",
    libraryDependencies += "com.google.code.findbugs" % "jsr305" % "2.0.1",
    libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.2.0" exclude("org.mortbay.jetty", "servlet-api")
      exclude("javax.servlet", "servlet-api"),
    libraryDependencies += "org.jgrapht" % "jgrapht-core" % "0.9.0",
    libraryDependencies ++= scalaDependencies,
    testOptions in Test += Tests.Argument("-oI")
  ) ++ assemblySettings ++ extraAssemblySettings

  val java_opts = if(System.getenv("JAVA_OPTS") != null) {
    System.getenv("JAVA_OPTS").split(" ").filter(x => x.startsWith("-D")).map {
      s => s.stripPrefix("-D")
    }.map(x => x.split("=")).filter(x => x.size > 1).map(x => (x(0), x(1)))
  } else {
    Array[(String, String)]()
  }
  val isLocal = scala.util.Properties.envOrElse("SPARK_MASTER", "local").contains("local")
  val getEnvCommand = java_opts.map{
    case (key, value) => "System.setProperty(\"%s\", \"%s\")".format(key, value)
  }.mkString("\n|")

  def sparkSettings = commonSettings ++ Seq(
    name := sparkProjectName,
    javaOptions in Test <+= baseDirectory map {dir => "-Dspark.classpath=" + dir + "/../lib_managed/jars/*"},
    // Add post-compile activities: touch the maven timestamp files so mvn doesn't have to compile again
    compile in Compile <<= compile in Compile andFinally { List("sh", "-c", "touch spark/" + targetDir + "/*timestamp") },
    resolvers ++= Seq(
      //"JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
      //"Spray Repository" at "http://repo.spray.cc/",
      //"Twitter4J Repository" at "http://twitter4j.org/maven2/"
      //"Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
    ),
    testOptions in Test += Tests.Argument("-oI"),
    libraryDependencies ++= rforge,
    libraryDependencies ++= spark_dependencies,
    if(isLocal) {
      initialCommands in console :=
        s"""
        |$getEnvCommand
        |import io.ddf.DDFManager
        |val manager = DDFManager.get("spark")
        |manager.sql2txt("drop table if exists airline")
        |manager.sql2txt("create external table airline (Year int,Month int,DayofMonth int,DayOfWeek int, " +
        |"aDepTime int,CRSDepTime int,ArrTime int,CRSArrTime int,UniqueCarrier string, " +
        |"FlightNum int, TailNum string, ActualElapsedTime int, CRSElapsedTime int, AirTime int, " +
        |"ArrDelay int, DepDelay int, Origin string, Dest string, Distance int, TaxiIn int, TaxiOut int, " +
        |"Cancelled int, CancellationCode string, Diverted string, CarrierDelay int, WeatherDelay int, " +
        |"NASDelay int, SecurityDelay int, LateAircraftDelay int ) " +
        |"ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
        |manager.sql2txt("load data local inpath 'resources/test/airlineBig.csv' into table airline")
        |println("SparkDDFManager is available as the DDF manager")""".stripMargin
    } else {
      initialCommands in console :=
        s"""
           |$getEnvCommand
           |import io.ddf.DDFManager
           |val manager = DDFManager.get("spark")
           |println("SparkDDFManager is available as the DDF manager")
         """.stripMargin
    }
  ) ++ assemblySettings ++ extraAssemblySettings

  def examplesSettings = commonSettings ++ Seq(
    name := examplesProjectName,
    //javaOptions in Test <+= baseDirectory map {dir => "-Dspark.classpath=" + dir + "/../lib_managed/jars/*"},
    // Add post-compile activities: touch the maven timestamp files so mvn doesn't have to compile again
    compile in Compile <<= compile in Compile andFinally { List("sh", "-c", "touch examples/" + targetDir + "/*timestamp") }
  ) ++ assemblySettings ++ extraAssemblySettings

  def s3Settings = commonSettings ++ Seq(
    name := s3ProjectName,
    compile in Compile <<= compile in Compile andFinally { List("sh", "-c", "touch s3/" + targetDir + "/*timestamp") },
    testOptions in Test += Tests.Argument("-oI"),
    libraryDependencies ++= s3_dependencies
  ) ++ assemblySettings ++ extraAssemblySettings

  def hdfsSettings = commonSettings ++ Seq(
    name := hdfsProjectName,  
    compile in Compile <<= compile in Compile andFinally { List("sh", "-c", "touch hdfs/" + targetDir + "/*timestamp") },
    testOptions in Test += Tests.Argument("-oI"),
    libraryDependencies ++= hdfs_dependencies
  ) ++ assemblySettings ++ extraAssemblySettings
  
  def testSettings = commonSettings ++ Seq(
    name := testProjectName,
    libraryDependencies ++= test_dependencies,
    scalacOptions := Seq("-unchecked", "-optimize", "-deprecation"),
    parallelExecution in ThisBuild := false,
    fork in Test := true,
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports"),
    parallelExecution in Test := false,
    publishArtifact in(Test, packageBin) := true
  ) ++ assemblySettings ++ extraAssemblySettings

  def extraAssemblySettings() = Seq(test in assembly := {}) ++ Seq(
    mergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.endsWith("eclipsef.sf") => MergeStrategy.discard
      case m if m.toLowerCase.endsWith("eclipsef.rsa") => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )
}
