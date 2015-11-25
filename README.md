DDF
===

Distributed DataFrame: Productivity = Power x Simplicity
For Big Data Scientists & Engineers

* [Visit DDF Website!](http://ddf.io)

* [Wiki](https://github.com/ddf-project/DDF/wiki)

* [Issues tracker](https://github.com/ddf-project/DDF/issues)

* [Questions/Comments/Feature Requests](https://groups.google.com/forum/#!forum/ddf-project)

# DDF - Distributed DataFrame 

DDF aims to make Big Data easy yet powerful, by bringing together
the best ideas from R Data Science, RDBMS/SQL, and Big Data distributed
processing.

It exposes high-level abstractions like RDBMS tables,
SQL queries, data cleansing and transformations, machine-learning
algorithms, even collaboration and authentication, etc., while
hiding all the complexities of parallel distributed processing
and data handling.

DDF is a general abstraction that can be implemented on multiple
execution and data engines. We are providing a native implementation
on Apache Spark, as it is today the most expressive in its DAG
parallelization and also most powerful in its in-memory distributed
dataset abstraction (RDD). With this release, DDF provides native
Spark support for R, Python, Java, Scala.

An aim of the DDF project is to shine a focus of Big Data conversations
on top-down, user-focussed simplicity and power, where "users" include
business analysts, data scientists, and high-level Big Data engineers.

---

### Directory Structure

| Directory | Description |
|-----------|-------------|
| bin | useful helper scripts |
| exe | DDF execution/launch scripts and executables |
| conf | DDF configuration files |
| R | DDF in R |
| python | DDF in python |
| core | DDF core API |
| spark | DDF Spark implementation |
| examples | DDF example API-user code |
| project | Scala build config files |

### Getting Started

First clone or fork a copy of DDF, e.g.:

```
$ git clone https://github.com/ddf-project/DDF 
```

Now set up the neccessary environment variables (MAVEN_OPTS, JAVA_TOOL_OPTIONS) for the build.

```
$ cd DDF
$ bin/set-env.sh
```
If you don't want to set up the environment variables every time,
please add the commands into ~/.bashrc or ~/.bash_profile.

If you don't have sbt and want to install, run the following command
```
$ bin/get-sbt.sh
```
This command will install a local sbt under bin/sbt

(Optional) The following regenerates Eclipse .project and .classpath files:

```
$ bin/make-eclipse-projects.sh
```

### Building `DDF_core` or `DDF_spark`
```
$ mvn clean install -DskipTests
```

Or if you want to just build separate modules, you can go into module
directories and build, for example,

```
$ (cd core ; mvn clean install -DskipTests)
$ (cd spark ; mvn clean install -DskipTests)
```

Or if you are more familar with the sbt tool,
```
sbt (bin/sbt) clean compile publishLocal
```
### Running tests
	
```
mvn test
```

Or if you want to run test for individual modules,
```
$ (cd core ; mvn test)
$ (cd spark ; mvn test)
```

Or if you are more familar with the sbt tool,
```
sbt (bin/sbt) test
``` 
