name := "LettuceTest"

version := "1.0"

scalaVersion := "2.11.8"

licenses += "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")

resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/"

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += Resolver.mavenLocal

// need to checkout this branch and install locally: https://github.com/ambling/rtree/tree/local_install
libraryDependencies += "com.github.davidmoten" % "rtree" % "0.8-RC11-SNAPSHOT"

libraryDependencies += "biz.paluch.redis" % "lettuce" % "4.3.1.Final" exclude("io.netty", "netty-common") exclude("io.netty", "netty-transport") exclude("io.netty", "netty-handler") exclude("io.netty", "netty-codec")

libraryDependencies += "io.netty" % "netty-transport-native-epoll" % "4.0.42.Final" classifier "linux-x86_64"

libraryDependencies += "io.netty" % "netty-all" % "4.0.42.Final"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

libraryDependencies += "junit" % "junit" % "4.10" % "test"
