plugins {
    `maven-publish`
    id("java")
    id("idea")
    id("com.github.vlsi.gradle-extensions") version "1.74"
    id("com.diffplug.spotless") version "6.19.0"
    id("io.github.gradle-nexus.publish-plugin") version "1.1.0"
    id("org.cyclonedx.bom") version "1.8.2"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

// Define version constants
val CALCITE_VERSION = property("calcite.version") as String
val GUAVA_VERSION = property("guava.version") as String
val IMMUTABLES_VERSION = property("immutables.version") as String
val JACKSON_VERSION = property("jackson.version") as String
val JUNIT_VERSION = property("junit.version") as String
val SLF4J_VERSION = property("slf4j.version") as String
val PROTOBUF_VERSION = property("protobuf.version") as String
val ANTLR_VERSION = property("antlr.version") as String

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

dependencies {
    // Jackson
    implementation("com.fasterxml.jackson.core:jackson-databind:${JACKSON_VERSION}")
    implementation("com.fasterxml.jackson.core:jackson-annotations:${JACKSON_VERSION}")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:${JACKSON_VERSION}")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:${JACKSON_VERSION}")

    // Google libraries
    implementation("com.google.guava:guava:${GUAVA_VERSION}")
    implementation("com.google.code.findbugs:jsr305:3.0.2")
    implementation("com.google.protobuf:protobuf-java-util:${PROTOBUF_VERSION}") {
        exclude("com.google.guava", "guava")
                .because("Brings in Guava for Android, which we don't want (and breaks multimaps).")
    }
    testImplementation("com.google.protobuf:protobuf-java:${PROTOBUF_VERSION}")

    // Calcite
    implementation("org.apache.calcite:calcite-server:1.39.0")
    testImplementation("org.apache.calcite:calcite-core:1.39.0")
    testImplementation("org.apache.calcite:calcite-plus:1.39.0")

    implementation("org.apache.beam:beam-sdks-java-core:2.56.0")
    implementation("org.apache.beam:beam-sdks-java-extensions-sql:2.56.0")
    // Runner: Direct Runner (use Flink or Spark if you prefer)
    implementation("org.apache.beam:beam-runners-direct-java:2.56.0")

    // (Optional) I/O connectors
    implementation("org.apache.beam:beam-sdks-java-io-google-cloud-platform:2.56.0")

    // (Optional) Beam examples (for reference)

    implementation("io.substrait:core:0.36.0")
    implementation("io.substrait:isthmus:0.36.0")
    // Fix for CVE in json-smart transitive dependency
    implementation("net.minidev:json-smart:2.5.2")

    // Immutables for code generation
    implementation("org.immutables:value-annotations:${IMMUTABLES_VERSION}")
    annotationProcessor("org.immutables:value:${IMMUTABLES_VERSION}")
    runtimeOnly("org.apache.arrow:arrow-memory-netty:18.3.0")
    implementation("org.apache.arrow:arrow-memory-core:18.3.0")
    // https://mvnrepository.com/artifact/org.apache.arrow/arrow-flight
    // https://mvnrepository.com/artifact/org.apache.arrow/flight-core
    implementation("org.apache.arrow:flight-core:14.0.1")
    implementation("org.apache.arrow:arrow-flight:14.0.1")

    // Utilities
    implementation("org.reflections:reflections:0.9.12")
    implementation("com.github.ben-manes.caffeine:caffeine:3.0.4")
    implementation("org.slf4j:slf4j-api:${SLF4J_VERSION}")

    implementation("net.bytebuddy:byte-buddy:1.14.9")
    implementation("net.bytebuddy:byte-buddy-agent:1.14.9")

    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter-api:${JUNIT_VERSION}")
    testImplementation("org.junit.jupiter:junit-jupiter-params:${JUNIT_VERSION}")
    testImplementation("org.junit.jupiter:junit-jupiter:${JUNIT_VERSION}")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:${JUNIT_VERSION}")
   // runtimeOnly("ch.qos.logback:logback-classic:1.4.11")
    implementation("net.openhft:chronicle-queue:5.27ea5")
    implementation("org.apache.calcite:calcite-arrow:1.39.0")
}

// Fix: Ensure mainClass is set correctly and the JVM args are applied exactly as required by Arrow
application {
    // Make sure this points to your actual main class including the package name
    mainClass.set("org.example.calciteStreamer")

    applicationDefaultJvmArgs = listOf(
            "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}

tasks.test {
    useJUnitPlatform()
}
