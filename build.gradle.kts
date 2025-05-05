plugins {
    `maven-publish`
    id("java")
    id("idea")
    id("com.github.vlsi.gradle-extensions") version "1.74"
    id("com.diffplug.spotless") version "6.19.0"
    id("io.github.gradle-nexus.publish-plugin") version "1.1.0"
    id("org.cyclonedx.bom") version "1.8.2"
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
    implementation("org.apache.calcite:calcite-server:${CALCITE_VERSION}")
    testImplementation("org.apache.calcite:calcite-core:${CALCITE_VERSION}")
    testImplementation("org.apache.calcite:calcite-plus:${CALCITE_VERSION}")

    implementation("io.substrait:core:0.36.0")
    implementation("io.substrait:isthmus:0.36.0")
    // Fix for CVE in json-smart transitive dependency
    implementation("net.minidev:json-smart:2.5.2")

    // Immutables for code generation
    implementation("org.immutables:value-annotations:${IMMUTABLES_VERSION}")
    annotationProcessor("org.immutables:value:${IMMUTABLES_VERSION}")


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
}

tasks.test {
    useJUnitPlatform()
}