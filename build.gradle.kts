plugins {
    id("java")
    id("application")
    kotlin("jvm") version "2.3.0"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:3.8.0")
    implementation("org.slf4j:slf4j-simple:2.0.13")
    implementation("org.xerial:sqlite-jdbc:3.46.0.0")
    implementation("org.json:json:20240303")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
kotlin {
   jvmToolchain {
       languageVersion.set(JavaLanguageVersion.of(25))
   }
}
application {
    mainClass.set("org.example.kafka.ProducerApp")
}

tasks.test {
    useJUnitPlatform()
}

tasks.register<JavaExec>("producer") {
    group = "kafka"
    description = "Lance le service REST producer (POST /sendMessage) qui alimente Kafka"
    mainClass.set("org.example.kafka.ProducerApp")
    classpath = sourceSets["main"].runtimeClasspath
}

tasks.register<JavaExec>("consumer") {
    group = "kafka"
    description = "Lance le consumer Kafka en ligne de commande"
    mainClass.set("org.example.kafka.ConsumerApp")
    classpath = sourceSets["main"].runtimeClasspath
    standardInput = System.`in`
}

tasks.register<JavaExec>("restApi") {
    group = "kafka"
    description = "Lance le service REST (POST/GET /consumer) adosse a SQLite"
    mainClass.set("org.example.rest.RestServerApp")
    classpath = sourceSets["main"].runtimeClasspath
}
