plugins {
    id("com.hivemq.extension")
    id("com.github.sgtsilvio.gradle.utf8")
    id("org.asciidoctor.jvm.convert")
}

group = "com.bytechaos.mqtt"
description = "Fun purpose only: Enable MQTT Wildcard Publish."

hivemqExtension {
    name.set("MQTT Fun Wilcard Publish")
    author.set("Byte Chaos")
    priority.set(1000)
    startPriority.set(10000)
    sdkVersion.set("${property("hivemq-extension-sdk.version")}")
}

/* ******************** resources ******************** */

val prepareAsciidoc by tasks.registering(Sync::class) {
    from("README.adoc").into({ temporaryDir })
}

tasks.asciidoctor {
    dependsOn(prepareAsciidoc)
    sourceDir(prepareAsciidoc.map { it.destinationDir })
}

hivemqExtension.resources {
    from("LICENSE")
    from("README.adoc") { rename { "README.txt" } }
    from(tasks.asciidoctor)
}

/* ******************** test ******************** */

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter-api:${property("junit-jupiter.version")}")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}

/* ******************** integration test ******************** */

dependencies {
    integrationTestImplementation("org.testcontainers:testcontainers:${property("testcontainers.version")}")
    integrationTestImplementation("com.hivemq:hivemq-testcontainer-junit5:${property("hivemq-testcontainer.version")}")
    integrationTestRuntimeOnly("ch.qos.logback:logback-classic:${property("logback.version")}")
}
