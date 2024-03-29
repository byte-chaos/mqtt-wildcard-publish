rootProject.name = "mqtt-wildcard-publish"

pluginManagement {
    plugins {
        id("com.hivemq.extension") version "${extra["plugin.hivemq-extension.version"]}"
        id("com.github.sgtsilvio.gradle.utf8") version "${extra["plugin.utf8.version"]}"
        id("org.asciidoctor.jvm.convert") version "${extra["plugin.asciidoctor.version"]}"
        id("com.github.hierynomus.license-report") version "${extra["plugin.license-report.version"]}"
    }
}