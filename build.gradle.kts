import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm") version "2.3.21"
    kotlin("plugin.serialization") version "2.3.21"
    id("com.vanniktech.maven.publish") version "0.37.0"
}

repositories {
    mavenCentral()
}

dependencies {
    // `api`, not `compileOnly`: Kord types appear in this library's public signatures, and the
    // published POM then carries the Kord floor the code was compiled and tested against.
    api("dev.kord:kord-core:0.18.1")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

kotlin {
    explicitApi()
    compilerOptions {
        jvmTarget = JvmTarget.JVM_17
        freeCompilerArgs.add("-Xjdk-release=17")
        allWarningsAsErrors = true
    }
}

tasks.test {
    useJUnitPlatform()
    systemProperty("tdl.version", version.toString())
    testLogging {
        events("passed", "skipped", "failed")
    }
}

mavenPublishing {
    publishToMavenCentral()
    // Local and CI builds publish unsigned; release.yml supplies the key, and Central rejects unsigned uploads.
    if (providers.gradleProperty("signingInMemoryKey").isPresent) {
        signAllPublications()
    }
    coordinates(group.toString(), "integration-kord", version.toString())

    pom {
        name = "TheDiscordList for Kord"
        description = "Official TheDiscordList integration for Kord: auto-post server count, " +
            "shard heartbeats and slash commands, and verify inbound webhooks."
        inceptionYear = "2026"
        url = "https://thediscordlist.com"
        licenses {
            license {
                name = "MIT License"
                url = "https://opensource.org/licenses/MIT"
                distribution = "repo"
            }
        }
        developers {
            developer {
                id = "thediscordlist"
                name = "TheDiscordList"
                url = "https://thediscordlist.com"
            }
        }
        scm {
            url = "https://github.com/TheDiscordList/integration-kord"
            connection = "scm:git:git://github.com/TheDiscordList/integration-kord.git"
            developerConnection = "scm:git:ssh://git@github.com/TheDiscordList/integration-kord.git"
        }
    }
}
