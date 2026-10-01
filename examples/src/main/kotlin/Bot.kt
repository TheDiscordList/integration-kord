// The whole integration, for most bots.
//
//   DISCORD_TOKEN=... TDL_TOKEN=... ./gradlew :examples:run

import com.thediscordlist.kord.TdlEvent
import com.thediscordlist.kord.autopost
import dev.kord.core.Kord
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach

suspend fun main() {
    val kord = Kord(System.getenv("DISCORD_TOKEN"))

    val poster = kord.autopost(System.getenv("TDL_TOKEN"))

    // Entirely optional. Without a collector the package logs failures instead.
    poster.events.onEach { event ->
        when (event) {
            is TdlEvent.Posted -> println("[tdl] ${event.type} -> ${event.status}")
            is TdlEvent.Failed -> println("[tdl] ${event.type} failed: ${event.message}")
            is TdlEvent.Disabled -> println("[tdl] ${event.type} disabled: ${event.reason}")
        }
    }.launchIn(kord)

    kord.login()
}
