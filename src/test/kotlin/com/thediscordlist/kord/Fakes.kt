package com.thediscordlist.kord

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlin.time.Duration

internal const val BASE = "https://tdl.test/api"

internal data class Sent(val path: String, val headers: Map<String, String>, val body: JsonObject)

internal class FakeTransport : Transport {
    val requests = mutableListOf<Sent>()
    var respond: (Sent) -> HttpReply = { reply(200) }

    override suspend fun post(url: String, headers: Map<String, String>, body: String): HttpReply {
        val sent = Sent(url.removePrefix(BASE), headers, Json.parseToJsonElement(body).jsonObject)
        requests += sent
        return respond(sent)
    }

    fun to(endpoint: String) = requests.filter { it.path.endsWith("/$endpoint") }
}

internal fun reply(status: Int, body: String = "{}", headers: Map<String, String> = emptyMap()) =
    HttpReply(status, body) { name -> headers.entries.firstOrNull { it.key.equals(name, ignoreCase = true) }?.value }

internal class FakeView(
    override val localShards: List<Int> = listOf(0),
    override val totalShards: Int = localShards.size,
    private val pings: Map<Int, Duration> = emptyMap(),
    var commands: JsonArray = JsonArray(emptyList()),
) : BotView {
    override fun latency(shard: Int): Duration? = pings[shard]
    override suspend fun fetchCommands(): JsonArray = commands
}

internal fun json(text: String): JsonObject = Json.parseToJsonElement(text).jsonObject
