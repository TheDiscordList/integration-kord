package com.thediscordlist.kord

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** Holds this SDK to the fixture every TheDiscordList SDK shares. The fixture is copied verbatim; never edit it here. */
class ConformanceTest {
    private val fixture = Json.parseToJsonElement(
        checkNotNull(javaClass.getResourceAsStream("/sdk-conformance.json")).use { it.readBytes().decodeToString() },
    )

    private fun JsonElement.at(vararg path: String) = path.fold(this) { node, key -> node.jsonObject.getValue(key) }

    private fun JsonElement.text() = jsonPrimitive.content

    @Test
    fun `every webhook vector verifies as the fixture says`() {
        val webhooks = fixture.at("webhook_signature")
        val secret = webhooks.at("secret").text()

        for (vector in webhooks.at("vectors").jsonArray) {
            val name = vector.at("name").text()
            val code = try {
                verifyWebhook(vector.at("body").text(), vector.at("header").text(), secret, nowEpochSeconds = vector.at("now").jsonPrimitive.long)
                null
            } catch (e: TdlWebhookException) {
                e.code
            }
            if (vector.at("valid").jsonPrimitive.boolean) {
                // The fixture's bodies are a bare event and data, and this SDK types the whole envelope,
                // so a signature that verifies can still end at the parse that follows it.
                assertTrue(code == null || code == WebhookErrorCode.MALFORMED_BODY, "$name: $code")
            } else {
                assertNotNull(code, name)
                assertNotEquals(WebhookErrorCode.MALFORMED_BODY, code, name)
            }
        }
    }

    @Test
    fun `every default matches the fixture`() {
        val defaults = fixture.at("defaults")
        assertEquals(defaults.at("base_url").text(), DEFAULT_BASE_URL)
        assertEquals(defaults.at("retry", "attempts").jsonPrimitive.long, MAX_ATTEMPTS.toLong())

        val stats = StatsOptions()
        mapOf(
            listOf("timeout_seconds") to REQUEST_TIMEOUT.inWholeMilliseconds,
            listOf("retry", "backoff_base_seconds") to BACKOFF_BASE_MS,
            listOf("retry", "backoff_max_seconds") to BACKOFF_MAX_MS,
            listOf("rate_limit", "default_seconds") to DEFAULT_RETRY_AFTER_MS,
            listOf("rate_limit", "max_seconds") to MAX_RETRY_AFTER_MS,
            listOf("rate_limit", "jitter_max_seconds") to RATE_LIMIT_JITTER_MS,
            listOf("forbidden_reprobe_seconds") to PLAN_REPROBE_MS,
            listOf("stats", "first_delay_seconds") to STATS_FIRST_POST_DELAY_MS,
            listOf("stats", "interval_seconds") to stats.interval.inWholeMilliseconds,
            listOf("stats", "debounce_seconds") to stats.debounce.inWholeMilliseconds,
            listOf("stats", "min_gap_seconds") to stats.minGap.inWholeMilliseconds,
            listOf("stats", "force_after_seconds") to STATS_FORCE_AFTER_MS,
            listOf("heartbeat", "interval_seconds") to HeartbeatOptions().interval.inWholeMilliseconds,
            listOf("heartbeat", "jitter_max_seconds") to HEARTBEAT_JITTER_MS,
            listOf("commands", "first_delay_seconds") to COMMANDS_DELAY_MS,
            listOf("commands", "retry_seconds") to COMMANDS_RETRY_MS,
        ).forEach { (path, actual) ->
            assertEquals(defaults.at(*path.toTypedArray()).jsonPrimitive.long * 1000, actual, path.joinToString("."))
        }
    }

    @Test
    fun `shard states match the fixture`() {
        assertEquals(fixture.at("shard_states").jsonArray.map { it.text() }, ShardStatus.entries.map { it.wire })
    }
}
