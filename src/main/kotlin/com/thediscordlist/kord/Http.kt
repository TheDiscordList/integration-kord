package com.thediscordlist.kord

import kotlinx.coroutines.future.await
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlin.coroutines.cancellation.CancellationException
import kotlin.math.max
import kotlin.math.min
import kotlin.math.roundToLong
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

internal class HttpReply(val status: Int, val body: String, val header: (String) -> String?)

internal fun interface Transport {
    suspend fun post(url: String, headers: Map<String, String>, body: String): HttpReply
}

internal class JdkTransport(private val timeout: Duration = 10.seconds) : Transport {
    private val client by lazy { HttpClient.newBuilder().connectTimeout(timeout.toJavaDuration()).build() }

    override suspend fun post(url: String, headers: Map<String, String>, body: String): HttpReply {
        val request = HttpRequest.newBuilder(URI.create(url))
            .timeout(timeout.toJavaDuration())
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .apply { headers.forEach { (name, value) -> header(name, value) } }
            .build()
        val response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
        return HttpReply(response.statusCode(), response.body()) { response.headers().firstValue(it).orElse(null) }
    }
}

internal sealed interface Outcome {
    data class Ok(val status: Int, val body: JsonElement?) : Outcome
    data class Payload(val status: Int) : Outcome
    data object Auth : Outcome
    data object Plan : Outcome
    data class Config(val status: Int) : Outcome
    data class Throttled(val retryAfterMs: Long) : Outcome
    data class Transient(val status: Int?, val message: String) : Outcome
    data object Dropped : Outcome
}

/** `min(1000 * 2^attempt, 30000)` scaled into its upper half. */
internal fun backoffMs(attempt: Int, random: Random): Long =
    (min(1000L shl attempt, 30_000L) * (0.5 + random.nextDouble() * 0.5)).roundToLong()

/**
 * One client per poster, with one suspension: the API keeps a single rate-limit counter per
 * client IP across stats and commands. Heartbeats bypass it, because a dropped stats post is a
 * stale number but a dropped heartbeat is a false "down" on the public status page.
 *
 * Not thread-safe on its own; the engine confines every call to one dispatcher.
 */
internal class ApiClient(
    private val transport: Transport,
    baseUrl: String,
    token: String,
    private val now: () -> Long,
    private val random: Random,
) {
    private val baseUrl = baseUrl.trimEnd('/')
    private val headers = mapOf(
        "Authorization" to "Bearer $token",
        "Content-Type" to "application/json",
        "User-Agent" to USER_AGENT,
    )
    private var suspendedUntil = 0L

    suspend fun post(path: String, body: JsonElement, heartbeat: Boolean): Outcome {
        if (!heartbeat && now() < suspendedUntil) return Outcome.Dropped

        val reply = try {
            transport.post(baseUrl + path, headers, body.toString())
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            return Outcome.Transient(null, e.message ?: e.javaClass.simpleName)
        }

        return when (val status = reply.status) {
            in 200..299 -> Outcome.Ok(status, runCatching { Json.parseToJsonElement(reply.body) }.getOrNull())
            401 -> Outcome.Auth
            403 -> Outcome.Plan
            // 404 is "unknown id, or the id belongs to a server listing". Never "bad token".
            404, 409 -> Outcome.Config(status)
            429 -> throttle(reply)
            in 400..499 -> Outcome.Payload(status)
            else -> Outcome.Transient(status, "TheDiscordList responded $status.")
        }
    }

    private fun throttle(reply: HttpReply): Outcome.Throttled {
        val retryAfterMs = reply.header("Retry-After")?.toDoubleOrNull()?.takeIf { it > 0 }
            ?.let { (it * 1000).roundToLong() } ?: 60_000L
        val resetAtMs = reply.header("X-RateLimit-Reset")?.toLongOrNull()?.takeIf { it > 0 }?.times(1000) ?: 0L
        suspendedUntil = max(now() + retryAfterMs, resetAtMs) + random.nextLong(0, 2_001)
        return Outcome.Throttled(retryAfterMs)
    }
}
