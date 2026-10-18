package com.thediscordlist.kord

import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import java.security.MessageDigest
import java.time.Instant
import java.util.HexFormat
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.math.abs
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

public const val WEBHOOK_SIGNATURE_HEADER: String = "X-TDL-Signature"
public const val WEBHOOK_DELIVERY_HEADER: String = "X-TDL-Delivery"

public enum class WebhookErrorCode {
    MISSING_SIGNATURE,
    MALFORMED_SIGNATURE,
    STALE_TIMESTAMP,
    INVALID_SIGNATURE,
    MALFORMED_BODY,
}

public class TdlWebhookException(public val code: WebhookErrorCode, message: String) : Exception(message)

@Serializable
public data class WebhookListing(
    /** Guild id for a server, application id for a bot. */
    public val id: String,
    /** `"server"` or `"bot"`. */
    public val type: String,
    public val name: String,
    public val url: String,
)

@Serializable
public data class WebhookUser(
    public val id: String? = null,
    /** Snapshotted at the user's last sign-in, not a live Discord handle. */
    public val username: String,
    /** A full CDN URL, not an avatar hash. */
    public val avatar: String? = null,
)

/** Timestamps are ISO-8601 strings with an offset, e.g. `2026-08-14T12:34:56+00:00`. */
public sealed interface WebhookData {
    @Serializable
    public data class ListingBumped(
        public val points: Int,
        @SerialName("bumped_at") public val bumpedAt: String,
        @SerialName("next_bump_at") public val nextBumpAt: String,
        public val user: WebhookUser? = null,
    ) : WebhookData

    /** The bump cooldown lifted. Sent once per cooldown. */
    @Serializable
    public data class ListingBumpReady(
        @SerialName("ready_at") public val readyAt: String,
    ) : WebhookData

    @Serializable
    public data class ListingUpvoted(
        @SerialName("voted_at") public val votedAt: String,
        @SerialName("total_upvotes") public val totalUpvotes: Int,
        public val user: WebhookUser? = null,
    ) : WebhookData

    /** Server listings only. */
    @Serializable
    public data class ServerJoined(
        @SerialName("joined_at") public val joinedAt: String,
        public val user: WebhookUser? = null,
    ) : WebhookData

    /** A click on the install hand-off. Discord never reports whether the install completed. */
    @Serializable
    public data class BotAdded(
        @SerialName("added_at") public val addedAt: String,
        public val user: WebhookUser? = null,
    ) : WebhookData

    /** Also fires on owner edits in the web UI and on the heartbeat's stats forward. */
    @Serializable
    public data class BotStatsReported(
        @SerialName("guild_count") public val guildCount: Int? = null,
        @SerialName("shard_count") public val shardCount: Int? = null,
        @SerialName("reported_at") public val reportedAt: String,
    ) : WebhookData

    @Serializable
    public data class Ping(public val message: String) : WebhookData

    /** An event this version does not know yet, or a shape it could not read. Answer 2xx anyway. */
    public data class Unknown(public val raw: JsonElement) : WebhookData
}

@Serializable
public data class WebhookEvent(
    /** Equals `X-TDL-Delivery` and is stable across every retry of one delivery: dedupe on it. */
    public val id: String,
    public val event: String,
    /** Enqueue time, not send time. Do not use it for freshness. */
    @SerialName("created_at") public val createdAt: String,
    public val listing: WebhookListing,
    public val data: JsonElement = JsonObject(emptyMap()),
) {
    /** [data] decoded for the known event names. */
    public val payload: WebhookData
        get() = when (event) {
            "listing.bumped" -> decode(WebhookData.ListingBumped.serializer())
            "listing.bump_ready" -> decode(WebhookData.ListingBumpReady.serializer())
            "listing.upvoted" -> decode(WebhookData.ListingUpvoted.serializer())
            "server.joined" -> decode(WebhookData.ServerJoined.serializer())
            "bot.added" -> decode(WebhookData.BotAdded.serializer())
            "bot.stats_reported" -> decode(WebhookData.BotStatsReported.serializer())
            "webhook.ping" -> decode(WebhookData.Ping.serializer())
            else -> WebhookData.Unknown(data)
        }

    private fun decode(serializer: KSerializer<out WebhookData>): WebhookData = try {
        webhookJson.decodeFromJsonElement(serializer, data)
    } catch (e: IllegalArgumentException) {
        WebhookData.Unknown(data)
    }
}

private val webhookJson = Json { ignoreUnknownKeys = true }

/**
 * Verifies `X-TDL-Signature` and returns the parsed envelope.
 *
 * [rawBody] must be the exact bytes that arrived. Re-serialising a parsed body can never verify:
 * the signature covers bytes, not meaning.
 *
 * @throws TdlWebhookException and nothing else; [TdlWebhookException.code] says why.
 */
public fun verifyWebhook(
    rawBody: ByteArray,
    signatureHeader: String?,
    secret: String,
    tolerance: Duration = 5.minutes,
    nowEpochSeconds: Long = Instant.now().epochSecond,
): WebhookEvent {
    if (signatureHeader.isNullOrBlank()) {
        throw TdlWebhookException(WebhookErrorCode.MISSING_SIGNATURE, "Missing $WEBHOOK_SIGNATURE_HEADER header.")
    }

    // Split on the first '=' only: a value may itself contain '='.
    var timestamp: String? = null
    var signature: String? = null
    for (part in signatureHeader.split(',')) {
        val eq = part.indexOf('=')
        if (eq == -1) continue
        val key = part.substring(0, eq).trim()
        val value = part.substring(eq + 1).trim()
        if (key == "t" && timestamp == null) {
            timestamp = value
        } else if (key == "v1" && signature == null && value.isNotEmpty()) {
            signature = value
        }
    }

    val t = timestamp?.toLongOrNull()
    if (t == null || signature == null) {
        throw TdlWebhookException(WebhookErrorCode.MALFORMED_SIGNATURE, "Malformed $WEBHOOK_SIGNATURE_HEADER header.")
    }

    // Two-sided, and against `t`, never `created_at`: that is stamped at enqueue time.
    if (abs(nowEpochSeconds - t) > tolerance.inWholeSeconds) {
        throw TdlWebhookException(WebhookErrorCode.STALE_TIMESTAMP, "Webhook timestamp outside the tolerance window.")
    }

    if (secret.isBlank()) {
        throw TdlWebhookException(WebhookErrorCode.INVALID_SIGNATURE, "The webhook secret is blank.")
    }
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(SecretKeySpec(secret.toByteArray(), "HmacSHA256"))
    mac.update("$timestamp.".toByteArray())
    val expected = HexFormat.of().formatHex(mac.doFinal(rawBody))

    if (!MessageDigest.isEqual(expected.toByteArray(), signature.lowercase().toByteArray())) {
        throw TdlWebhookException(WebhookErrorCode.INVALID_SIGNATURE, "Webhook signature did not match.")
    }

    return try {
        webhookJson.decodeFromString(WebhookEvent.serializer(), rawBody.decodeToString())
    } catch (e: IllegalArgumentException) {
        throw TdlWebhookException(WebhookErrorCode.MALFORMED_BODY, "Webhook body was not a valid event envelope.")
    }
}

/** UTF-8 encodes [rawBody]. Prefer the [ByteArray] overload when the framework offers raw bytes. */
public fun verifyWebhook(
    rawBody: String,
    signatureHeader: String?,
    secret: String,
    tolerance: Duration = 5.minutes,
    nowEpochSeconds: Long = Instant.now().epochSecond,
): WebhookEvent = verifyWebhook(rawBody.encodeToByteArray(), signatureHeader, secret, tolerance, nowEpochSeconds)
