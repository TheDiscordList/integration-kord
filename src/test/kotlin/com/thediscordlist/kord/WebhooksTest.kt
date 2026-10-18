package com.thediscordlist.kord

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class WebhooksTest {
    private val secret = "whsec_test"
    private val t = 1_700_000_000L
    private val body = """{"id":"d1","event":"webhook.ping","created_at":"2023-11-14T22:13:20+00:00",""" +
        """"listing":{"id":"1","type":"bot","name":"Bot","url":"https://thediscordlist.com/bots/1"},"data":{"message":"hi"}}"""

    /** From SPEC_VECTOR.txt, computed with the server's own PHP formula. */
    private val vectorV1 = "b6eeea31d04c3f86990dcbb706dfaf988c9ff2fc234316ae77e168831127dc59"

    private fun sign(body: String, secret: String = this.secret, t: Long = this.t): String {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(secret.toByteArray(), "HmacSHA256"))
        return mac.doFinal("$t.$body".toByteArray()).joinToString("") { "%02x".format(it) }
    }

    private fun verify(body: String, header: String?, now: Long = t, secret: String = this.secret) =
        verifyWebhook(body, header, secret, nowEpochSeconds = now)

    private fun assertRejected(code: WebhookErrorCode, block: () -> Unit) {
        assertEquals(code, assertFailsWith<TdlWebhookException> { block() }.code)
    }

    @Test
    fun `matches the shared test vector`() {
        assertEquals(vectorV1, sign(body))

        val event = verify(body, "t=1700000000,v1=$vectorV1")

        assertEquals("d1", event.id)
        assertEquals("webhook.ping", event.event)
        assertEquals("2023-11-14T22:13:20+00:00", event.createdAt)
        assertEquals(WebhookListing("1", "bot", "Bot", "https://thediscordlist.com/bots/1"), event.listing)
        assertEquals(WebhookData.Ping("hi"), event.payload)
    }

    @Test
    fun `verifies the raw bytes`() {
        val event = verifyWebhook(body.toByteArray(), "t=$t,v1=${sign(body)}", secret, nowEpochSeconds = t)
        assertEquals("d1", event.id)
    }

    @Test
    fun `rejects a body altered after signing`() {
        assertRejected(WebhookErrorCode.INVALID_SIGNATURE) {
            verify(body.replace("\"hi\"", "\"hj\""), "t=$t,v1=${sign(body)}")
        }
    }

    @Test
    fun `rejects a re-serialised body`() {
        val reEncoded = body.replace("https://", "https:\\/\\/")
        assertRejected(WebhookErrorCode.INVALID_SIGNATURE) { verify(reEncoded, "t=$t,v1=${sign(body)}") }
    }

    @Test
    fun `rejects the wrong secret`() {
        assertRejected(WebhookErrorCode.INVALID_SIGNATURE) { verify(body, "t=$t,v1=${sign(body, secret = "whsec_other")}") }
        assertRejected(WebhookErrorCode.INVALID_SIGNATURE) { verify(body, "t=$t,v1=${sign(body)}", secret = "") }
        assertRejected(WebhookErrorCode.INVALID_SIGNATURE) { verify(body, "t=$t,v1=${sign(body, secret = " ")}", secret = " ") }
    }

    @Test
    fun `rejects a stale timestamp in both directions`() {
        val header = "t=$t,v1=${sign(body)}"
        assertRejected(WebhookErrorCode.STALE_TIMESTAMP) { verify(body, header, now = t + 301) }
        assertRejected(WebhookErrorCode.STALE_TIMESTAMP) { verify(body, header, now = t - 301) }
        assertEquals("d1", verify(body, header, now = t + 300).id)
        assertEquals("d1", verify(body, header, now = t - 300).id)
    }

    @Test
    fun `rejects a missing header`() {
        assertRejected(WebhookErrorCode.MISSING_SIGNATURE) { verify(body, null) }
        assertRejected(WebhookErrorCode.MISSING_SIGNATURE) { verify(body, "") }
    }

    @Test
    fun `rejects a malformed header`() {
        val sig = sign(body)
        listOf("garbage", "t=abc,v1=$sig", "t=1.7e9,v1=$sig", "t=$t", "t=$t,v1=", "v1=$sig").forEach { header ->
            assertRejected(WebhookErrorCode.MALFORMED_SIGNATURE) { verify(body, header) }
        }
    }

    @Test
    fun `splits header parts on the first equals sign only`() {
        val sig = sign(body)
        assertEquals("d1", verify(body, "t=$t,v1=$sig,v2=abc==,x=a=b").id)
        assertEquals("d1", verify(body, " t = $t , v1=,v1=$sig").id)
        assertRejected(WebhookErrorCode.INVALID_SIGNATURE) { verify(body, "t=$t,v1=$sig=") }
    }

    @Test
    fun `accepts an uppercase hex signature`() {
        assertEquals("d1", verify(body, "t=$t,v1=${sign(body).uppercase()}").id)
    }

    @Test
    fun `rejects a correctly signed body that is not an event envelope`() {
        assertRejected(WebhookErrorCode.MALFORMED_BODY) { verify("not json", "t=$t,v1=${sign("not json")}") }
        assertRejected(WebhookErrorCode.MALFORMED_BODY) { verify("""{"id":"d1"}""", "t=$t,v1=${sign("""{"id":"d1"}""")}") }
    }

    @Test
    fun `parses an unknown event name and keeps its data`() {
        val future = """{"id":"d2","event":"listing.reviewed","created_at":"2026-08-14T12:34:56+00:00",""" +
            """"listing":{"id":"7","type":"server","name":"S","url":"https://thediscordlist.com/servers/7"},"data":[]}"""

        val event = verify(future, "t=$t,v1=${sign(future)}")

        assertEquals("listing.reviewed", event.event)
        assertIs<WebhookData.Unknown>(event.payload)
    }

    @Test
    fun `types the data of known events`() {
        val bumped = """{"id":"d3","event":"listing.bumped","created_at":"2026-08-14T12:34:56+00:00",""" +
            """"listing":{"id":"1","type":"bot","name":"Bot","url":"https://thediscordlist.com/bots/1"},""" +
            """"data":{"points":3,"bumped_at":"2026-08-14T12:34:56+00:00","next_bump_at":"2026-08-14T16:34:56+00:00",""" +
            """"user":{"id":null,"username":"sam","avatar":"https://cdn.discordapp.com/a.png"},"extra":true}}"""

        val event = verify(bumped, "t=$t,v1=${sign(bumped)}")

        assertEquals(
            WebhookData.ListingBumped(
                points = 3,
                bumpedAt = "2026-08-14T12:34:56+00:00",
                nextBumpAt = "2026-08-14T16:34:56+00:00",
                user = WebhookUser(id = null, username = "sam", avatar = "https://cdn.discordapp.com/a.png"),
            ),
            event.payload,
        )

        val bumpReady = """{"id":"d4","event":"listing.bump_ready","created_at":"2026-08-14T16:34:56+00:00",""" +
            """"listing":{"id":"1","type":"bot","name":"Bot","url":"https://thediscordlist.com/bots/1"},""" +
            """"data":{"ready_at":"2026-08-14T16:34:56+00:00"}}"""

        assertEquals(
            WebhookData.ListingBumpReady(readyAt = "2026-08-14T16:34:56+00:00"),
            verify(bumpReady, "t=$t,v1=${sign(bumpReady)}").payload,
        )
    }
}
