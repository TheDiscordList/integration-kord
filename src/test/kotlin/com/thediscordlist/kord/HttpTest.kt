package com.thediscordlist.kord

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.net.InetSocketAddress
import java.util.Collections
import kotlin.random.Random
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

class HttpTest {
    private class Received(val method: String, val path: String, val headers: Map<String, String>, val body: String)

    private lateinit var server: HttpServer
    private val received: MutableList<Received> = Collections.synchronizedList(mutableListOf())

    @Volatile
    private var respond: (HttpExchange) -> Unit = { it.send(200, """{"message":"Stats updated.","server_count":5}""") }
    private var clock = 1_000_000_000L

    @BeforeTest
    fun startServer() {
        server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/") { exchange ->
            received += Received(
                exchange.requestMethod,
                exchange.requestURI.path,
                exchange.requestHeaders.mapKeys { it.key.lowercase() }.mapValues { it.value.first() },
                exchange.requestBody.readBytes().decodeToString(),
            )
            respond(exchange)
        }
        server.start()
    }

    @AfterTest
    fun stopServer() = server.stop(0)

    private fun HttpExchange.send(status: Int, body: String = "", headers: Map<String, String> = emptyMap()) {
        headers.forEach { (name, value) -> responseHeaders.add(name, value) }
        val bytes = body.toByteArray()
        sendResponseHeaders(status, if (bytes.isEmpty()) -1 else bytes.size.toLong())
        responseBody.use { it.write(bytes) }
    }

    private fun client(transport: Transport = JdkTransport(), baseUrl: String = "http://127.0.0.1:${server.address.port}/api") =
        ApiClient(transport, baseUrl, "tdl_token", { clock }, Random(7))

    private val stats = buildJsonObject {
        put("server_count", 5)
        put("shard_count", 1)
    }

    @Test
    fun `sends one JSON POST with bearer auth and the user agent`(): Unit = runBlocking {
        val outcome = client().post("/bots/123/stats", stats, heartbeat = false)

        assertEquals(Outcome.Ok(200, json("""{"message":"Stats updated.","server_count":5}""")), outcome)
        val request = received.single()
        assertEquals("POST", request.method)
        assertEquals("/api/bots/123/stats", request.path)
        assertEquals("Bearer tdl_token", request.headers["authorization"])
        assertEquals("application/json", request.headers["content-type"])
        assertEquals("TheDiscordList-Kord/$VERSION (+https://github.com/TheDiscordList/integration-kord)", request.headers["user-agent"])
        assertEquals("""{"server_count":5,"shard_count":1}""", request.body)
    }

    @Test
    fun `classifies every status the API documents`(): Unit = runBlocking {
        val expected = mapOf(
            401 to Outcome.Auth,
            403 to Outcome.Plan,
            404 to Outcome.Config(404),
            409 to Outcome.Config(409),
            400 to Outcome.Payload(400),
            422 to Outcome.Payload(422),
            500 to Outcome.Transient(500, "TheDiscordList responded 500."),
            502 to Outcome.Transient(502, "TheDiscordList responded 502."),
        )
        expected.forEach { (status, outcome) ->
            respond = { it.send(status, """{"message":"x"}""") }
            assertEquals(outcome, client().post("/bots/1/stats", stats, heartbeat = false), "status $status")
        }
    }

    @Test
    fun `tolerates a 2xx body that is not JSON`(): Unit = runBlocking {
        respond = { it.send(200, "<html>OK</html> {") }
        assertEquals(Outcome.Ok(200, null), client().post("/bots/1/stats", stats, heartbeat = false))
    }

    @Test
    fun `honours Retry-After and X-RateLimit-Reset, and lets heartbeats through`(): Unit = runBlocking {
        val api = client()
        val resetAt = clock / 1000 + 120
        respond = { it.send(429, "", mapOf("Retry-After" to "5", "X-RateLimit-Reset" to "$resetAt")) }
        assertEquals(Outcome.Throttled(5_000), api.post("/bots/1/stats", stats, heartbeat = false))

        respond = { it.send(200, "{}") }
        clock += 100_000
        assertEquals(Outcome.Dropped, api.post("/bots/1/stats", stats, heartbeat = false))
        assertEquals(1, received.size)

        assertIs<Outcome.Ok>(api.post("/bots/1/heartbeat", stats, heartbeat = true))
        assertEquals(2, received.size)

        clock += 23_000
        assertIs<Outcome.Ok>(api.post("/bots/1/commands", stats, heartbeat = false))
        assertEquals(3, received.size)
    }

    @Test
    fun `suspends for 60 seconds when a 429 names no window`(): Unit = runBlocking {
        val api = client()
        respond = { it.send(429) }
        assertEquals(Outcome.Throttled(60_000), api.post("/bots/1/stats", stats, heartbeat = false))

        respond = { it.send(200, "{}") }
        clock += 59_000
        assertEquals(Outcome.Dropped, api.post("/bots/1/stats", stats, heartbeat = false))
        clock += 4_000
        assertIs<Outcome.Ok>(api.post("/bots/1/stats", stats, heartbeat = false))
    }

    @Test
    fun `reports a refused connection as transient`(): Unit = runBlocking {
        val closed = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).also { it.start() }
        val port = closed.address.port
        closed.stop(0)

        val outcome = client(baseUrl = "http://127.0.0.1:$port/api").post("/bots/1/stats", stats, heartbeat = false)

        assertIs<Outcome.Transient>(outcome)
        assertEquals(null, outcome.status)
    }

    @Test
    fun `reports a timeout as transient`(): Unit = runBlocking {
        respond = {
            Thread.sleep(1_000)
            it.send(200, "{}")
        }

        val outcome = client(JdkTransport(timeout = 200.milliseconds)).post("/bots/1/stats", stats, heartbeat = false)

        assertIs<Outcome.Transient>(outcome)
        assertTrue(outcome.message.isNotBlank())
    }

    @Test
    fun `backs off exponentially, capped at 30 seconds, in the upper half`() {
        val random = Random(3)
        repeat(200) {
            for (attempt in 0..6) {
                val ceiling = minOf(1000L shl attempt, 30_000L)
                assertTrue(backoffMs(attempt, random) in ceiling / 2..ceiling)
            }
        }
    }

    @Test
    fun `version matches the build`() {
        assertEquals(System.getProperty("tdl.version"), VERSION)
    }
}
