package com.thediscordlist.kord

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonArray
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

@OptIn(ExperimentalCoroutinesApi::class)
class EngineTest {
    private class Harness(val engine: Engine, val transport: FakeTransport, val events: List<TdlEvent>)

    private fun TestScope.harness(view: FakeView = FakeView(), configure: AutopostOptions.() -> Unit = {}): Harness {
        val transport = FakeTransport()
        val now = { testScheduler.currentTime }
        val random = Random(42)
        val engine = Engine(
            view = view,
            api = ApiClient(transport, BASE, "tdl_token", now, random),
            botId = "123",
            options = AutopostOptions().apply(configure),
            parent = backgroundScope.coroutineContext.job,
            dispatcher = StandardTestDispatcher(testScheduler),
            now = now,
            random = random,
        )
        val events = mutableListOf<TdlEvent>()
        backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) { engine.events.toList(events) }
        return Harness(engine, transport, events)
    }

    private fun Harness.started(): Harness = apply { engine.start() }

    private val commandList = JsonArray(listOf(json("""{"id":"9","type":1,"name":"ping","description":"Pong"}""")))

    @Test
    fun `beats immediately, before any shard is ready`() = runTest {
        val h = harness().started()
        runCurrent()

        val beat = h.transport.to("heartbeat").single()
        assertEquals("/bots/123/heartbeat", beat.path)
        assertEquals(json("""{"cluster":0,"cluster_count":1,"shards":[{"id":0,"status":"connecting","guilds":0}]}"""), beat.body)
    }

    @Test
    fun `heartbeat reports status, latency and guilds per shard, and omits an unknown latency`() = runTest {
        val h = harness(FakeView(localShards = listOf(0, 1), pings = mapOf(0 to 42.milliseconds))).started()
        runCurrent()
        h.engine.onReady(0, listOf(1UL, 2UL))

        assertEquals(PostResult(true, 200), h.engine.postHeartbeat())
        assertEquals(
            json(
                """{"cluster":0,"cluster_count":1,"shards":[""" +
                    """{"id":0,"status":"ready","latency_ms":42,"guilds":2},{"id":1,"status":"connecting","guilds":0}]}""",
            ),
            h.transport.to("heartbeat").last().body,
        )
    }

    @Test
    fun `coalesces a burst of shard transitions into one extra beat, 10 seconds after the last`() = runTest {
        val h = harness().started()
        runCurrent()
        advanceTimeBy(1_000)

        h.engine.onShardStatus(0, ShardStatus.RESUMING)
        advanceTimeBy(200)
        h.engine.onShardStatus(0, ShardStatus.CONNECTING)
        advanceTimeBy(200)
        h.engine.onShardStatus(0, ShardStatus.READY)
        advanceTimeBy(8_599)
        assertEquals(1, h.transport.to("heartbeat").size)
        advanceTimeBy(2)

        val beats = h.transport.to("heartbeat")
        assertEquals(2, beats.size)
        assertEquals(json("""{"id":0,"status":"ready","guilds":0}"""), (beats.last().body["shards"] as JsonArray).single())
    }

    @Test
    fun `retunes the heartbeat cadence from next_beat_within_seconds`() = runTest {
        val h = harness()
        h.transport.respond = { reply(200, """{"cluster":0,"status":"up","shard_count":1,"next_beat_within_seconds":150}""") }
        h.started()
        runCurrent()

        advanceTimeBy(49_999)
        assertEquals(1, h.transport.to("heartbeat").size)
        advanceTimeBy(5_002)
        assertEquals(2, h.transport.to("heartbeat").size)
    }

    @Test
    fun `posts stats 5 seconds after every local shard is ready, always with shard_count`() = runTest {
        val h = harness(FakeView(localShards = listOf(0, 1))).started()
        runCurrent()
        h.engine.onReady(0, listOf(1UL, 2UL))
        advanceTimeBy(30_000)
        assertEquals(0, h.transport.to("stats").size)

        h.engine.onReady(1, listOf(3UL))
        advanceTimeBy(4_999)
        assertEquals(0, h.transport.to("stats").size)
        advanceTimeBy(2)

        val post = h.transport.to("stats").single()
        assertEquals("/bots/123/stats", post.path)
        assertEquals(json("""{"server_count":3,"shard_count":2}"""), post.body)
        assertTrue(TdlEvent.Posted(PublisherType.STATS, 200) in h.events)
    }

    @Test
    fun `debounces guild changes, skips an unchanged count and republishes it after 30 minutes`() = runTest {
        val h = harness().started()
        h.engine.onReady(0, listOf(1UL, 2UL, 3UL))
        advanceTimeBy(5_001)
        advanceTimeBy(100_000)

        for (guild in 10UL..14UL) {
            h.engine.onGuildCreate(0, guild)
            advanceTimeBy(1_000)
        }
        h.engine.onGuildCreate(0, 10UL)
        advanceTimeBy(18_999)
        assertEquals(1, h.transport.to("stats").size)
        advanceTimeBy(2)
        assertEquals(json("""{"server_count":8,"shard_count":1}"""), h.transport.to("stats").last().body)

        advanceTimeBy(310_000)
        assertEquals(2, h.transport.to("stats").size)

        advanceTimeBy(1_800_000)
        assertEquals(3, h.transport.to("stats").size)
    }

    @Test
    fun `never posts stats twice inside the minimum gap, manual posts included`() = runTest {
        val h = harness().started()
        h.engine.onReady(0, listOf(1UL))
        advanceTimeBy(10_000)

        assertEquals(PostResult(false, null, "min-gap"), h.engine.postStats())
        assertEquals(1, h.transport.to("stats").size)
    }

    @Test
    fun `counts guilds from READY, ignores outages and drops guilds READY no longer lists`() = runTest {
        val h = harness().started()
        assertEquals(null, h.engine.measureServerCount())

        h.engine.onReady(0, listOf(1UL, 2UL, 3UL))
        assertEquals(3, h.engine.measureServerCount())
        h.engine.onGuildDelete(2UL, unavailable = true)
        assertEquals(3, h.engine.measureServerCount())
        h.engine.onGuildDelete(3UL, unavailable = false)
        h.engine.onGuildCreate(0, 4UL)
        assertEquals(3, h.engine.measureServerCount())
        h.engine.onReady(0, listOf(1UL, 9UL))
        assertEquals(2, h.engine.measureServerCount())
    }

    @Test
    fun `a 401 disables every publisher and stops all traffic until a manual post re-arms one`() = runTest {
        val h = harness(FakeView(commands = commandList))
        h.transport.respond = { reply(401, """{"message":"Unauthenticated."}""") }
        h.started()
        runCurrent()

        val disabled = h.events.filterIsInstance<TdlEvent.Disabled>()
        assertEquals(PublisherType.entries.toSet(), disabled.map { it.type }.toSet())
        assertTrue(disabled.all { it.status == 401 && it.reason == "unauthorized" })

        h.engine.onReady(0, listOf(1UL))
        h.engine.onShardStatus(0, ShardStatus.RESUMING)
        advanceTimeBy(15 * 60_000)
        assertEquals(1, h.transport.requests.size)

        h.transport.respond = { reply(200) }
        assertEquals(PostResult(true, 200), h.engine.postStats())
        advanceTimeBy(5 * 60_000)
        assertEquals(1, h.transport.to("heartbeat").size)
        assertEquals(0, h.transport.to("commands").size)
    }

    @Test
    fun `a 403 suspends heartbeats for 30 minutes and reprobes instead of disabling`() = runTest {
        val h = harness()
        h.transport.respond = { reply(403, """{"message":"Heartbeats need the Gold plan."}""") }
        h.started()
        runCurrent()
        h.engine.onShardStatus(0, ShardStatus.RESUMING)

        advanceTimeBy(1_799_999)
        assertEquals(1, h.transport.to("heartbeat").size)
        advanceTimeBy(2)
        assertEquals(2, h.transport.to("heartbeat").size)

        assertEquals(listOf(TdlEvent.Failed::class), h.events.map { it::class })
        assertEquals(403, (h.events.single() as TdlEvent.Failed).status)
    }

    @Test
    fun `a 404 disables only the publisher that received it`() = runTest {
        val h = harness()
        h.transport.respond = { if (it.path.endsWith("/stats")) reply(404) else reply(200) }
        h.started()
        h.engine.onReady(0, listOf(1UL))
        advanceTimeBy(5_001)

        assertTrue(TdlEvent.Disabled(PublisherType.STATS, 404, "not-found") in h.events)
        h.engine.onGuildCreate(0, 2UL)
        advanceTimeBy(10 * 60_000)
        assertEquals(1, h.transport.to("stats").size)
        assertTrue(h.transport.to("heartbeat").size > 10)
    }

    @Test
    fun `a 409 disables the commands publisher, and a manual post re-arms it`() = runTest {
        val h = harness(FakeView(commands = commandList))
        h.transport.respond = { if (it.path.endsWith("/commands")) reply(409) else reply(200) }
        h.started()
        h.engine.onReady(0, listOf(1UL))
        advanceTimeBy(10_001)

        assertEquals(json("""{"commands":[{"id":"9","type":1,"name":"ping","description":"Pong"}]}"""), h.transport.to("commands").single().body)
        assertTrue(TdlEvent.Disabled(PublisherType.COMMANDS, 409, "conflict") in h.events)

        h.transport.respond = { reply(200) }
        assertEquals(PostResult(true, 200), h.engine.postCommands())
        assertEquals(2, h.transport.to("commands").size)
    }

    @Test
    fun `a 429 suspends stats and commands together while heartbeats keep beating`() = runTest {
        val h = harness(FakeView(commands = commandList))
        h.transport.respond = { if (it.path.endsWith("/stats")) reply(429, headers = mapOf("Retry-After" to "120")) else reply(200) }
        h.started()
        h.engine.onReady(0, listOf(1UL))
        advanceTimeBy(5_001)
        val beatsBefore = h.transport.to("heartbeat").size

        advanceTimeBy(95_000)
        assertEquals(0, h.transport.to("commands").size)
        assertEquals(PostResult(false, null, "suspended"), h.engine.postCommands())
        assertTrue(h.transport.to("heartbeat").size >= beatsBefore + 2)

        advanceTimeBy(30_000)
        assertEquals(PostResult(true, 200), h.engine.postCommands())
    }

    @Test
    fun `retries a 5xx three times with backoff, then waits for the next tick`() = runTest {
        val h = harness()
        h.transport.respond = { if (it.path.endsWith("/stats")) reply(503) else reply(200) }
        h.started()
        h.engine.onReady(0, listOf(1UL))

        advanceTimeBy(5_001)
        assertEquals(1, h.transport.to("stats").size)
        advanceTimeBy(3_100)
        assertEquals(3, h.transport.to("stats").size)
        advanceTimeBy(290_000)
        assertEquals(3, h.transport.to("stats").size)
        advanceTimeBy(20_000)
        assertEquals(6, h.transport.to("stats").size)

        val failures = h.events.filterIsInstance<TdlEvent.Failed>()
        assertEquals(2, failures.size)
        assertTrue(failures.all { it.status == 503 && it.retryable })
    }

    @Test
    fun `a 422 is reported once and the publisher stays enabled`() = runTest {
        val h = harness()
        h.transport.respond = { if (it.path.endsWith("/stats")) reply(422) else reply(200) }
        h.started()
        h.engine.onReady(0, listOf(1UL))
        advanceTimeBy(5_001 + 300_000)

        assertEquals(2, h.transport.to("stats").size)
        assertEquals(listOf(TdlEvent.Failed(PublisherType.STATS, 422, false, "TheDiscordList rejected the stats payload (422).")), h.events.filterIsInstance<TdlEvent.Failed>())
        assertTrue(h.events.none { it is TdlEvent.Disabled })
    }

    @Test
    fun `forwards commands verbatim, and never posts an empty list unless postEmpty is set`() = runTest {
        val empty = harness().started()
        empty.engine.onReady(0, listOf(1UL))
        advanceTimeBy(10_001)
        assertEquals(0, empty.transport.to("commands").size)
        assertEquals(PostResult(false, null, "empty"), empty.engine.postCommands())

        val opted = harness { commands { postEmpty = true } }.started()
        opted.engine.onReady(0, listOf(1UL))
        advanceTimeBy(10_001)
        assertEquals(json("""{"commands":[]}"""), opted.transport.to("commands").single().body)
    }

    @Test
    fun `does not heartbeat when the cluster count is unknown`() = runTest {
        val h = harness(FakeView(localShards = listOf(0, 1), totalShards = 4)).started()
        runCurrent()
        h.engine.onReady(0, listOf(1UL))
        h.engine.onReady(1, listOf(2UL))
        advanceTimeBy(5 * 60_000)

        assertEquals(0, h.transport.requests.size)
        assertTrue(TdlEvent.Disabled(PublisherType.HEARTBEAT, null, "cluster-count-unknown") in h.events)
        assertTrue(TdlEvent.Disabled(PublisherType.STATS, null, "server-count-unknown") in h.events)
        assertEquals(PostResult(false, null, "cluster-count-unknown"), h.engine.postHeartbeat())
    }

    @Test
    fun `takes the server count from serverCount when shards are split across processes`() = runTest {
        val h = harness(FakeView(localShards = listOf(0, 1), totalShards = 4)) { serverCount = { 1234 } }.started()
        h.engine.onReady(0, listOf(1UL))
        h.engine.onReady(1, listOf(2UL))
        advanceTimeBy(5_001)

        assertEquals(json("""{"server_count":1234,"shard_count":4}"""), h.transport.to("stats").single().body)
        assertEquals(0, h.transport.to("heartbeat").size)
    }

    @Test
    fun `a non-zero cluster heartbeats for itself and never posts stats or commands`() = runTest {
        val view = FakeView(localShards = listOf(2, 3), totalShards = 4, commands = commandList)
        val h = harness(view) {
            cluster = Cluster(id = 1, count = 2)
            serverCount = { 10 }
        }.started()
        runCurrent()
        h.engine.onReady(2, listOf(1UL))
        h.engine.onReady(3, listOf(2UL))
        advanceTimeBy(60_000)

        val beat = h.transport.to("heartbeat").first().body
        assertEquals(json("""{"cluster":1,"cluster_count":2,"shards":[{"id":2,"status":"connecting","guilds":0},{"id":3,"status":"connecting","guilds":0}]}"""), beat)
        assertEquals(0, h.transport.to("stats").size + h.transport.to("commands").size)
        assertEquals(PostResult(false, null, "not-elected"), h.engine.postStats())
    }

    @Test
    fun `stop cancels every timer`() = runTest {
        val h = harness().started()
        runCurrent()
        h.engine.onReady(0, listOf(1UL))
        h.engine.stop()
        advanceTimeBy(10 * 60_000)

        assertEquals(1, h.transport.requests.size)
    }

    @Test
    fun `stop closes the transport once nothing is in flight`() = runTest {
        val h = harness().started()
        runCurrent()

        h.engine.stop()
        runCurrent()

        assertTrue(h.transport.closed)
    }

    @Test
    fun `detects topology from the shard range and the override`() {
        assertEquals(Topology(0, 1, true, 1, true), detectTopology(listOf(0), 1, null))
        assertEquals(Topology(0, 1, true, 4, true), detectTopology(listOf(0, 1, 2, 3), 4, null))
        assertEquals(Topology(0, null, true, 4, false), detectTopology(listOf(0, 1), 4, null))
        assertEquals(Topology(0, null, false, 4, false), detectTopology(listOf(2, 3), 4, null))
        assertEquals(Topology(1, 2, false, 4, false), detectTopology(listOf(2, 3), 4, Cluster(1, 2)))
    }
}
