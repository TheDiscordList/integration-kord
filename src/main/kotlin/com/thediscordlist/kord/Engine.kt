package com.thediscordlist.kord

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.put
import kotlin.coroutines.cancellation.CancellationException
import kotlin.math.min
import kotlin.math.roundToLong
import kotlin.random.Random
import kotlin.time.Duration

internal const val STATS_FIRST_POST_DELAY_MS = 5_000L
internal const val STATS_REMEASURE_MS = 60_000L

/** Republish an unchanged count this often, so a listing never looks abandoned. */
internal const val STATS_FORCE_AFTER_MS = 1_800_000L
internal const val HEARTBEAT_MIN_INTERVAL_MS = 15_000L
internal const val HEARTBEAT_MAX_INTERVAL_MS = 60_000L
internal const val HEARTBEAT_JITTER_MS = 5_000L
internal const val HEARTBEAT_OUT_OF_BAND_GAP_MS = 10_000L

/** Offset from the first stats post: the two share one server-side rate budget. */
internal const val COMMANDS_DELAY_MS = 10_000L

/** The Gold tier is derived per request, so a bot can gain it mid-run. */
internal const val PLAN_REPROBE_MS = 1_800_000L
internal const val MAX_LATENCY_MS = 600_000L
private const val MAX_ATTEMPTS = 3

internal val logger: System.Logger = System.getLogger("com.thediscordlist.kord")

internal fun warn(message: String) = logger.log(System.Logger.Level.WARNING, "[TheDiscordList] $message")

internal fun skipped(reason: String) = PostResult(ok = false, status = null, skipped = reason)

internal enum class ShardStatus(val wire: String) {
    READY("ready"),
    CONNECTING("connecting"),
    RESUMING("resuming"),
    DISCONNECTED("disconnected"),
}

/** The slice of a Kord client the engine reads, so tests can run without a gateway. */
internal interface BotView {
    val localShards: List<Int>
    val totalShards: Int
    fun latency(shard: Int): Duration?
    suspend fun fetchCommands(): JsonArray
}

internal data class Topology(
    val clusterId: Int,
    /** `null` means undeterminable, and the heartbeat publisher must not start. */
    val clusterCount: Int?,
    val isStatsPublisher: Boolean,
    val shardCount: Int,
    /** True when this process runs every shard, so its guild ledger is the whole bot. */
    val countsLocally: Boolean,
)

internal fun detectTopology(localShards: Collection<Int>, totalShards: Int, cluster: Cluster?): Topology {
    val countsLocally = (0 until totalShards).all { it in localShards }
    return when {
        cluster != null -> Topology(cluster.id, cluster.count, cluster.id == 0, totalShards, countsLocally)
        countsLocally -> Topology(0, 1, true, totalShards, true)
        // A partial shard range with no override: Kord has no registry of sibling processes.
        else -> Topology(0, null, 0 in localShards, totalShards, false)
    }
}

/**
 * Every piece of mutable state below is touched only from [dispatcher], which runs one task at
 * a time. Awaits still interleave, exactly like an event loop, so the in-flight flags matter.
 */
internal class Engine(
    private val view: BotView,
    internal val api: ApiClient,
    internal val botId: String,
    private val options: AutopostOptions,
    parent: Job?,
    private val dispatcher: CoroutineDispatcher,
    internal val now: () -> Long,
    internal val random: Random,
) {
    internal val topology = detectTopology(view.localShards, view.totalShards, options.cluster)
    internal val scope = CoroutineScope(
        SupervisorJob(parent) + dispatcher + CoroutineExceptionHandler { _, error -> warn("Unexpected failure: $error") },
    )

    init {
        // Once the last request has settled, so closing the client never waits on one.
        scope.coroutineContext.job.invokeOnCompletion { api.close() }
    }

    private val mutableEvents = MutableSharedFlow<TdlEvent>(
        extraBufferCapacity = 64,
        onBufferOverflow = BufferOverflow.DROP_OLDEST,
    )
    val events: SharedFlow<TdlEvent> = mutableEvents.asSharedFlow()

    private val stats = if (options.stats.enabled) StatsPublisher(this, options.stats) else null
    private val heartbeat = if (options.heartbeat.enabled) HeartbeatPublisher(this, options.heartbeat) else null
    private val commands = if (options.commands.enabled) CommandsPublisher(this, options.commands) else null

    /** Guild id to shard id. READY seeds it, so unavailable guilds count, as on every bot list. */
    private val guildShards = HashMap<ULong, Int>()
    private val readyShards = HashSet<Int>()
    private val shardStatus = HashMap<Int, ShardStatus>()
    private val warned = HashSet<String>()
    private var started = false

    internal val hasServerCount get() = options.serverCount != null

    /** Heartbeat first and before login: a booting bot must read "connecting", not dead. */
    fun start() {
        scope.launch { heartbeat?.start() }
    }

    fun stop() = scope.cancel()

    suspend fun postStats() = manual(stats)
    suspend fun postHeartbeat() = manual(heartbeat)
    suspend fun postCommands() = manual(commands)

    private suspend fun manual(publisher: Publisher?): PostResult {
        publisher ?: return skipped("disabled")
        return withContext(dispatcher) { guarded(publisher.type, PostResult(false, null, "error")) { publisher.post() } }
    }

    fun onReady(shard: Int, guildIds: Collection<ULong>) {
        // READY lists every guild of the session, so it replaces whatever the shard held before.
        // That also drops guilds the bot left while the shard was offline.
        guildShards.values.removeAll { it == shard }
        guildIds.forEach { guildShards[it] = shard }
        readyShards += shard
        onShardStatus(shard, ShardStatus.READY)

        if (started) {
            stats?.onGuildChange()
        } else if (readyShards.containsAll(view.localShards)) {
            started = true
            stats?.start()
            commands?.start()
        }
    }

    fun onGuildCreate(shard: Int, guildId: ULong) {
        // Most GUILD_CREATEs fill in guilds READY already announced; only a real join moves the count.
        if (guildShards.put(guildId, shard) == null && started) stats?.onGuildChange()
    }

    fun onGuildDelete(guildId: ULong, unavailable: Boolean) {
        // `unavailable` is an outage, not a removal. The bot is still in the guild.
        if (!unavailable && guildShards.remove(guildId) != null && started) stats?.onGuildChange()
    }

    fun onShardStatus(shard: Int, status: ShardStatus) {
        if (shardStatus.put(shard, status) != status) heartbeat?.beatNow()
    }

    internal suspend fun measureServerCount(): Int? {
        if (!topology.isStatsPublisher) return null
        options.serverCount?.let { supplier ->
            return try {
                supplier().takeIf { it >= 0 }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                null
            }
        }
        if (!topology.countsLocally || !readyShards.containsAll(view.localShards)) return null
        return guildShards.size
    }

    internal fun shardReports(): JsonArray {
        val guildsPerShard = guildShards.values.groupingBy { it }.eachCount()
        return JsonArray(
            view.localShards.map { id ->
                buildJsonObject {
                    put("id", id)
                    put("status", (shardStatus[id] ?: ShardStatus.CONNECTING).wire)
                    // The API floor is 0 and its ceiling ten minutes; no reading means no key.
                    view.latency(id)?.inWholeMilliseconds?.takeIf { it >= 0 }
                        ?.let { put("latency_ms", min(it, MAX_LATENCY_MS)) }
                    put("guilds", guildsPerShard[id] ?: 0)
                }
            },
        )
    }

    internal suspend fun fetchCommands(): JsonArray = view.fetchCommands()

    internal fun localShardSummary() = "${view.localShards} of ${view.totalShards}"

    internal fun disableAll() {
        warnOnce(
            "unauthorized",
            "TheDiscordList rejected the stats token (401). Every publisher is disabled; " +
                "a manual post re-arms one once the token is fixed.",
        )
        listOfNotNull(stats, heartbeat, commands).forEach { it.disable(401, "unauthorized", quiet = true) }
    }

    /** Failures nobody is collecting are logged, so they never vanish silently. */
    internal fun emit(event: TdlEvent, quiet: Boolean = false) {
        mutableEvents.tryEmit(event)
        if (quiet || mutableEvents.subscriptionCount.value > 0) return
        when (event) {
            is TdlEvent.Failed -> warn("${event.type.wire} failed: ${event.message}")
            is TdlEvent.Disabled -> warn("${event.type.wire} disabled: ${event.reason}")
            is TdlEvent.Posted -> Unit
        }
    }

    internal fun warnOnce(key: String, message: String) {
        if (warned.add(key)) warn(message)
    }

    internal suspend fun <T> guarded(type: PublisherType, fallback: T, block: suspend () -> T): T = try {
        block()
    } catch (e: CancellationException) {
        throw e
    } catch (e: Exception) {
        emit(TdlEvent.Failed(type, null, false, e.message ?: e.toString()))
        fallback
    }
}

internal abstract class Publisher(protected val engine: Engine, val type: PublisherType) {
    private var timer: Job? = null
    protected var disabledReason: String? = null
    protected var suspendedUntil = 0L
    private val reported = HashSet<Int>()

    abstract suspend fun post(): PostResult

    /**
     * Replaces the pending run. The work is launched apart from the timer, so rescheduling never
     * cancels a request that is already in flight.
     */
    protected fun schedule(delayMs: Long, run: suspend () -> Unit) {
        timer?.cancel()
        timer = engine.scope.launch {
            delay(delayMs)
            engine.scope.launch { engine.guarded(type, Unit) { run() } }
        }
    }

    fun disable(status: Int?, reason: String, quiet: Boolean = false) {
        timer?.cancel()
        if (disabledReason != null) return
        disabledReason = reason
        engine.emit(TdlEvent.Disabled(type, status, reason), quiet)
    }

    /** An explicit manual post re-arms a disabled publisher. */
    protected fun rearm() {
        disabledReason = null
    }

    private fun reportOnce(status: Int, message: String) {
        if (reported.add(status)) engine.emit(TdlEvent.Failed(type, status, false, message))
    }

    /**
     * Retries only transient failures. A backoff that would outlast the next tick is dropped
     * instead: the next tick carries fresher data, and a failed request still burns budget.
     */
    protected suspend fun send(path: String, body: JsonElement, heartbeat: Boolean, nextTickInMs: Long?): PostResult {
        var outcome: Outcome = Outcome.Dropped
        for (attempt in 0 until MAX_ATTEMPTS) {
            outcome = engine.api.post(path, body, heartbeat)
            if (outcome !is Outcome.Transient || attempt == MAX_ATTEMPTS - 1) break
            val wait = backoffMs(attempt, engine.random)
            if (nextTickInMs != null && wait >= nextTickInMs) break
            delay(wait)
        }
        return react(outcome)
    }

    protected open fun react(outcome: Outcome): PostResult = when (outcome) {
        is Outcome.Ok -> {
            engine.emit(TdlEvent.Posted(type, outcome.status))
            PostResult(true, outcome.status)
        }
        Outcome.Dropped -> skipped("suspended")
        is Outcome.Payload -> {
            // Stays enabled: the next tick may carry a valid payload.
            reportOnce(outcome.status, "TheDiscordList rejected the ${type.wire} payload (${outcome.status}).")
            PostResult(false, outcome.status)
        }
        Outcome.Auth -> {
            engine.disableAll()
            PostResult(false, 401)
        }
        Outcome.Plan -> {
            // Suspend, never disable: the plan is checked per request and can change mid-run.
            suspendedUntil = engine.now() + PLAN_REPROBE_MS
            reportOnce(403, "The ${type.wire} endpoint needs a higher plan. Reprobing in 30 minutes.")
            PostResult(false, 403, "plan")
        }
        is Outcome.Config -> {
            disable(outcome.status, if (outcome.status == 409) "conflict" else "not-found")
            PostResult(false, outcome.status)
        }
        is Outcome.Throttled -> PostResult(false, 429, "throttled")
        is Outcome.Transient -> {
            engine.emit(TdlEvent.Failed(type, outcome.status, true, outcome.message))
            PostResult(false, outcome.status)
        }
    }
}

internal class StatsPublisher(engine: Engine, options: StatsOptions) : Publisher(engine, PublisherType.STATS) {
    private val interval = options.interval.inWholeMilliseconds
    private val debounce = options.debounce.inWholeMilliseconds
    private val minGap = options.minGap.inWholeMilliseconds

    private var lastPostAt: Long? = null
    private var lastCount: Int? = null
    private var lastShardCount: Int? = null
    private var debounceCap: Long? = null
    private var inFlight = false

    fun start() {
        val topology = engine.topology
        if (disabledReason != null || !topology.isStatsPublisher) return
        if (!topology.countsLocally && !engine.hasServerCount) {
            engine.warnOnce(
                "server-count",
                "This process runs shards ${engine.localShardSummary()}, and Kord has no cross-process " +
                    "broadcast to sum guilds with. Pass serverCount = { ... } to post stats from here.",
            )
            disable(null, "server-count-unknown")
            return
        }
        schedule(STATS_FIRST_POST_DELAY_MS) { publish(manual = false) }
    }

    /** Trailing debounce, capped at [minGap] so a join storm cannot starve the post. */
    fun onGuildChange() {
        if (disabledReason != null || !engine.topology.isStatsPublisher) return
        val now = engine.now()
        val cap = debounceCap ?: (now + minGap).also { debounceCap = it }
        schedule(min(now + debounce, cap) - now) { publish(manual = false) }
    }

    override suspend fun post(): PostResult {
        rearm()
        return publish(manual = true)
    }

    private suspend fun publish(manual: Boolean): PostResult {
        debounceCap = null
        if (!manual && disabledReason != null) return skipped("disabled")
        if (inFlight) return skipped("in-flight")

        val now = engine.now()
        if (!manual && now < suspendedUntil) {
            schedule(suspendedUntil - now) { publish(manual = false) }
            return skipped("plan")
        }

        // A hard floor that manual posts and join storms route through alike.
        val last = lastPostAt
        if (last != null && now < last + minGap) {
            schedule(last + minGap - now) { publish(manual = false) }
            return skipped("min-gap")
        }

        inFlight = true
        try {
            val count = engine.measureServerCount()
            if (count == null) {
                val topology = engine.topology
                return when {
                    !topology.isStatsPublisher -> skipped("not-elected")
                    !topology.countsLocally && !engine.hasServerCount -> skipped("server-count-unknown")
                    else -> {
                        schedule(STATS_REMEASURE_MS) { publish(manual = false) }
                        skipped("unmeasurable")
                    }
                }
            }

            val shardCount = engine.topology.shardCount
            // Every stats post re-indexes the listing and fires a webhook even when nothing moved,
            // and the server does not dedupe, so the skip has to happen here.
            if (count == lastCount && shardCount == lastShardCount && last != null &&
                engine.now() < last + STATS_FORCE_AFTER_MS
            ) {
                schedule(interval) { publish(manual = false) }
                return skipped("unchanged")
            }

            lastPostAt = engine.now()
            // shard_count is always sent: the server stores it unconditionally, so omitting it nulls it.
            val body = buildJsonObject {
                put("server_count", count)
                put("shard_count", shardCount)
            }
            val result = send("/bots/${engine.botId}/stats", body, heartbeat = false, nextTickInMs = interval)
            if (result.ok) {
                lastCount = count
                lastShardCount = shardCount
            }
            if (disabledReason == null) schedule(interval) { publish(manual = false) }
            return result
        } finally {
            inFlight = false
        }
    }
}

internal class HeartbeatPublisher(engine: Engine, options: HeartbeatOptions) : Publisher(engine, PublisherType.HEARTBEAT) {
    private var interval = options.interval.inWholeMilliseconds
    private var inFlight = false
    private var lastBeatAt: Long? = null

    fun start() {
        // Every beat hard-deletes the cluster rows with an id >= its cluster_count. A process that
        // guessed 1 would wipe all of its siblings, so an unknown count means silence.
        if (engine.topology.clusterCount == null) {
            engine.warnOnce(
                "cluster-count",
                "This process runs shards ${engine.localShardSummary()}, so the cluster count is unknown. " +
                    "Heartbeats are disabled, because a wrong cluster_count deletes sibling clusters " +
                    "server-side. Pass cluster = Cluster(id, count) to enable them.",
            )
            disable(null, "cluster-count-unknown")
            return
        }
        schedule(0) { beat(manual = false) }
    }

    /**
     * Out-of-band beat after a shard transition, at least 10 seconds after the previous one.
     * Calls within a second coalesce.
     */
    fun beatNow() {
        if (disabledReason != null || engine.topology.clusterCount == null || engine.now() < suspendedUntil) return
        val earliest = lastBeatAt?.plus(HEARTBEAT_OUT_OF_BAND_GAP_MS) ?: 0L
        schedule(maxOf(earliest - engine.now(), 1_000L)) { beat(manual = false) }
    }

    override suspend fun post(): PostResult {
        if (engine.topology.clusterCount == null) return skipped("cluster-count-unknown")
        rearm()
        return beat(manual = true)
    }

    private suspend fun beat(manual: Boolean): PostResult {
        if (!manual && disabledReason != null) return skipped("disabled")
        if (inFlight) return skipped("in-flight")
        val clusterCount = engine.topology.clusterCount ?: return skipped("cluster-count-unknown")

        val startedAt = engine.now()
        lastBeatAt = startedAt
        val body = buildJsonObject {
            put("cluster", engine.topology.clusterId)
            put("cluster_count", clusterCount)
            put("shards", engine.shardReports())
        }

        inFlight = true
        val result = try {
            send("/bots/${engine.botId}/heartbeat", body, heartbeat = true, nextTickInMs = interval)
        } finally {
            inFlight = false
        }

        if (disabledReason == null) {
            // Measured from the start of this beat, so a slow request cannot drift the cadence.
            val jitter = (engine.random.nextDouble() * HEARTBEAT_JITTER_MS).roundToLong()
            val floor = if (result.skipped == "throttled") interval else 1_000L
            val now = engine.now()
            schedule(maxOf(startedAt + interval + jitter - now, floor, suspendedUntil - now)) { beat(manual = false) }
        }
        return result
    }

    /**
     * The cadence follows `next_beat_within_seconds` rather than a constant, so a change to the
     * server's grace window retunes existing installs without a release.
     */
    override fun react(outcome: Outcome): PostResult {
        if (outcome is Outcome.Ok) {
            val seconds = ((outcome.body as? JsonObject)?.get("next_beat_within_seconds") as? JsonPrimitive)?.doubleOrNull
            if (seconds != null && seconds > 0) {
                interval = (seconds / 3 * 1000).roundToLong().coerceIn(HEARTBEAT_MIN_INTERVAL_MS, HEARTBEAT_MAX_INTERVAL_MS)
            }
        }
        return super.react(outcome)
    }
}

internal class CommandsPublisher(engine: Engine, private val options: CommandsOptions) : Publisher(engine, PublisherType.COMMANDS) {
    /** Once after ready. Commands change on deploy, and a deploy is a restart. */
    fun start() {
        if (disabledReason == null && engine.topology.isStatsPublisher) schedule(COMMANDS_DELAY_MS) { publish() }
    }

    override suspend fun post(): PostResult {
        rearm()
        return publish()
    }

    private suspend fun publish(): PostResult {
        if (disabledReason != null) return skipped("disabled")
        if (engine.now() < suspendedUntil) return skipped("plan")

        val commands = try {
            engine.fetchCommands()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            engine.emit(TdlEvent.Failed(type, null, false, "Could not fetch the global commands from Discord: ${e.message}"))
            return skipped("fetch-failed")
        }

        // An empty array CLEARS the listing's commands, so it is never sent by accident.
        if (commands.isEmpty() && !options.postEmpty) return skipped("empty")

        return send("/bots/${engine.botId}/commands", buildJsonObject { put("commands", commands) }, heartbeat = false, nextTickInMs = null)
    }
}
