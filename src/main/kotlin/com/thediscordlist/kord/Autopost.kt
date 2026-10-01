package com.thediscordlist.kord

import dev.kord.common.entity.DiscordApplicationCommand
import dev.kord.core.Kord
import dev.kord.core.event.Event
import dev.kord.core.event.gateway.ConnectEvent
import dev.kord.core.event.gateway.DisconnectEvent
import dev.kord.core.event.gateway.ReadyEvent
import dev.kord.core.event.gateway.ResumedEvent
import dev.kord.core.event.guild.GuildCreateEvent
import dev.kord.core.event.guild.GuildDeleteEvent
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.launch
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.jsonArray
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

public const val VERSION: String = "0.1.0"
public const val DEFAULT_BASE_URL: String = "https://thediscordlist.com/api"
internal const val USER_AGENT = "TheDiscordList-Kord/$VERSION (+https://github.com/TheDiscordList/integration-kord)"

public enum class PublisherType {
    STATS,
    HEARTBEAT,
    COMMANDS,
    ;

    internal val wire: String get() = name.lowercase()
}

/**
 * The outcome of one publish. [skipped] names why nothing was sent: `"disabled"`, `"min-gap"`,
 * `"unchanged"`, `"not-elected"`, `"suspended"`, `"plan"`, `"empty"`, `"cluster-count-unknown"`, ...
 */
public data class PostResult(
    public val ok: Boolean,
    public val status: Int?,
    public val skipped: String? = null,
)

public sealed interface TdlEvent {
    public val type: PublisherType

    public data class Posted(override val type: PublisherType, public val status: Int) : TdlEvent

    public data class Failed(
        override val type: PublisherType,
        public val status: Int?,
        public val retryable: Boolean,
        public val message: String,
    ) : TdlEvent

    public data class Disabled(override val type: PublisherType, public val status: Int?, public val reason: String) : TdlEvent
}

/** Names this process's place in a multi-process deployment. Cluster ids are zero-based. */
public data class Cluster(public val id: Int, public val count: Int)

public class StatsOptions {
    public var enabled: Boolean = true
    public var interval: Duration = 5.minutes

    /** Trailing debounce after a guild join or leave. */
    public var debounce: Duration = 20.seconds

    /** Hard floor between two stats POSTs, manual posts included. */
    public var minGap: Duration = 60.seconds
}

public class HeartbeatOptions {
    public var enabled: Boolean = true

    /** Starting cadence; the API's `next_beat_within_seconds` retunes it. */
    public var interval: Duration = 30.seconds
}

public class CommandsOptions {
    public var enabled: Boolean = true

    /** Posting an empty list CLEARS the listing's commands, so it is off unless asked for. */
    public var postEmpty: Boolean = false
}

public class AutopostOptions {
    /** Discord application id. Defaults to `kord.resources.applicationId`. */
    public var botId: String? = null
    public var baseUrl: String = DEFAULT_BASE_URL

    /** Required when this process runs only some of the bot's shards. */
    public var cluster: Cluster? = null

    /** The whole bot's guild count, for deployments that split shards across processes. */
    public var serverCount: (suspend () -> Int)? = null

    public val stats: StatsOptions = StatsOptions()
    public val heartbeat: HeartbeatOptions = HeartbeatOptions()
    public val commands: CommandsOptions = CommandsOptions()

    public fun stats(configure: StatsOptions.() -> Unit): Unit = stats.configure()
    public fun heartbeat(configure: HeartbeatOptions.() -> Unit): Unit = heartbeat.configure()
    public fun commands(configure: CommandsOptions.() -> Unit): Unit = commands.configure()
}

/**
 * Publishes stats, heartbeats and commands in the background. Nothing here throws at the host:
 * the `post` functions return a [PostResult], and background failures arrive on [events] (or the
 * `com.thediscordlist.kord` logger when nothing collects them).
 */
public class Autoposter internal constructor(private val engine: Engine?) {
    public val events: SharedFlow<TdlEvent> = engine?.events ?: MutableSharedFlow<TdlEvent>().asSharedFlow()

    public suspend fun postStats(): PostResult = engine?.postStats() ?: skipped("disabled")

    public suspend fun postHeartbeat(): PostResult = engine?.postHeartbeat() ?: skipped("disabled")

    public suspend fun postCommands(): PostResult = engine?.postCommands() ?: skipped("disabled")

    /** Cancels every timer and detaches from Kord. Not restartable. */
    public fun stop() {
        engine?.stop()
    }
}

/**
 * Wires this Kord instance to a TheDiscordList listing. Call it before [Kord.login], like any
 * other event handler.
 *
 * With a missing token it logs a warning and returns an inert [Autoposter], so a forgotten
 * environment variable never takes down a deploy.
 */
public fun Kord.autopost(token: String?, configure: AutopostOptions.() -> Unit = {}): Autoposter = autopost(
    token = token,
    options = AutopostOptions().apply(configure),
    transport = JdkTransport(),
    dispatcher = Dispatchers.Default.limitedParallelism(1),
    now = System::currentTimeMillis,
)

internal fun Kord.autopost(
    token: String?,
    options: AutopostOptions,
    transport: Transport,
    dispatcher: CoroutineDispatcher,
    now: () -> Long,
    random: Random = Random.Default,
): Autoposter {
    if (token.isNullOrBlank()) {
        warn("No stats token supplied; auto-posting is disabled.")
        return Autoposter(null)
    }

    val engine = Engine(
        view = KordView(this),
        api = ApiClient(transport, options.baseUrl, token, now, random),
        botId = options.botId ?: resources.applicationId.toString(),
        options = options,
        parent = coroutineContext[Job],
        dispatcher = dispatcher,
        now = now,
        random = random,
    )
    // Undispatched, so the subscription exists before this function returns and login cannot race it.
    engine.scope.launch(start = CoroutineStart.UNDISPATCHED) {
        events.collect { engine.handle(it) }
    }
    engine.start()
    return Autoposter(engine)
}

private fun Engine.handle(event: Event) {
    when (event) {
        is ReadyEvent -> onReady(event.shard, event.guildIds.map { it.value })
        is ResumedEvent -> onShardStatus(event.shard, ShardStatus.READY)
        is ConnectEvent -> onShardStatus(event.shard, ShardStatus.CONNECTING)
        is DisconnectEvent -> onShardStatus(event.shard, event.toShardStatus())
        is GuildCreateEvent -> onGuildCreate(event.shard, event.guild.id.value)
        is GuildDeleteEvent -> onGuildDelete(event.guildId.value, event.unavailable)
        else -> Unit
    }
}

private fun DisconnectEvent.toShardStatus(): ShardStatus = when (this) {
    is DisconnectEvent.DetachEvent,
    is DisconnectEvent.UserCloseEvent,
    is DisconnectEvent.RetryLimitReachedEvent,
    -> ShardStatus.DISCONNECTED
    is DisconnectEvent.DiscordCloseEvent -> if (recoverable) ShardStatus.RESUMING else ShardStatus.DISCONNECTED
    is DisconnectEvent.SessionReset -> ShardStatus.CONNECTING
    else -> ShardStatus.RESUMING
}

/** Kord's own REST entity, so the commands serialise in Discord's wire shape and forward verbatim. */
private val commandsSerializer = ListSerializer(DiscordApplicationCommand.serializer())

private class KordView(private val kord: Kord) : BotView {
    override val localShards: List<Int> = kord.gateway.gateways.keys.sorted()
    override val totalShards: Int = kord.resources.shards.totalShards

    override fun latency(shard: Int): Duration? = kord.gateway.gateways[shard]?.ping?.value

    override suspend fun fetchCommands(): JsonArray = encodeCommands(
        kord.rest.interaction.getGlobalApplicationCommands(kord.resources.applicationId),
    )
}

internal fun encodeCommands(commands: List<DiscordApplicationCommand>): JsonArray =
    Json.encodeToJsonElement(commandsSerializer, commands).jsonArray
