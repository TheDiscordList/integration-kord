package com.thediscordlist.kord

import dev.kord.cache.api.DataCache
import dev.kord.common.entity.ApplicationCommandOption
import dev.kord.common.entity.ApplicationCommandOptionType
import dev.kord.common.entity.ApplicationCommandType
import dev.kord.common.entity.DiscordApplicationCommand
import dev.kord.common.entity.Permission
import dev.kord.common.entity.Permissions
import dev.kord.common.entity.Snowflake
import dev.kord.common.entity.optional.Optional
import dev.kord.common.entity.optional.OptionalBoolean
import dev.kord.core.ClientResources
import dev.kord.core.Kord
import dev.kord.core.cache.data.UserData
import dev.kord.core.entity.User
import dev.kord.core.event.Event
import dev.kord.core.event.gateway.DisconnectEvent
import dev.kord.core.event.gateway.ReadyEvent
import dev.kord.core.event.guild.GuildDeleteEvent
import dev.kord.core.gateway.DefaultMasterGateway
import dev.kord.core.gateway.handler.DefaultGatewayEventInterceptor
import dev.kord.core.supplier.EntitySupplyStrategy
import dev.kord.gateway.Gateway
import dev.kord.gateway.GatewayCloseCode
import dev.kord.gateway.builder.Shards
import dev.kord.rest.request.Request
import dev.kord.rest.request.RequestHandler
import dev.kord.rest.service.RestClient
import io.ktor.client.HttpClient
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonArray
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/** Drives a real Kord instance with no network: a fake gateway, a fake REST handler and our own event flow. */
@OptIn(ExperimentalCoroutinesApi::class)
class KordWiringTest {
    private class PingGateway(latency: Duration) : Gateway by Gateway.none() {
        override val ping: StateFlow<Duration?> = MutableStateFlow(latency)
    }

    private class CommandsOnly(private val commands: List<DiscordApplicationCommand>) : RequestHandler {
        override val token: String = "discord-token"

        @Suppress("UNCHECKED_CAST")
        override suspend fun <B : Any, R> handle(request: Request<B, R>): R = commands as R
    }

    private val ping = DiscordApplicationCommand(
        id = Snowflake(9),
        type = Optional(ApplicationCommandType.ChatInput),
        applicationId = Snowflake(123),
        name = "echo",
        description = "Echoes",
        options = Optional(
            listOf(
                ApplicationCommandOption(
                    type = ApplicationCommandOptionType.String,
                    name = "text",
                    description = "What to echo",
                    required = OptionalBoolean.Value(true),
                ),
            ),
        ),
        defaultMemberPermissions = Permissions(Permission.Administrator),
        version = Snowflake(1),
    )

    private val pingJson = """{"id":9,"type":1,"application_id":123,"name":"echo","description":"Echoes",""" +
        """"options":[{"type":3,"name":"text","description":"What to echo","required":true}],""" +
        """"default_member_permissions":"8","version":1}"""

    private fun TestScope.kord(events: MutableSharedFlow<Event>): Kord = Kord(
        ClientResources("discord-token", Snowflake(123), Shards(2), 1, HttpClient(), EntitySupplyStrategy.cache),
        DataCache.none(),
        DefaultMasterGateway(mapOf(0 to PingGateway(42.milliseconds), 1 to Gateway.none())),
        RestClient(CommandsOnly(listOf(ping))),
        Snowflake(123),
        events,
        StandardTestDispatcher(testScheduler),
        DefaultGatewayEventInterceptor(),
    )

    private fun ready(kord: Kord, shard: Int, vararg guilds: Long) = ReadyEvent(
        10,
        guilds.map { Snowflake(it) }.toSet(),
        User(UserData(Snowflake(123), "bot"), kord),
        "session",
        "wss://gateway.discord.gg",
        kord,
        shard,
        null,
    )

    /** The poster is a child of Kord's job, so cancelling Kord stops it too, even when an assertion fails. */
    private inline fun <T> Kord.closing(block: (Kord) -> T): T = try {
        block(this)
    } finally {
        cancel()
    }

    @Test
    fun `serialises commands in Discord's wire shape`() {
        assertEquals(JsonArray(listOf(json(pingJson))), encodeCommands(listOf(ping)))
    }

    @Test
    fun `wires shards, guild events, latency and commands from a real Kord`() = runTest {
        val events = MutableSharedFlow<Event>(extraBufferCapacity = 64)
        val transport = FakeTransport()

        kord(events).closing { kord ->
            kord.autopost(
                token = "tdl_token",
                options = AutopostOptions().apply { baseUrl = BASE },
                transport = transport,
                dispatcher = StandardTestDispatcher(testScheduler),
                now = { testScheduler.currentTime },
                random = Random(1),
            )
            runCurrent()

            val boot = transport.to("heartbeat").single()
            assertEquals("/bots/123/heartbeat", boot.path)
            assertEquals(
                json(
                    """{"cluster":0,"cluster_count":1,"shards":[""" +
                        """{"id":0,"status":"connecting","latency_ms":42,"guilds":0},{"id":1,"status":"connecting","guilds":0}]}""",
                ),
                boot.body,
            )

            events.emit(ready(kord, 0, 1, 2))
            events.emit(ready(kord, 1, 3))
            advanceTimeBy(5_001)
            assertEquals("/bots/123/stats", transport.to("stats").single().path)
            assertEquals(json("""{"server_count":3,"shard_count":2}"""), transport.to("stats").single().body)

            advanceTimeBy(5_000)
            assertEquals(json("""{"commands":[$pingJson]}"""), transport.to("commands").single().body)

            events.emit(GuildDeleteEvent(Snowflake(3), false, null, kord, 1, null))
            advanceTimeBy(60_000)
            assertEquals(json("""{"server_count":2,"shard_count":2}"""), transport.to("stats").last().body)

            events.emit(DisconnectEvent.DiscordCloseEvent(kord, 0, GatewayCloseCode.SessionTimeout, true, null))
            advanceTimeBy(1_001)
            assertEquals(
                json(
                    """{"cluster":0,"cluster_count":1,"shards":[""" +
                        """{"id":0,"status":"resuming","latency_ms":42,"guilds":2},{"id":1,"status":"ready","guilds":0}]}""",
                ),
                transport.to("heartbeat").last().body,
            )
        }
    }

    @Test
    fun `strips whitespace and a pasted Bearer prefix from the token`() = runTest {
        val transport = FakeTransport()

        kord(MutableSharedFlow()).closing { kord ->
            kord.autopost(" Bearer tdl_token\n", AutopostOptions().apply { baseUrl = BASE }, transport, StandardTestDispatcher(testScheduler), { testScheduler.currentTime })
            runCurrent()

            assertEquals("Bearer tdl_token", transport.requests.first().headers["Authorization"])
        }
    }

    @Test
    fun `a missing token returns an inert poster that never sends`() = runTest {
        val transport = FakeTransport()

        kord(MutableSharedFlow()).closing { kord ->
            for (token in listOf(null, "", "   ")) {
                val poster = kord.autopost(token, AutopostOptions(), transport, StandardTestDispatcher(testScheduler), { testScheduler.currentTime })
                assertEquals(PostResult(false, null, "disabled"), poster.postStats())
                assertEquals(PostResult(false, null, "disabled"), poster.postHeartbeat())
                assertEquals(PostResult(false, null, "disabled"), poster.postCommands())
                poster.stop()
            }
            assertEquals(PostResult(false, null, "disabled"), kord.autopost(null).postStats())

            advanceTimeBy(10 * 60_000)
            assertEquals(0, transport.requests.size)
        }
    }
}
