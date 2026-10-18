# com.thediscordlist:integration-kord

Publish your Kord bot's server count, shard heartbeats and application commands
to [TheDiscordList](https://thediscordlist.com) automatically, and verify its
inbound webhooks.

No dependencies beyond Kord itself: HTTP is `java.net.http`, signatures are
`javax.crypto`, and JSON is the kotlinx.serialization Kord already ships.

```kotlin
// build.gradle.kts
dependencies {
    implementation("com.thediscordlist:integration-kord:0.2.0")
}
```

```xml
<dependency>
  <groupId>com.thediscordlist</groupId>
  <artifactId>integration-kord</artifactId>
  <version>0.2.0</version>
</dependency>
```

## Quickstart

```kotlin
import com.thediscordlist.kord.autopost
import dev.kord.core.Kord

suspend fun main() {
    val kord = Kord(System.getenv("DISCORD_TOKEN"))

    kord.autopost(System.getenv("TDL_TOKEN"))

    kord.login()
}
```

That is the whole integration, and it is also the recommended production setup.
It starts three publishers:

| Publisher | What it sends | Cadence |
| --- | --- | --- |
| Stats | `server_count`, `shard_count` | 5s after every shard is ready, then every 5 min, debounced on guild join/leave |
| Heartbeat | per-shard status, latency and guild counts | immediately, then every ~30s, plus on any shard transition, at least 10s after the previous beat |
| Commands | your registered global commands | once, ~10s after ready |

Your **stats token** is on the listing's manage page, under the API tab. It is
scoped to one listing, so it is the only credential this package needs.

Call `autopost` before `login()`, like any other Kord event handler: `login()`
suspends until the bot shuts down. With a missing token it logs a warning and
returns an inert poster, so a forgotten environment variable never takes down a
deploy. The poster lives in Kord's own coroutine scope and stops with it.

## Sharding

Kord runs every shard you give it in one process, so most bots need no
configuration at all:

| Topology | `server_count` comes from | Stats posted by | Heartbeats |
| --- | --- | --- | --- |
| Single shard | the guild ledger | this process | this process |
| All shards in one process (`sharding { Shards(it) }`) | the guild ledger | this process | this process |
| Shards split across processes | your `serverCount` | cluster 0 | every process, for itself |

The guild ledger is built from gateway events: `READY` seeds it with every
guild of the session (unavailable ones included, as on any bot list), joins and
leaves adjust it, and an outage (`unavailable = true`) leaves it alone. It does
not depend on Kord's cache settings.

When a process runs only some shards, for example
`sharding { Shards(totalShards = 16, indices = 0..7) }`, Kord has no
cross-process broadcast to sum guilds with. Name the topology and supply the
whole bot's count yourself:

```kotlin
kord.autopost(System.getenv("TDL_TOKEN")) {
    cluster = Cluster(id = System.getenv("CLUSTER_ID").toInt(), count = System.getenv("CLUSTER_COUNT").toInt())
    serverCount = { redis.sumGuildCounts() } // only called on cluster 0
}
```

Every process heartbeats for **itself** on purpose. A manager reporting "all
fine" for a process whose host has died is exactly the failure the status page
exists to catch.

> **If the cluster count cannot be determined, heartbeats do not start.** The
> API hard-deletes every cluster row whose id is `>= cluster_count` in the
> request, so one process guessing `1` would erase all of its siblings and
> their monitoring history. A process running a partial shard range without
> `cluster` emits `Disabled(HEARTBEAT, reason = "cluster-count-unknown")` and
> logs the fix. Losing a status page beats losing data.

## Options

```kotlin
kord.autopost(token) {
    botId = "161660517914509312" // default: kord.resources.applicationId
    baseUrl = "https://thediscordlist.com/api"

    stats {
        enabled = true
        interval = 5.minutes
        debounce = 20.seconds
        minGap = 60.seconds
    }
    heartbeat {
        enabled = true
        interval = 30.seconds
    }
    commands {
        enabled = true
        postEmpty = false
    }

    cluster = Cluster(id = 0, count = 1)
    serverCount = { 1234 }
}
```

`minGap` is a hard floor between two stats POSTs. A join storm, a mass
reconnect and a manual `postStats()` all route through it, so the ceiling is
structurally one request per minute. An unchanged count is not re-sent for 30
minutes.

`commands.postEmpty` is `false` because posting an empty list **clears** the
command list on your listing. Turn it on only if that is what you mean.

## Events and manual posting

```kotlin
val poster = kord.autopost(System.getenv("TDL_TOKEN"))

poster.events.onEach { event ->
    when (event) {
        is TdlEvent.Posted -> {}   // event.type, event.status
        is TdlEvent.Failed -> {}   // event.status, event.retryable, event.message
        is TdlEvent.Disabled -> {} // event.reason: "unauthorized", "not-found", "conflict", ...
    }
}.launchIn(kord)

poster.postStats()     // never throws; returns PostResult(ok, status, skipped)
poster.postCommands()
poster.postHeartbeat()

poster.stop()
```

Nothing in this package throws at your bot. Background work is wrapped, the
`post` functions return a `PostResult`, and while nothing collects `events`,
failures go to the `com.thediscordlist.kord` `System.Logger` (java.util.logging
unless you bridge it, e.g. with `slf4j-jdk-platform-logging`).

An explicit `postX()` also re-arms a publisher that disabled itself, so you can
retry after fixing a token or a listing id without restarting.

## Webhooks

`verifyWebhook` needs no Kord client and no framework. It takes the raw body,
the `X-TDL-Signature` header and your webhook secret, and returns the parsed
event or throws a `TdlWebhookException` whose `code` says why.

### Ktor

```kotlin
import com.thediscordlist.kord.*

routing {
    post("/webhooks/tdl") {
        val event = try {
            verifyWebhook(call.receive<ByteArray>(), call.request.headers[WEBHOOK_SIGNATURE_HEADER], secret)
        } catch (e: TdlWebhookException) {
            return@post call.respond(HttpStatusCode.BadRequest) // e.code says why
        }

        when (val data = event.payload) {
            is WebhookData.ListingUpvoted -> rewardVoter(data.user, data.totalUpvotes)
            is WebhookData.ListingBumped -> announceBump(data.points, data.nextBumpAt)
            else -> Unit // webhook.ping, and events this version does not know yet
        }
        call.respond(HttpStatusCode.NoContent)
    }
}
```

The body must be the exact bytes that arrived. Receive it as a `ByteArray` (or
the raw string), never as a deserialised object: re-encoding a parsed payload
can never verify, because the signature covers bytes, not meaning.

### Events

| `event.event` | `event.payload` |
| --- | --- |
| `listing.bumped` | `WebhookData.ListingBumped` |
| `listing.bump_ready` | `WebhookData.ListingBumpReady` |
| `listing.upvoted` | `WebhookData.ListingUpvoted` |
| `server.joined` | `WebhookData.ServerJoined` (server listings only) |
| `bot.added` | `WebhookData.BotAdded` |
| `bot.stats_reported` | `WebhookData.BotStatsReported` |
| `webhook.ping` | `WebhookData.Ping` |
| anything newer | `WebhookData.Unknown` with the raw JSON |

Two of them are not what their names suggest. `bot.added` is a **click** event,
dispatched on the install hand-off: Discord never tells anyone whether the
install completed. And `bot.stats_reported` is not an echo of your own POST: it
also fires when the owner edits the listing in the web UI, and from the
heartbeat's internal stats forward.

### Rules for a healthy endpoint

- **Always answer 2xx**, including for `webhook.ping` and for event names you
  do not recognise. The sender disables an endpoint after 10 consecutive failed
  deliveries. A 500 on an unknown event is a self-inflicted outage.
- **Answer within 10 seconds.** The sender times out after that and retries.
  Launch slow work in a coroutine and reply first.
- A disabled endpoint is probed with a `webhook.ping` every 6 hours for 7 days,
  and **a 2xx to that probe re-enables it**.
- **Register an exact `https://` URL with no redirect.** The sender treats 3xx
  as a failure, so an `http → https` or trailing-slash redirect burns every
  retry and counts toward the disable streak.
- **Deduplicate on `event.id`.** It equals the `X-TDL-Delivery` header and is
  stable across all five retries of one delivery. A manual "redeliver" from the
  UI mints a new id deliberately, so it is not dropped.
- **Rotating the secret has no grace window.** Deliveries already queued under
  the old secret become unverifiable.

## Plans, limits and known behaviour

- **Heartbeats need Gold or higher.** On a `403` the heartbeat publisher
  suspends and reprobes every 30 minutes rather than disabling: the plan is
  checked per request, so a bot can gain it mid-run.
- **Rate limits are shared.** The API counts stats and commands against one
  budget per bot (60/min; heartbeats have their own 120/min). A `429`
  suspends stats and commands together for `Retry-After` (or until
  `X-RateLimit-Reset`, at most an hour); heartbeats keep beating, because a
  stale number beats a false "down" on your status page.
- **`404` never means "bad token".** It means the id is unknown or belongs to a
  server listing, and it disables only the publisher that received it. A `401`
  is the bad-token signal, and it disables all three.
- Transient failures (`408`, `425`, 5xx, timeouts, network) are retried twice
  with jittered exponential backoff, then left to the next run. Any other `4xx`
  is reported once and not retried.
- **Only global commands are published.** Guild-scoped commands are not part of
  a listing.
- **Keep the `Guilds` intent.** Kord's default intents include it. Without it
  the count still comes from `READY`, but joins and leaves stop moving it.

## Requirements

- JVM 17 or newer
- Kotlin 2.2 or newer, as Kord 0.18 requires
- Kord `0.18.x` (built and tested against `0.18.1`)

Kord is an `api` dependency rather than `compileOnly`: its types are in this
library's public signatures, and the published POM then pins the Kord version
this code was compiled and tested against. Gradle resolves to the newer one if
your build asks for a later Kord.

## Publishing (maintainers)

Releases go to Maven Central through the Sonatype Central Portal, using the
[vanniktech maven-publish plugin](https://vanniktech.github.io/gradle-maven-publish-plugin/).
Central has no trusted publishing, so `release.yml` reads four repository secrets.

One-time setup:

1. Sign in at [central.sonatype.com](https://central.sonatype.com) and verify
   the `com.thediscordlist` namespace (a DNS TXT record on `thediscordlist.com`).
2. Under **View Account → Generate User Token**, create a token. Store its
   username and password as the `MAVEN_CENTRAL_USERNAME` and
   `MAVEN_CENTRAL_PASSWORD` repository secrets.
3. Create a signing key (`gpg --quick-generate-key "TheDiscordList <...>" rsa4096 sign never`),
   publish it (`gpg --keyserver keyserver.ubuntu.com --send-keys <KEY_ID>`), and
   store `gpg --armor --export-secret-keys <KEY_ID>` as `MAVEN_GPG_PRIVATE_KEY` and its
   passphrase as `MAVEN_GPG_PASSPHRASE`.

Each release:

1. Set `version` in `gradle.properties` and move the `CHANGELOG.md` entry under it.
2. Commit, then tag and push: `git tag v0.2.0 && git push origin v0.2.0`.
3. `release.yml` checks that the tag matches `version`, builds, signs and runs
   `publishAndReleaseToMavenCentral`. The artifact is searchable on Maven
   Central within about 30 minutes.

## License

MIT
