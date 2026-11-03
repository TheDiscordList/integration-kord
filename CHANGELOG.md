# Changelog

## 0.2.1 - 2026-11-03

- A shard leaves `guilds` out of its heartbeat until it is ready, so a restart's
  first beat cannot zero the listing's server count.
- A `429` is retried once its window lifts, when that comes before the next
  run.
- A `429` on a heartbeat no longer suspends stats and commands: the API limits
  them separately.
- Commands are retried every 5 minutes after a 5xx, a rate limit, a plan
  suspension or a failed fetch from Discord, until they land or are rejected.
- `minGap` cannot go below 60 seconds, `interval` cannot go below `minGap`, and
  the starting heartbeat `interval` is clamped to 15-60 seconds.
- A burst of shard transitions no longer pushes the out-of-band beat back, which
  could starve heartbeats past the API's grace window.
- `verifyWebhook` throws `IllegalArgumentException` for a blank secret before
  looking at the delivery, so a misconfiguration no longer reads as bad
  deliveries.

## 0.2.0 - 2026-10-18

- `WebhookData.ListingBumpReady` for `listing.bump_ready`, with `readyAt`.
- `408` and `425` are retried like 5xx; every other `4xx` is reported once and
  not retried.
- A `429` suspends for at most an hour.
- A shard transition beats no sooner than 10 seconds after the previous beat.
- Suspensions and gaps run on the monotonic clock, so a wall-clock jump cannot
  stretch or cut them short.
- The HTTP client is closed when the poster stops, on Java 21 and newer.
- A token pasted with surrounding whitespace or a leading `Bearer ` works.
- A blank webhook secret is rejected like an empty one.

## 0.1.0 - 2026-10-15

- `kord.autopost(token)`: posts server count, per-cluster shard heartbeats and global
  application commands to TheDiscordList, with the API's status handling (401 disables,
  403 suspends heartbeats, 404/409 disable one publisher, 429 suspends stats and commands
  together, 5xx retried with backoff).
- `cluster` and `serverCount` options for bots that split shards across processes.
- `verifyWebhook` for `X-TDL-Signature`, with typed payloads for every current event.
