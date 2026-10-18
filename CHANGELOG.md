# Changelog

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
