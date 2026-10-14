# Changelog

## 0.1.0 - 2026-10-15

- `kord.autopost(token)`: posts server count, per-cluster shard heartbeats and global
  application commands to TheDiscordList, with the API's status handling (401 disables,
  403 suspends heartbeats, 404/409 disable one publisher, 429 suspends stats and commands
  together, 5xx retried with backoff).
- `cluster` and `serverCount` options for bots that split shards across processes.
- `verifyWebhook` for `X-TDL-Signature`, with typed payloads for every current event.
