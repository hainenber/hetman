# Hetman

Hetman is an agent fetching logs from target files and push to various downstream services
At the moment, the agent only supports Loki as downstream log consumer

## Features
* Resume to correct read position when restarted after sudden interruption.
* Persist failed-to-deliver logs to disk.
* Correctly follow post-renaming both old and new files, similar to `logrotate`'s `create` directive.
* Correctly follow file when rotated by truncating old files, similar to `logrotate`'s `copytruncate` directive.
* Reset read position of a tailed file during truncation.

## Roadmap
- [ ] Allow configuration with VRL.
- [ ] Ability to parse multi-line logs.
- [ ] Deliver logs in batches.
- [ ] Deliver logs in exponential backoff manner if downstream is having intermittent issue.
- [ ] Support multiple downstreams aside from Loki.
- [ ] Work with downstream's backpressure signal.
- [ ] Code coverage >80%
- [ ] Ability to gracefully reload.
