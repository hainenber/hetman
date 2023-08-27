# Hetman

Hetman is an agent fetching logs from target files and push to various downstream services
At the moment, the agent only supports Loki as downstream log consumer

## Features
* Resume to correct read position when restarted after sudden interruption.
* Persist failed-to-deliver logs to disk.
* Apply exponential backoff to failed delivery request(s) if downstream is having intermittent issue.
* Correctly follow post-renaming both old and new files, similar to `logrotate`'s `create` directive.
* Correctly follow file when rotated by truncating old files, similar to `logrotate`'s `copytruncate` directive.
* Reset read position of a tailed file during truncation.
* Gracefully reload configurations changes when sending SIGHUP to running agent.
* Deliver logs in batches.
* Built-in backpressure engine to control input/output bandwidth.
* High percentage of code coverage (>= 80% for the majority of components)
* Capable to parse multi-line logs.
* Support multiple downstreams:
    * Loki
    * Kafka

## Roadmap
- [ ] Fleet management.
