# Axelar-monitor

This project looks at events on Axelar and emits Prometheus metrics for:

- Heartbeats
- EVM Votes against consensus


Currently, the consensus is determined purely by vote popularity, without using VP for weight adjustment.
