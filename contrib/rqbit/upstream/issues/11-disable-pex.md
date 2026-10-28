# Add an explicit session option to disable peer exchange

An embedded caller can disable DHT and select trackers, but cannot opt out of
peer exchange on public torrents. For an application with an explicit peer
discovery policy, disabling only DHT leaves another discovery route active.
This is a proposed option, not a report that the current default is incorrect.

Would an opt-in `SessionOptions::disable_pex`, defaulting to `false`, fit the
library's configuration model? The intended contract is to omit `ut_pex` from
the extended handshake, send no PEX updates, and ignore incoming PEX messages
when enabled. Existing private-torrent restrictions would remain in force.
The protocol being controlled is [BEP 11](https://www.bittorrent.org/beps/bep_0011.html).

I have a downstream prototype against v8.1.1, but it does not apply to current
`main` (`f4ca7d919638a7b8b031ab062a61861a9eb586e4`, inspected 2026-10-28).
This issue asks about the API before porting that prototype. There is no new
upstream build or test claim for the option.

If the option is welcome, I would propose a small PR with a CLI flag and
loopback protocol tests covering handshake advertisement, outbound updates,
incoming PEX handling, the default path, and private torrents. Please advise
whether the scope should instead be per torrent or an existing discovery
configuration structure.

## AI disclosure

Prepared with AI coding assistants (OpenAI Codex; Anthropic Claude via Claude
Code), including the downstream prototype and this issue draft.
