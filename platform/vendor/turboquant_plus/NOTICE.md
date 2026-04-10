This repository vendors a lightweight reference copy of the Python TurboQuant
implementation from:

https://github.com/TheTom/turboquant_plus

Upstream license: Apache License 2.0

Mission Control adapts the vendored reference implementation so it can run inside
this repo without adding SciPy as a hard dependency. The vendored code is used for:

- local verification that the TurboQuant reference algorithm is present
- OpenClaw execution-architecture diagnostics
- lightweight round-trip/runtime smoke checks

It is not, by itself, a production inference backend. Live KV-cache acceleration
still requires a compatible runtime such as a patched llama.cpp-style backend.
