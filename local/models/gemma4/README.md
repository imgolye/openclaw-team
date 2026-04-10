Place local Gemma 4 GGUF files in this directory for Docker deployments.

Expected preset filenames:
- gemma-4-e2b-it-q4_k_m.gguf
- gemma-4-e4b-it-q4_k_m.gguf
- mmproj-e2b-f16.gguf
- mmproj-e4b-f16.gguf

The `mission-control` container mounts `./models` to `/models`, so the built-in
Gemma local-runtime presets resolve these files as:
- /models/gemma4/gemma-4-e2b-it-q4_k_m.gguf
- /models/gemma4/gemma-4-e4b-it-q4_k_m.gguf
- /models/gemma4/mmproj-e2b-f16.gguf
- /models/gemma4/mmproj-e4b-f16.gguf
