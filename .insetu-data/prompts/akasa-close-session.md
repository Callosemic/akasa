{{include_prompt: ctx://prompts/.templates/git/close_session_protocol.md
{
guardrail_rules:"- **Host-Agnostic Boundaries:** Never hardcode host paths (e.g., `.insetu`); resolve dynamically via `AKASA_CONTROL_DIR`.\n- **Blind Substrate Mandate:** Never bleed domain-specific business logic (UI, Git, specific repositories) into the pure Ring 0 / Tier 1 kernel.\n- **Multi-Tenant State Isolation:** Never use unprotected globals for tenant data; use thread-local stores (`_local`) or tenant-keyed registries.\n- **Concurrency & VFS Confinement:** Route writes through the VFS write queue and avoid blocking worker/metronome pools."
}
}}