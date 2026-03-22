from __future__ import annotations

from pathlib import Path


def forge_block_skill_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "skills" / "forge-block-author"


def forge_block_skill_file() -> Path:
    return forge_block_skill_dir() / "SKILL.md"


def load_forge_block_skill() -> str:
    return forge_block_skill_file().read_text(encoding="utf-8")


def render_block_author_prompt(
    *,
    block_name: str,
    description: str,
    category: str,
    n_inputs: int,
    output_count: int,
) -> str:
    return "\n".join(
        [
            "Use the Forge block-authoring skill in this repository to create a new atomic block.",
            f"Block name: {block_name}",
            f"Description: {description}",
            f"Category: {category}",
            f"Number of inputs: {n_inputs}",
            f"Number of outputs: {output_count}",
            "Requirements:",
            "- Implement the block as a Python class under blocks/.",
            "- Follow BaseBlock metadata and execute/validate conventions used elsewhere in Forge.",
            "- Add or update tests covering the new behavior.",
            "- Preserve deterministic output handle naming and any provenance-sensitive behavior.",
            f"Skill path: {forge_block_skill_dir()}",
        ]
    )
