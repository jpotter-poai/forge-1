"""REST endpoint that returns the MCP server configuration for this installation."""
from __future__ import annotations

import json
import platform
import sys
import sysconfig
from pathlib import Path

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from backend.api.deps import get_services
from backend.services import AppServices

router = APIRouter(prefix="/mcp-config", tags=["mcp-config"])


def _site_packages() -> str:
    """Return the site-packages directory for the current Python installation."""
    purelib = sysconfig.get_path("purelib")
    if purelib:
        return str(Path(purelib).resolve())
    for p in sys.path:
        if "site-packages" in p:
            return str(Path(p).resolve())
    return ""


class McpConfigResponse(BaseModel):
    python_executable: str
    pythonpath: str
    blocks_dir: str
    pipeline_dir: str
    checkpoint_dir: str
    log_level: str
    config_json: dict
    setup_prompt: str
    os_name: str


@router.get("", response_model=McpConfigResponse)
def get_mcp_config(services: AppServices = Depends(get_services)) -> McpConfigResponse:
    """Return the MCP configuration for this Forge installation with all paths resolved."""
    settings = services.settings
    python_exe = str(Path(sys.executable).resolve())
    pythonpath = _site_packages()

    # Resolve all paths to absolute so the config works from any working directory
    blocks_dir = str(Path(settings.blocks_dir).resolve())
    pipeline_dir = str(Path(settings.pipeline_dir).resolve())
    checkpoint_dir = str(Path(settings.checkpoint_dir).resolve())

    mcp_entry = {
        "command": python_exe,
        "args": [
            "-c",
            (
                "from backend.settings import Settings; "
                "from backend.services import build_services; "
                "from backend.mcp_server import run_mcp_stdio; "
                "run_mcp_stdio(build_services(Settings.from_env()))"
            ),
        ],
        "env": {
            "PYTHONPATH": pythonpath,
            "BLOCKS_DIR": blocks_dir,
            "PIPELINE_DIR": pipeline_dir,
            "CHECKPOINT_DIR": checkpoint_dir,
            "LOG_LEVEL": settings.log_level,
        },
    }
    config_json = {"forge": mcp_entry}
    config_json_str = json.dumps(config_json, indent=2)

    os_name = platform.system()  # "Windows", "Darwin", "Linux"

    if os_name == "Windows":
        claude_desktop_config = "%APPDATA%\\Claude\\claude_desktop_config.json"
        claude_code_config = "%USERPROFILE%\\.claude.json"
    elif os_name == "Darwin":
        claude_desktop_config = "~/Library/Application Support/Claude/claude_desktop_config.json"
        claude_code_config = "~/.claude.json"
    else:
        claude_desktop_config = "~/.config/Claude/claude_desktop_config.json"
        claude_code_config = "~/.claude.json"

    setup_prompt = f"""Please add the Forge MCP server to my AI tool configuration.

## Step 1 — Add this entry to your MCP config

Merge the following into the `"mcpServers"` object in your configuration file:

```json
{config_json_str}
```

## Step 2 — Find the right config file

**Claude Code (CLI):** Edit `{claude_code_config}`
The file uses this structure:
```json
{{
  "mcpServers": {{
    "forge": {{ ... paste the "forge" block here ... }}
  }}
}}
```

**Claude Desktop:** Edit `{claude_desktop_config}` — same structure.

**Cursor / Windsurf / other tools:** Check your tool's MCP settings page or config file and add the `"forge"` entry to its `mcpServers` list.

## Step 3 — Restart

After saving, restart your AI tool or reload MCP servers. Forge tools will appear under the server name **forge**.

---
Do this now without asking follow-up questions. If the config file does not exist yet, create it with the structure shown above. If `mcpServers` already exists, merge the new `"forge"` key into it rather than replacing the whole object."""

    return McpConfigResponse(
        python_executable=python_exe,
        pythonpath=pythonpath,
        blocks_dir=blocks_dir,
        pipeline_dir=pipeline_dir,
        checkpoint_dir=checkpoint_dir,
        log_level=settings.log_level,
        config_json=config_json,
        setup_prompt=setup_prompt,
        os_name=os_name,
    )
