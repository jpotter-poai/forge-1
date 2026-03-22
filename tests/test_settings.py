from __future__ import annotations

from pathlib import Path

from backend.settings import Settings


def test_settings_from_env_reads_dotenv_file(tmp_path: Path, monkeypatch) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "CHECKPOINT_DIR=./checkpoints",
                "PIPELINE_DIR=./pipelines",
                "BLOCKS_DIR=./blocks",
                "DEFAULT_FILE_PATH=./toy_datasets/",
                "LOG_LEVEL=DEBUG",
                "CORS_ORIGINS=http://localhost:5173,http://localhost:4173",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DEFAULT_FILE_PATH", raising=False)
    monkeypatch.delenv("CORS_ORIGINS", raising=False)

    settings = Settings.from_env()

    assert settings.default_file_path == "./toy_datasets/"
    assert settings.log_level == "DEBUG"
    assert settings.cors_origins == ["http://localhost:5173", "http://localhost:4173"]


def test_settings_environment_overrides_dotenv(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".env").write_text("DEFAULT_FILE_PATH=./toy_datasets/\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DEFAULT_FILE_PATH", "./override/")

    settings = Settings.from_env()

    assert settings.default_file_path == "./override/"
