use log::info;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Config file stored in the Forge data directory to track workspace setup.
const CONFIG_FILE: &str = "workspace.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceConfig {
    /// The user's chosen workspace directory (e.g. Documents/Forge)
    pub workspace_dir: String,
    /// Whether first-time setup has been completed
    pub setup_complete: bool,
    /// Whether to automatically refresh third-party Python dependencies on every boot.
    #[serde(default)]
    pub auto_update_packages: bool,
}

/// Returns the path to the workspace config file.
fn config_path() -> Option<PathBuf> {
    crate::python::forge_data_dir().map(|d| d.join(CONFIG_FILE))
}

/// Returns the default workspace directory (Documents/Forge).
pub fn default_workspace_dir() -> Option<PathBuf> {
    dirs::document_dir().map(|d| d.join("Forge"))
}

/// Check if first-time workspace setup has been completed.
pub fn is_setup_complete() -> bool {
    if let Some(path) = config_path() {
        if path.exists() {
            if let Ok(data) = std::fs::read_to_string(&path) {
                if let Ok(config) = serde_json::from_str::<WorkspaceConfig>(&data) {
                    return config.setup_complete;
                }
            }
        }
    }
    false
}

/// Load the saved workspace config, if it exists.
pub fn load_config() -> Option<WorkspaceConfig> {
    let path = config_path()?;
    let data = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&data).ok()
}

/// Save workspace config to the Forge data directory.
pub fn save_config(config: &WorkspaceConfig) -> Result<(), String> {
    let path = config_path().ok_or("Could not determine config path")?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create config directory: {e}"))?;
    }
    let json = serde_json::to_string_pretty(config)
        .map_err(|e| format!("Failed to serialize config: {e}"))?;
    std::fs::write(&path, json)
        .map_err(|e| format!("Failed to write config: {e}"))?;
    info!("[workspace] Saved config to {}", path.display());
    Ok(())
}

/// Create the workspace directory structure and copy toy datasets.
pub fn initialize_workspace(workspace_dir: &str, forge_root: &PathBuf) -> Result<(), String> {
    let workspace = PathBuf::from(workspace_dir);
    info!("[workspace] Initializing workspace at: {}", workspace.display());

    // Create workspace subdirectories
    for subdir in &["pipelines", "outputs", "datasets"] {
        let dir = workspace.join(subdir);
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("Failed to create {}: {e}", dir.display()))?;
        info!("[workspace] Created: {}", dir.display());
    }

    // Copy toy_datasets into workspace/datasets/
    let source = forge_root.join("toy_datasets");
    let dest = workspace.join("datasets");
    if source.exists() {
        if let Ok(entries) = std::fs::read_dir(&source) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    let filename = entry.file_name();
                    let dest_file = dest.join(&filename);
                    if !dest_file.exists() {
                        std::fs::copy(&path, &dest_file)
                            .map_err(|e| format!("Failed to copy {}: {e}", filename.to_string_lossy()))?;
                        info!("[workspace] Copied: {} → {}", path.display(), dest_file.display());
                    }
                }
            }
        }
    } else {
        info!("[workspace] No toy_datasets directory found at {}", source.display());
    }

    info!("[workspace] Workspace initialized successfully");
    Ok(())
}

/// Write the .env file for the backend, pointing at the workspace directories.
/// Writes to both forge_root (for dev) and the Forge data dir (for production,
/// where the backend cwd is the data dir, not the source tree).
pub fn write_env_file(forge_root: &PathBuf, workspace_dir: &str) -> Result<(), String> {
    let workspace = PathBuf::from(workspace_dir);

    let contents = format!(
        "CHECKPOINT_DIR={checkpoints}\n\
         PIPELINE_DIR={pipelines}\n\
         BLOCKS_DIR=./blocks\n\
         DEFAULT_FILE_PATH={datasets}\n\
         LOG_LEVEL=INFO\n\
         CORS_ORIGINS=http://tauri.localhost,https://tauri.localhost,tauri://localhost,http://localhost:1420\n",
        checkpoints = workspace.join("checkpoints").to_string_lossy(),
        pipelines = workspace.join("pipelines").to_string_lossy(),
        datasets = workspace.join("datasets").to_string_lossy(),
    );

    // Write to forge_root (works in dev where cwd = repo root)
    let env_path = forge_root.join(".env");
    let _ = std::fs::write(&env_path, &contents);
    info!("[workspace] Wrote .env to {}", env_path.display());

    // Also write to the Forge data dir (production: backend cwd = data dir)
    if let Some(data_dir) = crate::python::forge_data_dir() {
        let _ = std::fs::create_dir_all(&data_dir);
        let data_env = data_dir.join(".env");
        std::fs::write(&data_env, &contents)
            .map_err(|e| format!("Failed to write .env to data dir: {e}"))?;
        info!("[workspace] Wrote .env to {}", data_env.display());
    }

    Ok(())
}
