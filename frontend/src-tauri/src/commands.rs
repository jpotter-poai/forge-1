use crate::python::{self, BackendState, BackendStatus};
use crate::workspace;
use std::path::PathBuf;
use tauri::{Emitter, Manager, State};

/// Check the current status of the backend.
#[tauri::command]
pub fn get_backend_status(state: State<BackendState>) -> BackendStatus {
    state.status.lock().unwrap().clone()
}

/// Check if Python 3.12+ is available on the system.
#[tauri::command]
pub fn check_python() -> Result<String, String> {
    python::find_system_python().ok_or_else(|| "Python 3.12+ not found on your system".to_string())
}

/// Check if the managed venv already exists.
#[tauri::command]
pub fn check_venv() -> bool {
    python::venv_exists()
}

/// Reset setup status so a retry can proceed.
#[tauri::command]
pub fn reset_setup(state: State<BackendState>) {
    let mut status = state.status.lock().unwrap();
    *status = BackendStatus::NotStarted;
    log::info!("[setup] Status reset to NotStarted for retry");
}

/// Run the full first-time setup: create venv, install deps, start server.
/// Emits status events so the frontend can show progress.
/// All blocking operations run on a background thread so the UI stays responsive.
#[tauri::command]
pub async fn setup_and_start(
    app: tauri::AppHandle,
    state: State<'_, BackendState>,
) -> Result<u16, String> {
    let port: u16 = 40964;

    // Prevent concurrent setup runs (React StrictMode can double-fire)
    {
        let status = state.status.lock().unwrap();
        match *status {
            BackendStatus::CreatingVenv
            | BackendStatus::InstallingDeps
            | BackendStatus::StartingServer => {
                log::info!("[setup] Already in progress, ignoring duplicate call");
                return Err("Setup already in progress".to_string());
            }
            BackendStatus::Ready { port } => {
                log::info!("[setup] Already ready on port {port}, ignoring");
                return Ok(port);
            }
            _ => {}
        }
    }

    // Kill any existing backend process (e.g. from a previous failed attempt)
    {
        let mut proc = state.process.lock().unwrap();
        if let Some(child) = proc.as_mut() {
            log::info!("[setup] Killing previous backend process");
            let _ = child.kill();
            let _ = child.wait();
        }
        *proc = None;
    }

    // Step 1: Find Python
    let system_python = tokio::task::spawn_blocking(|| python::find_system_python())
        .await
        .map_err(|e| format!("Task failed: {e}"))?
        .ok_or("Python 3.12+ not found. Please install Python from python.org.")?;

    // Step 2: Create venv if needed
    let venv_is_new = !python::venv_exists();
    if venv_is_new {
        {
            let mut status = state.status.lock().unwrap();
            *status = BackendStatus::CreatingVenv;
        }
        let _ = app.emit("backend-status", BackendStatus::CreatingVenv);

        // Yield so the event can be delivered before blocking
        tokio::task::yield_now().await;

        let py = system_python.clone();
        tokio::task::spawn_blocking(move || python::create_venv(&py))
            .await
            .map_err(|e| format!("Task failed: {e}"))??;
    }

    // Step 3: Sync the managed Forge install.
    // Forge itself is refreshed automatically when the bundled package changes.
    // Third-party Python dependencies only refresh automatically on fresh venvs,
    // when their manifest changes, or when auto_update_packages is enabled.
    let auto_update_dependencies = workspace::load_config()
        .map(|c| c.auto_update_packages)
        .unwrap_or(false);

    let forge_root = resolve_forge_root(&app)?;
    let current_install_state = tokio::task::spawn_blocking({
        let root_clone = forge_root.clone();
        move || python::bundled_install_state(&root_clone)
    })
    .await
    .map_err(|e| format!("Task failed: {e}"))??;
    let installed_state = python::load_install_state();
    let sync_plan = python::determine_install_sync(
        &current_install_state,
        installed_state.as_ref(),
        venv_is_new,
        auto_update_dependencies,
    );

    if sync_plan.needs_work() {
        {
            let mut status = state.status.lock().unwrap();
            *status = BackendStatus::InstallingDeps;
        }
        let _ = app.emit("backend-status", BackendStatus::InstallingDeps);
        tokio::task::yield_now().await;

        let root_clone = forge_root.clone();
        let plan_clone = sync_plan.clone();
        tokio::task::spawn_blocking(move || python::sync_forge_install(&root_clone, &plan_clone))
            .await
            .map_err(|e| format!("Task failed: {e}"))??;
        python::save_install_state(&current_install_state)?;
    } else {
        log::info!("[setup] Forge package and dependency sync not needed");
    }

    // Ensure .env exists in the data dir (for production backend cwd)
    if let Some(config) = workspace::load_config() {
        if let Err(e) = workspace::write_env_file(&forge_root, &config.workspace_dir) {
            log::warn!("[setup] Failed to write .env: {e}");
        }
    }

    // Step 4: Start the backend
    {
        let mut status = state.status.lock().unwrap();
        *status = BackendStatus::StartingServer;
    }
    let _ = app.emit("backend-status", BackendStatus::StartingServer);
    tokio::task::yield_now().await;

    let child = python::start_backend(&forge_root, port)?;

    // Store the child process so we can kill it on exit
    {
        let mut proc = state.process.lock().unwrap();
        *proc = Some(child);
    }

    // Wait for the server to be ready (poll /api/blocks)
    wait_for_backend(port).await?;

    // Mark as ready
    {
        let mut status = state.status.lock().unwrap();
        *status = BackendStatus::Ready { port };
    }
    let _ = app.emit("backend-status", BackendStatus::Ready { port });

    Ok(port)
}

/// Resolve the Forge project root directory.
/// In development, this is the repo root.
/// In production, this will be the bundled resource directory.
fn resolve_forge_root(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    // In development, walk up from the executable to find pyproject.toml
    if cfg!(debug_assertions) {
        let mut dir =
            std::env::current_dir().map_err(|e| format!("Could not get working directory: {e}"))?;

        // Walk up looking for pyproject.toml
        loop {
            if dir.join("pyproject.toml").exists() {
                return Ok(dir);
            }
            if !dir.pop() {
                break;
            }
        }
    }

    // Fallback: try the resource directory (production build)
    let resource_dir = app
        .path()
        .resource_dir()
        .map_err(|e| format!("Could not resolve resource dir: {e}"))?;

    if resource_dir.join("pyproject.toml").exists() {
        return Ok(resource_dir);
    }

    // Tauri v2 maps "../../<file>" resources into "_up_/_up_/<file>" in the resource dir
    let prefixed = resource_dir.join("_up_").join("_up_");
    if prefixed.join("pyproject.toml").exists() {
        return Ok(prefixed);
    }

    Err("Could not find Forge project root (pyproject.toml)".to_string())
}

/// Poll the backend until it responds, with a timeout.
async fn wait_for_backend(port: u16) -> Result<(), String> {
    let url = format!("http://127.0.0.1:{port}/api/blocks");
    let client = reqwest::Client::new();
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(30);

    loop {
        if start.elapsed() > timeout {
            return Err("Backend did not start within 30 seconds".to_string());
        }

        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            _ => {}
        }

        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

// ── Workspace commands ──────────────────────────────────────────────────────

/// Check if first-time workspace setup has been completed.
#[tauri::command]
pub fn check_workspace_setup() -> bool {
    workspace::is_setup_complete()
}

/// Get the default workspace directory path (Documents/Forge).
#[tauri::command]
pub fn get_default_workspace() -> Result<String, String> {
    workspace::default_workspace_dir()
        .map(|p| p.to_string_lossy().to_string())
        .ok_or_else(|| "Could not determine Documents directory".to_string())
}

/// Load the current workspace settings.
#[tauri::command]
pub fn load_settings() -> Result<workspace::WorkspaceConfig, String> {
    workspace::load_config().ok_or_else(|| "No settings found".to_string())
}

/// Save updated workspace settings and regenerate the .env file.
#[tauri::command]
pub async fn save_settings(
    app: tauri::AppHandle,
    config: workspace::WorkspaceConfig,
) -> Result<(), String> {
    let forge_root = resolve_forge_root(&app)?;
    workspace::save_config(&config)?;
    workspace::write_env_file(&forge_root, &config.workspace_dir)?;
    Ok(())
}

/// Initialize the workspace: create directories, copy sample data, write .env.
#[tauri::command]
pub async fn initialize_workspace(
    app: tauri::AppHandle,
    workspace_dir: String,
) -> Result<(), String> {
    let forge_root = resolve_forge_root(&app)?;

    let root_clone = forge_root.clone();
    let dir_clone = workspace_dir.clone();
    tokio::task::spawn_blocking(move || workspace::initialize_workspace(&dir_clone, &root_clone))
        .await
        .map_err(|e| format!("Task failed: {e}"))??;

    // Write .env pointing at the workspace
    workspace::write_env_file(&forge_root, &workspace_dir)?;

    // Save workspace config (preserve existing auto_update_packages setting if present)
    let existing = workspace::load_config();
    let config = workspace::WorkspaceConfig {
        workspace_dir,
        setup_complete: true,
        auto_update_packages: existing.map(|c| c.auto_update_packages).unwrap_or(false),
    };
    workspace::save_config(&config)?;

    Ok(())
}

/// Return the path to the log file so the UI can show it in error messages.
#[tauri::command]
pub fn get_log_path(app: tauri::AppHandle) -> Result<String, String> {
    let log_dir = app
        .path()
        .app_log_dir()
        .map_err(|e| format!("Could not resolve log dir: {e}"))?;
    let log_file = log_dir.join("forge.log");
    Ok(log_file.to_string_lossy().to_string())
}

/// Manually trigger a dependency update for the managed venv.
/// This also repairs the installed Forge package if the bundled copy is newer.
#[tauri::command]
pub async fn update_packages(app: tauri::AppHandle) -> Result<(), String> {
    log::info!("[update_packages] Manual package update requested");
    let forge_root = resolve_forge_root(&app)?;
    let current_install_state = tokio::task::spawn_blocking({
        let root_clone = forge_root.clone();
        move || python::bundled_install_state(&root_clone)
    })
    .await
    .map_err(|e| format!("Task failed: {e}"))??;
    let installed_state = python::load_install_state();
    let mut sync_plan = python::determine_install_sync(
        &current_install_state,
        installed_state.as_ref(),
        false,
        false,
    );
    sync_plan.refresh_dependencies = true;

    let root_clone = forge_root.clone();
    let plan_clone = sync_plan.clone();
    tokio::task::spawn_blocking(move || python::sync_forge_install(&root_clone, &plan_clone))
        .await
        .map_err(|e| format!("Task failed: {e}"))??;
    python::save_install_state(&current_install_state)?;
    Ok(())
}
