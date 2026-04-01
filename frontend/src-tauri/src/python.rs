use log::info;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::time::Duration;

const INSTALL_STATE_FILE: &str = "install_state.json";

/// Create a Command that won't spawn a visible console window on Windows.
#[cfg(target_os = "windows")]
fn silent_command<S: AsRef<std::ffi::OsStr>>(program: S) -> Command {
    use std::os::windows::process::CommandExt;
    const CREATE_NO_WINDOW: u32 = 0x08000000;
    let mut cmd = Command::new(program);
    cmd.creation_flags(CREATE_NO_WINDOW);
    cmd
}

/// On macOS, GUI apps launched from Finder inherit a minimal PATH that typically
/// excludes Homebrew (/opt/homebrew/bin, /usr/local/bin) and other user-installed
/// Python locations. Augment PATH so subprocesses can find Python.
#[cfg(target_os = "macos")]
fn silent_command<S: AsRef<std::ffi::OsStr>>(program: S) -> Command {
    let mut cmd = Command::new(program);
    let extra = "/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/local/sbin:/usr/bin:/bin:/usr/sbin:/sbin";
    let current = std::env::var("PATH").unwrap_or_default();
    let new_path = if current.is_empty() {
        extra.to_string()
    } else {
        format!("{extra}:{current}")
    };
    cmd.env("PATH", new_path);
    cmd
}

#[cfg(not(any(target_os = "windows", target_os = "macos")))]
fn silent_command<S: AsRef<std::ffi::OsStr>>(program: S) -> Command {
    Command::new(program)
}

/// Represents the state of the Python backend lifecycle.
#[derive(Debug, Clone, Serialize)]
#[allow(dead_code)]
pub enum BackendStatus {
    NotStarted,
    PythonNotFound,
    CreatingVenv,
    InstallingDeps,
    StartingServer,
    Ready { port: u16 },
    Error { message: String },
}

/// Holds the current backend status, shared across Tauri commands.
pub struct BackendState {
    pub status: Mutex<BackendStatus>,
    pub process: Mutex<Option<std::process::Child>>,
}

impl Default for BackendState {
    fn default() -> Self {
        Self {
            status: Mutex::new(BackendStatus::NotStarted),
            process: Mutex::new(None),
        }
    }
}

pub fn forge_data_dir() -> Option<PathBuf> {
    dirs::data_local_dir().map(|d| d.join("Forge"))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstallState {
    pub forge_package_hash: String,
    pub requirements_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSyncPlan {
    pub refresh_forge_package: bool,
    pub refresh_dependencies: bool,
}

impl InstallSyncPlan {
    pub fn needs_work(&self) -> bool {
        self.refresh_forge_package || self.refresh_dependencies
    }
}

fn install_state_path() -> Option<PathBuf> {
    forge_data_dir().map(|d| d.join(INSTALL_STATE_FILE))
}

pub fn load_install_state() -> Option<InstallState> {
    let path = install_state_path()?;
    let data = fs::read_to_string(path).ok()?;
    serde_json::from_str(&data).ok()
}

pub fn save_install_state(state: &InstallState) -> Result<(), String> {
    let path = install_state_path().ok_or("Could not determine install state path")?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create install state directory: {e}"))?;
    }
    let json = serde_json::to_string_pretty(state)
        .map_err(|e| format!("Failed to serialize install state: {e}"))?;
    fs::write(&path, json).map_err(|e| format!("Failed to write install state: {e}"))?;
    info!("[install_state] Saved install state to {}", path.display());
    Ok(())
}

pub fn bundled_install_state(forge_root: &PathBuf) -> Result<InstallState, String> {
    Ok(InstallState {
        forge_package_hash: hash_install_inputs(
            forge_root,
            &["pyproject.toml", "backend", "blocks", "Forge"],
        )?,
        requirements_hash: hash_install_inputs(forge_root, &["requirements.txt"])?,
    })
}

pub fn determine_install_sync(
    current: &InstallState,
    installed: Option<&InstallState>,
    venv_is_new: bool,
    auto_update_dependencies: bool,
) -> InstallSyncPlan {
    if venv_is_new {
        return InstallSyncPlan {
            refresh_forge_package: true,
            refresh_dependencies: true,
        };
    }

    let Some(installed) = installed else {
        // Migration path from older releases: sync everything once so the
        // existing venv is brought under manifest management.
        return InstallSyncPlan {
            refresh_forge_package: true,
            refresh_dependencies: true,
        };
    };

    InstallSyncPlan {
        refresh_forge_package: installed.forge_package_hash != current.forge_package_hash,
        refresh_dependencies: auto_update_dependencies
            || installed.requirements_hash != current.requirements_hash,
    }
}

fn hash_install_inputs(forge_root: &Path, relative_paths: &[&str]) -> Result<String, String> {
    let mut hasher = Sha256::new();
    for relative in relative_paths {
        let path = forge_root.join(relative);
        hash_path_recursive(forge_root, &path, &mut hasher)?;
    }
    let digest = hasher.finalize();
    Ok(format!("{digest:x}"))
}

fn normalized_relative_path(base: &Path, path: &Path) -> String {
    path.strip_prefix(base)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn hash_path_recursive(base: &Path, path: &Path, hasher: &mut Sha256) -> Result<(), String> {
    if path.is_file() {
        hasher.update(b"file\0");
        hasher.update(normalized_relative_path(base, path).as_bytes());
        hasher.update(b"\0");

        let mut file =
            fs::File::open(path).map_err(|e| format!("Failed to read {}: {e}", path.display()))?;
        let mut buffer = [0_u8; 8192];
        loop {
            let read = file
                .read(&mut buffer)
                .map_err(|e| format!("Failed to read {}: {e}", path.display()))?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        hasher.update(b"\0");
        return Ok(());
    }

    if path.is_dir() {
        hasher.update(b"dir\0");
        hasher.update(normalized_relative_path(base, path).as_bytes());
        hasher.update(b"\0");

        let mut children = fs::read_dir(path)
            .map_err(|e| format!("Failed to walk {}: {e}", path.display()))?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .collect::<Vec<_>>();
        children.sort();

        for child in children {
            hash_path_recursive(base, &child, hasher)?;
        }
        return Ok(());
    }

    hasher.update(b"missing\0");
    hasher.update(normalized_relative_path(base, path).as_bytes());
    hasher.update(b"\0");
    Ok(())
}

pub fn venv_dir() -> Option<PathBuf> {
    forge_data_dir().map(|d| d.join("venv"))
}

pub fn venv_python() -> Option<PathBuf> {
    let venv = venv_dir()?;
    let candidates = [
        venv.join("Scripts").join("python.exe"),
        venv.join("bin").join("python.exe"),
        venv.join("bin").join("python"),
    ];
    let found = candidates.into_iter().find(|p| p.exists());
    if let Some(ref p) = found {
        info!("[venv_python] Found venv python at: {}", p.display());
        return found;
    }
    // Venv doesn't exist yet — return the platform default
    let default = if cfg!(target_os = "windows") {
        venv.join("Scripts").join("python.exe")
    } else {
        venv.join("bin").join("python")
    };
    info!(
        "[venv_python] No venv python found, defaulting to: {}",
        default.display()
    );
    Some(default)
}

/// Try to find a working Python 3.12+ on the system.
pub fn find_system_python() -> Option<String> {
    info!("[find_python] Starting Python detection...");

    if cfg!(target_os = "windows") {
        // Check what the py launcher has installed
        let installed = get_py_launcher_versions();
        info!(
            "[find_python] py launcher reports versions: {:?}",
            installed
        );

        for minor in [13, 12] {
            let ver = format!("3.{minor}");
            if installed.contains(&ver) {
                info!("[find_python] Trying py -3.{minor}...");
                let flag = format!("-3.{minor}");
                if check_python_suitability("py", &[&flag, "-c"]) {
                    info!("[find_python] ✓ Using py -{ver}");
                    return Some(format!("py -{ver}"));
                }
                info!("[find_python] ✗ py -3.{minor} not suitable");
            }
        }

        // Fallback: let py pick
        info!("[find_python] Trying py -3 (launcher default)...");
        if check_python_suitability("py", &["-3", "-c"]) {
            info!("[find_python] ✓ Using py -3");
            return Some("py -3".to_string());
        }
        info!("[find_python] ✗ py -3 not suitable");
    }

    for candidate in &["python3", "python"] {
        info!("[find_python] Trying {candidate}...");
        if check_python_suitability(candidate, &["-c"]) {
            info!("[find_python] ✓ Using {candidate}");
            return Some(candidate.to_string());
        }
        info!("[find_python] ✗ {candidate} not suitable");
    }

    // On macOS, fall back to absolute path checks. PATH augmentation in
    // silent_command should handle most cases, but this catches edge cases
    // where the binary exists but isn't on the augmented PATH.
    #[cfg(target_os = "macos")]
    {
        let macos_candidates = [
            "/opt/homebrew/bin/python3.13",
            "/opt/homebrew/bin/python3.12",
            "/opt/homebrew/bin/python3",
            "/usr/local/bin/python3.13",
            "/usr/local/bin/python3.12",
            "/usr/local/bin/python3",
            "/usr/bin/python3",
        ];
        for candidate in &macos_candidates {
            if std::path::Path::new(candidate).exists() {
                info!("[find_python] Trying absolute path {candidate}...");
                if check_python_suitability(candidate, &["-c"]) {
                    info!("[find_python] ✓ Found macOS Python at {candidate}");
                    return Some(candidate.to_string());
                }
            }
        }
    }

    info!("[find_python] No suitable Python found");
    None
}

/// Query the `py` launcher for installed Python versions.
fn get_py_launcher_versions() -> Vec<String> {
    info!("[py_versions] Running py --list...");
    let output = silent_command("py").args(["--list"]).output();

    match output {
        Ok(o) if o.status.success() => {
            let text = String::from_utf8_lossy(&o.stdout);
            info!("[py_versions] Raw output:\n{text}");
            let versions: Vec<String> = text
                .lines()
                .filter_map(|line| {
                    // Extract "3.XX" from lines like " -V:3.12 *  C:\Python312\python.exe"
                    let trimmed = line.trim();
                    if let Some(idx) = trimmed.find("3.") {
                        let rest = &trimmed[idx..];
                        let ver: String = rest
                            .chars()
                            .take_while(|c| c.is_ascii_digit() || *c == '.')
                            .collect();
                        // Must be at least "3.XX"
                        if ver.len() >= 4
                            || (ver.len() == 3
                                && ver.chars().last().map_or(false, |c| c.is_ascii_digit()))
                        {
                            Some(ver)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect();
            info!("[py_versions] Parsed versions: {versions:?}");
            versions
        }
        Ok(o) => {
            info!("[py_versions] py --list failed with status: {}", o.status);
            vec![]
        }
        Err(e) => {
            info!("[py_versions] py --list error: {e}");
            vec![]
        }
    }
}

/// Check that a python candidate is 3.12+, not MSYS2, and responds within 10 seconds.
fn check_python_suitability(cmd: &str, flag_args: &[&str]) -> bool {
    let script = concat!(
        "import sys, struct\n",
        "v = sys.version_info\n",
        "if 'GCC' in sys.version and ('MSYS' in sys.version or 'MinGW' in sys.version ",
        "or 'msys' in sys.executable.lower() or 'mingw' in sys.executable.lower()):\n",
        "    sys.exit(1)\n",
        "if v.major != 3 or v.minor < 12:\n",
        "    sys.exit(1)\n",
        "print(f'{v.major}.{v.minor}.{v.micro}')\n",
    );

    let mut args: Vec<&str> = flag_args.to_vec();
    args.push(script);

    info!(
        "[check_python] Running: {cmd} {}",
        args.join(" ").chars().take(60).collect::<String>()
    );

    match silent_command(cmd).args(&args).spawn() {
        Ok(mut child) => match child.wait_timeout(Duration::from_secs(10)) {
            Ok(Some(status)) => {
                info!("[check_python] {cmd} exited with: {status}");
                status.success()
            }
            Ok(None) => {
                info!("[check_python] {cmd} timed out after 10s, killing");
                let _ = child.kill();
                let _ = child.wait();
                false
            }
            Err(e) => {
                info!("[check_python] {cmd} wait error: {e}");
                let _ = child.kill();
                false
            }
        },
        Err(e) => {
            info!("[check_python] Failed to spawn {cmd}: {e}");
            false
        }
    }
}

pub fn venv_exists() -> bool {
    let exists = venv_python().map(|p| p.exists()).unwrap_or(false);
    info!("[venv_exists] {exists}");
    exists
}

/// Create the managed venv using the system Python.
pub fn create_venv(system_python: &str) -> Result<(), String> {
    let venv_path = venv_dir().ok_or("Could not determine Forge data directory")?;
    info!("[create_venv] Target: {}", venv_path.display());

    // Ensure parent directory exists
    if let Some(parent) = venv_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create Forge data directory: {e}"))?;
    }

    // Remove existing venv directory entirely
    if venv_path.exists() {
        info!("[create_venv] Removing existing venv directory...");
        match std::fs::remove_dir_all(&venv_path) {
            Ok(_) => info!("[create_venv] Removed successfully"),
            Err(e) => {
                info!("[create_venv] remove_dir_all failed: {e}");
                return Err(format!(
                    "Could not remove old environment. A program may be using files in {}. \
                     Close any Python programs or terminals, then retry. ({})",
                    venv_path.display(),
                    e
                ));
            }
        }
    }

    // Handle compound commands like "py -3.12"
    let parts: Vec<&str> = system_python.split_whitespace().collect();
    let (cmd, prefix_args) = (parts[0], &parts[1..]);

    let mut args: Vec<&str> = prefix_args.to_vec();
    args.extend_from_slice(&["-m", "venv"]);
    let venv_str = venv_path.to_string_lossy().to_string();
    args.push(&venv_str);

    info!("[create_venv] Running: {cmd} {}", args.join(" "));

    let output = silent_command(cmd)
        .args(&args)
        .output()
        .map_err(|e| format!("Failed to run {} -m venv: {e}", system_python))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        info!("[create_venv] FAILED. stderr: {stderr}");
        info!("[create_venv] stdout: {stdout}");
        return Err(format!("venv creation failed: {stderr}"));
    }

    info!("[create_venv] Success");

    // Verify the python binary exists
    if let Some(py) = venv_python() {
        if py.exists() {
            info!("[create_venv] Verified venv python at: {}", py.display());
        } else {
            info!(
                "[create_venv] WARNING: venv python NOT found at: {}",
                py.display()
            );
            // List what actually got created
            if let Ok(entries) = std::fs::read_dir(&venv_path) {
                let names: Vec<String> = entries
                    .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().to_string()))
                    .collect();
                info!("[create_venv] Venv directory contains: {names:?}");
            }
        }
    }

    Ok(())
}

/// Run a pip command and return a clear error on failure.
fn run_pip(python: &PathBuf, cwd: &PathBuf, args: &[&str]) -> Result<(), String> {
    let mut full_args = vec!["-m", "pip"];
    full_args.extend_from_slice(args);

    info!(
        "[pip] Running: {} {}",
        python.display(),
        full_args.join(" ")
    );

    let output = silent_command(python)
        .current_dir(cwd)
        .args(&full_args)
        .output()
        .map_err(|e| format!("Failed to run pip: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        info!("[pip] FAILED. stderr: {stderr}");
        info!("[pip] stdout: {stdout}");
        let detail = if !stderr.trim().is_empty() {
            &stderr
        } else {
            &stdout
        };
        return Err(format!("pip install failed: {detail}"));
    }

    info!("[pip] Success");
    Ok(())
}

fn prepare_install_dir(forge_root: &PathBuf) -> Result<(PathBuf, PathBuf), String> {
    let python = venv_python().ok_or("Managed venv python not found")?;
    info!("[install_forge] Using python: {}", python.display());
    info!("[install_forge] Forge root: {}", forge_root.display());

    if !python.exists() {
        return Err(format!(
            "Venv python not found at {}. Venv may not have been created correctly.",
            python.display()
        ));
    }

    // Copy source to a writable temp directory for pip install.
    // The bundled app directory may be read-only (e.g. /Applications on macOS,
    // Program Files on Windows), and pip needs to create egg-info/build artifacts.
    let temp_dir = std::env::temp_dir().join("forge-install-src");
    if temp_dir.exists() {
        let _ = fs::remove_dir_all(&temp_dir);
    }
    info!(
        "[install_forge] Copying source to temp dir: {}",
        temp_dir.display()
    );
    copy_dir_recursive(forge_root, &temp_dir)
        .map_err(|e| format!("Failed to copy source to temp dir: {e}"))?;

    Ok((python, temp_dir))
}

fn install_forge_package(
    python: &PathBuf,
    install_dir: &PathBuf,
    with_deps: bool,
) -> Result<(), String> {
    let mut args = vec!["install", "--force-reinstall"];
    if !with_deps {
        args.push("--no-deps");
    }
    args.push(".");
    args.push("--quiet");

    info!("[install_forge] Installing forge...");
    run_pip(python, install_dir, &args)
}

fn install_forge_requirements(python: &PathBuf, install_dir: &PathBuf) -> Result<bool, String> {
    let requirements = install_dir.join("requirements.txt");
    if requirements.exists() {
        info!("[install_forge] Installing requirements.txt...");
        run_pip(
            python,
            install_dir,
            &["install", "-r", "requirements.txt", "--quiet"],
        )?;
        return Ok(true);
    }
    info!("[install_forge] requirements.txt not found, skipping dependency sync");
    Ok(false)
}

/// Sync the Forge package and/or third-party Python dependencies inside the managed venv.
/// Copies source to a temp directory first because the bundled location
/// (e.g. C:\Program Files\Forge) is read-only and pip needs to write build artifacts.
pub fn sync_forge_install(forge_root: &PathBuf, plan: &InstallSyncPlan) -> Result<(), String> {
    if !plan.needs_work() {
        info!("[install_forge] No package or dependency refresh needed");
        return Ok(());
    }

    let (python, install_dir) = prepare_install_dir(forge_root)?;
    let result: Result<(), String> = (|| {
        let mut requirements_installed = false;

        if plan.refresh_dependencies {
            info!("[install_forge] Upgrading pip...");
            let _ = run_pip(
                &python,
                &install_dir,
                &["install", "--upgrade", "pip", "--quiet"],
            );
            requirements_installed = install_forge_requirements(&python, &install_dir)?;
        }

        if plan.refresh_forge_package {
            install_forge_package(
                &python,
                &install_dir,
                plan.refresh_dependencies && !requirements_installed,
            )?;
        }

        Ok(())
    })();

    if let Err(e) = fs::remove_dir_all(&install_dir) {
        info!(
            "[install_forge] Failed to clean temp dir {}: {}",
            install_dir.display(),
            e
        );
    }

    result?;

    info!(
        "[install_forge] Managed install sync complete (forge package: {}, dependencies: {})",
        plan.refresh_forge_package, plan.refresh_dependencies
    );
    Ok(())
}

/// Recursively copy a directory and all its contents.
fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let dest_path = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&entry.path(), &dest_path)?;
        } else {
            fs::copy(entry.path(), &dest_path)?;
        }
    }
    Ok(())
}

/// Start the FastAPI backend server.
pub fn start_backend(_forge_root: &PathBuf, port: u16) -> Result<std::process::Child, String> {
    let python = venv_python().ok_or("Managed venv python not found")?;
    info!(
        "[start_backend] Spawning uvicorn on port {port} with python: {}",
        python.display()
    );

    // Use the Forge data dir as the working directory (writable), not Program Files.
    // The backend package is already installed in the venv, so cwd doesn't matter for imports.
    let work_dir = forge_data_dir().unwrap_or_else(|| std::env::temp_dir());
    info!("[start_backend] Working directory: {}", work_dir.display());

    // Pipe stderr so we can log backend errors
    let mut child = silent_command(&python)
        .current_dir(&work_dir)
        .args([
            "-m",
            "uvicorn",
            "backend.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            &port.to_string(),
        ])
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("Failed to start backend: {e}"))?;

    info!("[start_backend] Spawned with PID: {}", child.id());

    // Spawn a thread to read stderr and log it
    if let Some(stderr) = child.stderr.take() {
        std::thread::spawn(move || {
            use std::io::BufRead;
            let reader = std::io::BufReader::new(stderr);
            for line in reader.lines() {
                match line {
                    Ok(l) => info!("[backend-stderr] {}", l),
                    Err(_) => break,
                }
            }
        });
    }

    Ok(child)
}

/// Extension trait to add wait_timeout to Child on all platforms.
trait ChildExt {
    fn wait_timeout(&mut self, dur: Duration) -> std::io::Result<Option<std::process::ExitStatus>>;
}

impl ChildExt for std::process::Child {
    fn wait_timeout(&mut self, dur: Duration) -> std::io::Result<Option<std::process::ExitStatus>> {
        let start = std::time::Instant::now();
        loop {
            match self.try_wait()? {
                Some(status) => return Ok(Some(status)),
                None => {
                    if start.elapsed() > dur {
                        return Ok(None);
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{determine_install_sync, InstallState};

    fn state(package: &str, requirements: &str) -> InstallState {
        InstallState {
            forge_package_hash: package.to_string(),
            requirements_hash: requirements.to_string(),
        }
    }

    #[test]
    fn new_venv_requires_full_sync() {
        let current = state("pkg-a", "req-a");
        let installed = state("pkg-a", "req-a");

        let plan = determine_install_sync(&current, Some(&installed), true, false);

        assert!(plan.refresh_forge_package);
        assert!(plan.refresh_dependencies);
    }

    #[test]
    fn missing_install_state_triggers_one_time_full_sync() {
        let current = state("pkg-a", "req-a");

        let plan = determine_install_sync(&current, None, false, false);

        assert!(plan.refresh_forge_package);
        assert!(plan.refresh_dependencies);
    }

    #[test]
    fn package_hash_change_refreshes_only_forge_package() {
        let current = state("pkg-b", "req-a");
        let installed = state("pkg-a", "req-a");

        let plan = determine_install_sync(&current, Some(&installed), false, false);

        assert!(plan.refresh_forge_package);
        assert!(!plan.refresh_dependencies);
    }

    #[test]
    fn requirements_hash_change_refreshes_dependencies() {
        let current = state("pkg-a", "req-b");
        let installed = state("pkg-a", "req-a");

        let plan = determine_install_sync(&current, Some(&installed), false, false);

        assert!(!plan.refresh_forge_package);
        assert!(plan.refresh_dependencies);
    }

    #[test]
    fn auto_update_only_targets_dependencies_when_package_is_current() {
        let current = state("pkg-a", "req-a");
        let installed = state("pkg-a", "req-a");

        let plan = determine_install_sync(&current, Some(&installed), false, true);

        assert!(!plan.refresh_forge_package);
        assert!(plan.refresh_dependencies);
    }

    #[test]
    fn matching_state_without_auto_update_skips_sync() {
        let current = state("pkg-a", "req-a");
        let installed = state("pkg-a", "req-a");

        let plan = determine_install_sync(&current, Some(&installed), false, false);

        assert!(!plan.refresh_forge_package);
        assert!(!plan.refresh_dependencies);
    }
}
