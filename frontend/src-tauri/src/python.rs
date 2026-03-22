use log::info;
use serde::Serialize;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Mutex;
use std::time::Duration;

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
    info!("[venv_python] No venv python found, defaulting to: {}", default.display());
    Some(default)
}

/// Try to find a working Python 3.12+ on the system.
pub fn find_system_python() -> Option<String> {
    info!("[find_python] Starting Python detection...");

    if cfg!(target_os = "windows") {
        // Check what the py launcher has installed
        let installed = get_py_launcher_versions();
        info!("[find_python] py launcher reports versions: {:?}", installed);

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

    info!("[find_python] No suitable Python found");
    None
}

/// Query the `py` launcher for installed Python versions.
fn get_py_launcher_versions() -> Vec<String> {
    info!("[py_versions] Running py --list...");
    let output = Command::new("py").args(["--list"]).output();

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
                        if ver.len() >= 4 || (ver.len() == 3 && ver.chars().last().map_or(false, |c| c.is_ascii_digit())) {
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

    info!("[check_python] Running: {cmd} {}", args.join(" ").chars().take(60).collect::<String>());

    match Command::new(cmd).args(&args).spawn() {
        Ok(mut child) => {
            match child.wait_timeout(Duration::from_secs(10)) {
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
            }
        }
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
                    venv_path.display(), e
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

    let output = Command::new(cmd)
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
            info!("[create_venv] WARNING: venv python NOT found at: {}", py.display());
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

    info!("[pip] Running: {} {}", python.display(), full_args.join(" "));

    let output = Command::new(python)
        .current_dir(cwd)
        .args(&full_args)
        .output()
        .map_err(|e| format!("Failed to run pip: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        info!("[pip] FAILED. stderr: {stderr}");
        info!("[pip] stdout: {stdout}");
        let detail = if !stderr.trim().is_empty() { &stderr } else { &stdout };
        return Err(format!("pip install failed: {detail}"));
    }

    info!("[pip] Success");
    Ok(())
}

/// Install the Forge backend into the managed venv.
pub fn install_forge(forge_root: &PathBuf) -> Result<(), String> {
    let python = venv_python().ok_or("Managed venv python not found")?;
    info!("[install_forge] Using python: {}", python.display());
    info!("[install_forge] Forge root: {}", forge_root.display());

    if !python.exists() {
        return Err(format!(
            "Venv python not found at {}. Venv may not have been created correctly.",
            python.display()
        ));
    }

    // Upgrade pip first
    info!("[install_forge] Upgrading pip...");
    let _ = run_pip(&python, forge_root, &["install", "--upgrade", "pip", "--quiet"]);

    // Install forge in editable mode
    info!("[install_forge] Installing forge (editable)...");
    run_pip(&python, forge_root, &["install", "-e", ".", "--quiet"])?;

    // Also install requirements.txt for pinned versions
    let requirements = forge_root.join("requirements.txt");
    if requirements.exists() {
        info!("[install_forge] Installing requirements.txt...");
        run_pip(&python, forge_root, &["install", "-r", "requirements.txt", "--quiet"])?;
    }

    info!("[install_forge] All dependencies installed");
    Ok(())
}

/// Start the FastAPI backend server.
pub fn start_backend(forge_root: &PathBuf, port: u16) -> Result<std::process::Child, String> {
    let python = venv_python().ok_or("Managed venv python not found")?;
    info!("[start_backend] Spawning uvicorn on port {port} with python: {}", python.display());

    let child = Command::new(&python)
        .current_dir(forge_root)
        .args([
            "-m", "uvicorn",
            "backend.main:app",
            "--host", "127.0.0.1",
            "--port", &port.to_string(),
        ])
        .spawn()
        .map_err(|e| format!("Failed to start backend: {e}"))?;

    info!("[start_backend] Spawned with PID: {}", child.id());
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
