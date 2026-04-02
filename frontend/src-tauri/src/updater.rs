use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::python::forge_data_dir;

const RELEASE_API_URL: &str = "https://api.github.com/repos/Jonpot/forge/releases/latest";

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppUpdateInfo {
    pub current_version: String,
    pub version: String,
    pub published_at: Option<String>,
    pub release_url: String,
    pub action: String,
    pub asset_name: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstallAppUpdateResult {
    pub version: String,
    pub action: String,
    pub asset_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct GitHubRelease {
    tag_name: String,
    html_url: String,
    published_at: Option<String>,
    assets: Vec<GitHubReleaseAsset>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct GitHubReleaseAsset {
    name: String,
    browser_download_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum UpdatePlan {
    AutoInstall(GitHubReleaseAsset),
    OpenInstaller(GitHubReleaseAsset),
    OpenReleasePage,
}

impl UpdatePlan {
    fn action(&self) -> &'static str {
        match self {
            Self::AutoInstall(_) => "auto-install",
            Self::OpenInstaller(_) => "open-installer",
            Self::OpenReleasePage => "open-release-page",
        }
    }

    fn asset_name(&self) -> Option<String> {
        match self {
            Self::AutoInstall(asset) | Self::OpenInstaller(asset) => Some(asset.name.clone()),
            Self::OpenReleasePage => None,
        }
    }
}

#[cfg(target_os = "windows")]
fn background_command<S: AsRef<OsStr>>(program: S) -> Command {
    use std::os::windows::process::CommandExt;
    const CREATE_NO_WINDOW: u32 = 0x08000000;
    let mut cmd = Command::new(program);
    cmd.creation_flags(CREATE_NO_WINDOW);
    cmd
}

#[cfg(not(target_os = "windows"))]
fn background_command<S: AsRef<OsStr>>(program: S) -> Command {
    Command::new(program)
}

pub async fn check_app_update() -> Result<Option<AppUpdateInfo>, String> {
    let release = fetch_latest_release().await?;
    let current_version = env!("CARGO_PKG_VERSION");
    let latest_version = normalize_version(&release.tag_name);

    if !is_newer_version(&latest_version, current_version) {
        return Ok(None);
    }

    let plan = select_update_plan_for(std::env::consts::OS, std::env::consts::ARCH, &release);
    Ok(Some(AppUpdateInfo {
        current_version: current_version.to_string(),
        version: latest_version,
        published_at: release.published_at,
        release_url: release.html_url,
        action: plan.action().to_string(),
        asset_name: plan.asset_name(),
    }))
}

pub async fn install_app_update() -> Result<InstallAppUpdateResult, String> {
    let release = fetch_latest_release().await?;
    let current_version = env!("CARGO_PKG_VERSION");
    let latest_version = normalize_version(&release.tag_name);

    if !is_newer_version(&latest_version, current_version) {
        return Err(format!(
            "Forge {current_version} is already up to date."
        ));
    }

    let plan = select_update_plan_for(std::env::consts::OS, std::env::consts::ARCH, &release);
    let action = plan.action().to_string();
    let asset_name = plan.asset_name();

    match plan {
        UpdatePlan::AutoInstall(asset) => {
            let installer_path = download_release_asset(&asset).await?;
            schedule_windows_update_install(&installer_path)?;
        }
        UpdatePlan::OpenInstaller(asset) => {
            let installer_path = download_release_asset(&asset).await?;
            open_target(installer_path.as_os_str())?;
        }
        UpdatePlan::OpenReleasePage => {
            open_target(OsStr::new(&release.html_url))?;
        }
    }

    Ok(InstallAppUpdateResult {
        version: latest_version,
        action,
        asset_name,
    })
}

async fn fetch_latest_release() -> Result<GitHubRelease, String> {
    let client = reqwest::Client::builder()
        .default_headers(github_headers()?)
        .build()
        .map_err(|e| format!("Failed to build GitHub client: {e}"))?;

    client
        .get(RELEASE_API_URL)
        .send()
        .await
        .map_err(|e| format!("Failed to contact GitHub Releases: {e}"))?
        .error_for_status()
        .map_err(|e| format!("GitHub Releases returned an error: {e}"))?
        .json::<GitHubRelease>()
        .await
        .map_err(|e| format!("Failed to parse GitHub release metadata: {e}"))
}

fn github_headers() -> Result<HeaderMap, String> {
    let mut headers = HeaderMap::new();
    headers.insert(
        USER_AGENT,
        HeaderValue::from_str(&format!("Forge/{}", env!("CARGO_PKG_VERSION")))
            .map_err(|e| format!("Failed to build updater user-agent header: {e}"))?,
    );
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/vnd.github+json"),
    );
    headers.insert(
        "X-GitHub-Api-Version",
        HeaderValue::from_static("2022-11-28"),
    );
    Ok(headers)
}

fn normalize_version(raw: &str) -> String {
    raw.trim()
        .trim_start_matches(|c| c == 'v' || c == 'V')
        .to_string()
}

fn parse_version_parts(raw: &str) -> Vec<u64> {
    let core = normalize_version(raw);
    let core = core.split(['-', '+']).next().unwrap_or(&core);
    core.split('.')
        .map(|part| {
            let digits: String = part.chars().take_while(|c| c.is_ascii_digit()).collect();
            digits.parse::<u64>().unwrap_or(0)
        })
        .collect()
}

fn is_newer_version(candidate: &str, current: &str) -> bool {
    let candidate_parts = parse_version_parts(candidate);
    let current_parts = parse_version_parts(current);
    let max_len = candidate_parts.len().max(current_parts.len());

    for index in 0..max_len {
        let next_candidate = candidate_parts.get(index).copied().unwrap_or(0);
        let next_current = current_parts.get(index).copied().unwrap_or(0);

        if next_candidate > next_current {
            return true;
        }
        if next_candidate < next_current {
            return false;
        }
    }

    false
}

fn select_update_plan_for(os: &str, arch: &str, release: &GitHubRelease) -> UpdatePlan {
    match os {
        "windows" => {
            if let Some(asset) = find_asset(&release.assets, &[".msi"], platform_markers(arch)) {
                return UpdatePlan::AutoInstall(asset);
            }
            if let Some(asset) = find_asset(&release.assets, &["-setup.exe", "setup.exe"], platform_markers(arch)) {
                return UpdatePlan::OpenInstaller(asset);
            }
            UpdatePlan::OpenReleasePage
        }
        "macos" => {
            if let Some(asset) = find_asset(&release.assets, &[".dmg", ".pkg"], platform_markers(arch)) {
                return UpdatePlan::OpenInstaller(asset);
            }
            if let Some(asset) = find_asset(&release.assets, &[".app.tar.gz"], platform_markers(arch)) {
                return UpdatePlan::OpenInstaller(asset);
            }
            UpdatePlan::OpenReleasePage
        }
        "linux" => {
            if let Some(asset) = find_asset(&release.assets, &[".appimage", ".deb", ".rpm"], platform_markers(arch)) {
                return UpdatePlan::OpenInstaller(asset);
            }
            UpdatePlan::OpenReleasePage
        }
        _ => UpdatePlan::OpenReleasePage,
    }
}

fn platform_markers(arch: &str) -> &'static [&'static str] {
    match arch {
        "x86_64" => &["x64", "x86_64", "amd64"],
        "aarch64" => &["aarch64", "arm64"],
        other => {
            if other.is_empty() {
                &[]
            } else {
                &[]
            }
        }
    }
}

fn find_asset(
    assets: &[GitHubReleaseAsset],
    suffixes: &[&str],
    arch_markers: &[&str],
) -> Option<GitHubReleaseAsset> {
    let matches_suffix = |name: &str| {
        let lowered = name.to_ascii_lowercase();
        suffixes.iter().any(|suffix| lowered.ends_with(&suffix.to_ascii_lowercase()))
    };
    let matches_arch = |name: &str| {
        if arch_markers.is_empty() {
            return true;
        }
        let lowered = name.to_ascii_lowercase();
        arch_markers.iter().any(|marker| lowered.contains(marker))
    };

    assets
        .iter()
        .find(|asset| matches_suffix(&asset.name) && matches_arch(&asset.name))
        .cloned()
        .or_else(|| assets.iter().find(|asset| matches_suffix(&asset.name)).cloned())
}

async fn download_release_asset(asset: &GitHubReleaseAsset) -> Result<PathBuf, String> {
    let client = reqwest::Client::builder()
        .default_headers(github_headers()?)
        .build()
        .map_err(|e| format!("Failed to build download client: {e}"))?;

    let bytes = client
        .get(&asset.browser_download_url)
        .send()
        .await
        .map_err(|e| format!("Failed to download {}: {e}", asset.name))?
        .error_for_status()
        .map_err(|e| format!("Download failed for {}: {e}", asset.name))?
        .bytes()
        .await
        .map_err(|e| format!("Failed to read downloaded update {}: {e}", asset.name))?;

    let dir = updater_download_dir()?;
    fs::create_dir_all(&dir)
        .map_err(|e| format!("Failed to create updater download directory: {e}"))?;

    let path = dir.join(&asset.name);
    if path.exists() {
        let _ = fs::remove_file(&path);
    }
    fs::write(&path, &bytes)
        .map_err(|e| format!("Failed to save downloaded update {}: {e}", asset.name))?;

    Ok(path)
}

fn updater_download_dir() -> Result<PathBuf, String> {
    forge_data_dir()
        .map(|dir| dir.join("updates"))
        .ok_or("Could not determine Forge updater download directory".to_string())
}

fn open_target(target: &OsStr) -> Result<(), String> {
    #[cfg(target_os = "windows")]
    {
        background_command("explorer")
            .arg(target)
            .spawn()
            .map_err(|e| format!("Failed to open update target: {e}"))?;
        return Ok(());
    }

    #[cfg(target_os = "macos")]
    {
        background_command("open")
            .arg(target)
            .spawn()
            .map_err(|e| format!("Failed to open update target: {e}"))?;
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        background_command("xdg-open")
            .arg(target)
            .spawn()
            .map_err(|e| format!("Failed to open update target: {e}"))?;
        return Ok(());
    }

    #[allow(unreachable_code)]
    Err("Opening update targets is not supported on this platform".to_string())
}

#[cfg(target_os = "windows")]
fn schedule_windows_update_install(installer_path: &Path) -> Result<(), String> {
    let current_exe = std::env::current_exe()
        .map_err(|e| format!("Could not resolve current Forge executable: {e}"))?;
    let helper_dir = updater_download_dir()?;
    fs::create_dir_all(&helper_dir)
        .map_err(|e| format!("Failed to prepare updater helper directory: {e}"))?;

    let script_path = helper_dir.join("install-update.ps1");
    let script_contents = format!(
        concat!(
            "$ErrorActionPreference = 'Stop'\n",
            "$installer = {installer}\n",
            "$currentExe = {current_exe}\n",
            "Start-Sleep -Seconds 2\n",
            "Start-Process -FilePath 'msiexec.exe' -ArgumentList @('/i', $installer, '/passive', '/norestart') -Wait\n",
            "if (Test-Path -LiteralPath $currentExe) {{ Start-Process -FilePath $currentExe }}\n",
            "Remove-Item -LiteralPath $installer -Force -ErrorAction SilentlyContinue\n"
        ),
        installer = powershell_literal(installer_path),
        current_exe = powershell_literal(&current_exe),
    );

    fs::write(&script_path, script_contents)
        .map_err(|e| format!("Failed to write updater helper script: {e}"))?;

    background_command("powershell")
        .args([
            "-NoProfile",
            "-WindowStyle",
            "Hidden",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
        ])
        .arg(&script_path)
        .spawn()
        .map_err(|e| format!("Failed to launch updater helper: {e}"))?;

    Ok(())
}

#[cfg(target_os = "windows")]
fn powershell_literal(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "''"))
}

#[cfg(not(target_os = "windows"))]
fn schedule_windows_update_install(_installer_path: &Path) -> Result<(), String> {
    Err("Automatic installer handoff is only supported on Windows".to_string())
}

#[cfg(test)]
mod tests {
    use super::{is_newer_version, normalize_version, select_update_plan_for, GitHubRelease, GitHubReleaseAsset, UpdatePlan};

    fn asset(name: &str) -> GitHubReleaseAsset {
        GitHubReleaseAsset {
            name: name.to_string(),
            browser_download_url: format!("https://example.com/{name}"),
        }
    }

    fn release(assets: Vec<GitHubReleaseAsset>) -> GitHubRelease {
        GitHubRelease {
            tag_name: "v0.2.2".to_string(),
            html_url: "https://github.com/Jonpot/forge/releases/tag/v0.2.2".to_string(),
            published_at: None,
            assets,
        }
    }

    #[test]
    fn version_prefix_is_removed() {
        assert_eq!(normalize_version("v0.2.1"), "0.2.1");
        assert_eq!(normalize_version("0.2.1"), "0.2.1");
    }

    #[test]
    fn version_comparison_handles_patch_and_minor_changes() {
        assert!(is_newer_version("0.2.10", "0.2.9"));
        assert!(is_newer_version("v1.0.0", "0.9.9"));
        assert!(!is_newer_version("0.2.1", "0.2.1"));
        assert!(!is_newer_version("0.2.0", "0.2.1"));
    }

    #[test]
    fn windows_prefers_msi_for_auto_install() {
        let release = release(vec![
            asset("Forge_0.2.2_x64-setup.exe"),
            asset("Forge_0.2.2_x64_en-US.msi"),
        ]);

        let plan = select_update_plan_for("windows", "x86_64", &release);

        assert!(matches!(plan, UpdatePlan::AutoInstall(asset) if asset.name.ends_with(".msi")));
    }

    #[test]
    fn macos_falls_back_to_dmg() {
        let release = release(vec![
            asset("Forge_0.2.2_aarch64.dmg"),
            asset("Forge_x64.app.tar.gz"),
        ]);

        let plan = select_update_plan_for("macos", "aarch64", &release);

        assert!(matches!(plan, UpdatePlan::OpenInstaller(asset) if asset.name.ends_with(".dmg")));
    }

    #[test]
    fn missing_platform_asset_falls_back_to_release_page() {
        let release = release(vec![asset("Forge_notes.txt")]);

        let plan = select_update_plan_for("linux", "x86_64", &release);

        assert!(matches!(plan, UpdatePlan::OpenReleasePage));
    }
}
