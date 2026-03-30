mod commands;
mod python;
mod workspace;

use python::BackendState;
use tauri::Manager;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(
            tauri_plugin_log::Builder::default()
                .level(log::LevelFilter::Info)
                .max_file_size(2_000_000) // 2 MB cap, rotates automatically (keeps 1 file)
                .target(tauri_plugin_log::Target::new(
                    tauri_plugin_log::TargetKind::Stdout,
                ))
                .target(tauri_plugin_log::Target::new(
                    tauri_plugin_log::TargetKind::LogDir { file_name: Some("forge.log".into()) },
                ))
                .build(),
        )
        .manage(BackendState::default())
        .invoke_handler(tauri::generate_handler![
            commands::get_backend_status,
            commands::check_python,
            commands::check_venv,
            commands::reset_setup,
            commands::setup_and_start,
            commands::check_workspace_setup,
            commands::get_default_workspace,
            commands::initialize_workspace,
            commands::load_settings,
            commands::save_settings,
            commands::get_log_path,
            commands::update_packages,
        ])
        .on_window_event(|window, event| {
            if let tauri::WindowEvent::Destroyed = event {
                kill_backend_from_window(window);
            }
        })
        .build(tauri::generate_context!())
        .expect("error while building Forge")
        .run(|app, event| {
            if let tauri::RunEvent::Exit = event {
                kill_backend_from_handle(app);
            }
        });
}

fn kill_backend_from_window(window: &tauri::Window) {
    let state: tauri::State<BackendState> = window.state();
    kill_backend_state(&state);
}

fn kill_backend_from_handle(app: &tauri::AppHandle) {
    let state: tauri::State<BackendState> = app.state();
    kill_backend_state(&state);
}

fn kill_backend_state(state: &tauri::State<BackendState>) {
    let mut proc = state.process.lock().unwrap();
    if let Some(child) = proc.as_mut() {
        log::info!("Killing backend process");
        let _ = child.kill();
        let _ = child.wait();
    }
    *proc = None;
}
