#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use serde::Serialize;
use std::collections::HashSet;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::UNIX_EPOCH;
use sysinfo::Disks;

mod partitioning;

// --- DATENMODELLE ---

#[derive(Serialize)]
struct SystemDisk {
    name: String,
    mount_point: String,
    total_space: u64,
    available_space: u64,
    is_removable: bool,
    is_mounted: bool,
    device: Option<String>,
}

#[derive(Serialize)]
struct FileNode {
    name: String,
    #[serde(rename = "path")]
    path: String,
    // Nivo braucht 'value' bei Blättern. Wir geben es auch bei Ordnern mit,
    // damit wir Tooltips korrekt anzeigen können.
    value: u64,
    // Wir nutzen Box, um unendliche Rekursion im Typ zu vermeiden
    children: Option<Vec<Box<FileNode>>>,

    // Zusatzinfos für UI
    #[serde(rename = "displaySize")]
    display_size: String,
    #[serde(rename = "fileCount")]
    file_count: u64,
    #[serde(rename = "modifiedAt", skip_serializing_if = "Option::is_none")]
    modified_at: Option<u64>,
}

// --- HILFS-STRUCTS FÜR ALGORITHMUS ---

// Identifiziert eine Datei eindeutig auf dem Mac
#[derive(Hash, Eq, PartialEq, Clone, Copy)]
struct FileID {
    dev: u64,
    ino: u64,
}

// --- COMMANDS ---

#[tauri::command]
fn get_disks(include_system: bool) -> Vec<SystemDisk> {
    let disks = Disks::new_with_refreshed_list();
    let mut disks_list = Vec::new();
    let mut mounted_points = HashSet::new();
    let mut seen_mounts = HashSet::new();
    let root_name = disks
        .list()
        .iter()
        .find(|d| d.mount_point().to_string_lossy() == "/")
        .map(|d| d.name().to_string_lossy().to_string());

    for disk in disks.list() {
        let mount = disk.mount_point().to_string_lossy().to_string();
        let is_root = mount == "/";
        let is_volumes = mount.starts_with("/Volumes");
        if include_system && !is_root && !is_volumes {
            if let Some(ref root) = root_name {
                if disk.name().to_string_lossy() == root.as_str() {
                    continue;
                }
            }
        }
        if (is_root || is_volumes || include_system) && seen_mounts.insert(mount.clone()) {
            disks_list.push(SystemDisk {
                name: disk.name().to_string_lossy().to_string(),
                mount_point: mount.clone(),
                total_space: disk.total_space(),
                available_space: disk.available_space(),
                is_removable: disk.is_removable(),
                is_mounted: true,
                device: None,
            });
            mounted_points.insert(mount);
        }
    }

    // Ergänze unmontierte Devices (macOS)
    disks_list.extend(get_unmounted_disks(&mounted_points, include_system));
    disks_list
}

fn get_unmounted_disks(mounted_points: &HashSet<String>, include_system: bool) -> Vec<SystemDisk> {
    #[cfg(target_os = "macos")]
    {
        use plist::Value;
        use std::process::Command;

        let mut result = Vec::new();
        let mut seen_devices = HashSet::new();

        let output = Command::new("diskutil").args(["list", "-plist"]).output();
        let output = match output {
            Ok(o) if o.status.success() => o,
            _ => return result,
        };

        let plist = match Value::from_reader_xml(&output.stdout[..]) {
            Ok(p) => p,
            Err(_) => return result,
        };

        let dict = match plist.as_dictionary() {
            Some(d) => d,
            None => return result,
        };

        let all_disks = match dict.get("AllDisksAndPartitions") {
            Some(Value::Array(arr)) => arr,
            _ => return result,
        };

        for entry in all_disks {
            if let Some(disk_dict) = entry.as_dictionary() {
                collect_unmounted_from_dict(disk_dict, mounted_points, &mut seen_devices, &mut result, include_system);

                if let Some(Value::Array(parts)) = disk_dict.get("Partitions") {
                    for part in parts {
                        if let Some(part_dict) = part.as_dictionary() {
                            collect_unmounted_from_dict(part_dict, mounted_points, &mut seen_devices, &mut result, include_system);
                        }
                    }
                }
            }
        }

        result
    }

    #[cfg(not(target_os = "macos"))]
    {
        Vec::new()
    }
}

#[cfg(target_os = "macos")]
fn collect_unmounted_from_dict(
    dict: &plist::Dictionary,
    mounted_points: &HashSet<String>,
    seen_devices: &mut HashSet<String>,
    result: &mut Vec<SystemDisk>,
    include_system: bool,
) {
    let mount_point = dict
        .get("MountPoint")
        .and_then(|v| v.as_string())
        .unwrap_or("")
        .to_string();

    if !mount_point.is_empty() {
        return;
    }

    let device = dict
        .get("DeviceIdentifier")
        .and_then(|v| v.as_string())
        .map(|s| s.to_string());

    if let Some(dev) = &device {
        if seen_devices.contains(dev) {
            return;
        }
        seen_devices.insert(dev.clone());
    }

    let name = dict
        .get("VolumeName")
        .and_then(|v| v.as_string())
        .or_else(|| dict.get("DeviceIdentifier").and_then(|v| v.as_string()))
        .unwrap_or("Unbekannt")
        .to_string();

    if mounted_points.contains(&mount_point) {
        return;
    }

    let total_space = dict
        .get("Size")
        .and_then(|v| v.as_unsigned_integer())
        .unwrap_or(0);

    let internal = dict
        .get("Internal")
        .and_then(|v| v.as_boolean())
        .unwrap_or(true);

    if !include_system && internal {
        return;
    }

    result.push(SystemDisk {
        name,
        mount_point,
        total_space,
        available_space: 0,
        is_removable: !internal,
        is_mounted: false,
        device,
    });
}

#[tauri::command]
fn scan_directory(path: String) -> FileNode {
    // HashSet für Hardlink-Erkennung (Baobab Logik)
    let mut seen_inodes = HashSet::new();

    // Starte Scan mit max Tiefe 5 (Performance)
    scan_recursive(Path::new(&path), 0, 5, &mut seen_inodes)
}

fn scan_recursive(path: &Path, depth: usize, max_depth: usize, seen: &mut HashSet<FileID>) -> FileNode {
    let name = path
        .file_name()
        .unwrap_or(path.as_os_str())
        .to_string_lossy()
        .to_string();
    let path_string = path.to_string_lossy().to_string();

    // 1. Metadaten holen (Fehler ignorieren -> Größe 0)
    let meta = fs::symlink_metadata(path).ok();

    // 2. Größe berechnen (Baobab Style: Allocated Blocks)
    let mut size = 0;
    let mut is_dir = false;
    let mut modified_at: Option<u64> = None;

    if let Some(m) = &meta {
        is_dir = m.is_dir();

        if let Ok(modified) = m.modified() {
            if let Ok(duration) = modified.duration_since(UNIX_EPOCH) {
                modified_at = Some(duration.as_secs());
            }
        }

        // HARDLINK CHECK
        let file_id = FileID {
            dev: m.dev(),
            ino: m.ino(),
        };

        if is_dir || seen.insert(file_id) {
            size = m.blocks() * 512;
        } else {
            size = 0;
        }
    }

    // 3. Rekursion (nur wenn Ordner und Tiefe ok)
    let mut children = Vec::new();
    let mut file_count: u64 = if is_dir { 0 } else { 1 };

    if is_dir && depth < max_depth {
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                let child_node = scan_recursive(&entry.path(), depth + 1, max_depth, seen);
                size += child_node.value;
                file_count += child_node.file_count;
                children.push(Box::new(child_node));
            }
        }
    }

    // 4. Sortieren & Gruppieren
    children.sort_by(|a, b| b.value.cmp(&a.value));

    if size > 0 {
        let threshold = size / 100;
        let mut keep = Vec::new();
        let mut other_sum: u64 = 0;
        let mut other_count: u64 = 0;

        for child in children.into_iter() {
            if child.value < threshold {
                other_sum += child.value;
                other_count += child.file_count;
            } else {
                keep.push(child);
            }
        }

        if other_sum > 0 {
            keep.push(Box::new(FileNode {
                name: "Sonstiges".to_string(),
                path: path_string.clone(),
                value: other_sum,
                children: None,
                display_size: format_bytes(other_sum),
                file_count: other_count,
                modified_at: None,
            }));
        }

        children = keep;
    }

    FileNode {
        name,
        path: path_string,
        value: size,
        children: if children.is_empty() { None } else { Some(children) },
        display_size: format_bytes(size),
        file_count,
        modified_at,
    }
}

#[tauri::command]
fn open_in_finder(path: String) -> Result<(), String> {
    open::that(path).map_err(|e| e.to_string())
}

#[tauri::command]
fn move_to_trash(path: String) -> Result<(), String> {
    trash::delete(path).map_err(|e| e.to_string())
}

// Hilfsfunktion für schöne Strings direkt aus Rust
fn format_bytes(bytes: u64) -> String {
    const UNIT: u64 = 1024;
    if bytes < UNIT {
        return format!("{} B", bytes);
    }
    let div = UNIT as f64;
    let exp = (bytes as f64).log(div) as i32;
    let pre = "KMGTPE".chars().nth((exp - 1) as usize).unwrap_or('?');
    let val = (bytes as f64) / div.powi(exp);
    format!("{:.1} {}B", val, pre)
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .invoke_handler(tauri::generate_handler![
            get_disks,
            scan_directory,
            open_in_finder,
            move_to_trash,
            partitioning::get_partition_devices,
            partitioning::wipe_device,
            partitioning::create_partition_table,
            partitioning::create_partition,
            partitioning::delete_partition,
            partitioning::format_partition,
            partitioning::set_label_uuid,
            partitioning::install_sudoers_helper,
            partitioning::mount_disk,
            partitioning::mount_volume,
            partitioning::check_partition,
            partitioning::resize_partition,
            partitioning::move_partition,
            partitioning::copy_partition,
            partitioning::get_sidecar_status,
            partitioning::get_partition_bounds,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
