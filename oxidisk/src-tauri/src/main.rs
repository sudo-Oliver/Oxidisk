#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use serde::Serialize;
use std::collections::HashSet;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use sysinfo::Disks;

// --- DATENMODELLE ---

#[derive(Serialize)]
struct SystemDisk {
    name: String,
    mount_point: String,
    total_space: u64,
    available_space: u64,
    is_removable: bool,
}

#[derive(Serialize)]
struct FileNode {
    name: String,
    // Nivo braucht 'value' bei Blättern. Wir geben es auch bei Ordnern mit,
    // damit wir Tooltips korrekt anzeigen können.
    value: u64,
    // Wir nutzen Box, um unendliche Rekursion im Typ zu vermeiden
    children: Option<Vec<Box<FileNode>>>,

    // Zusatzinfos für UI
    #[serde(rename = "displaySize")]
    display_size: String,
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
fn get_disks() -> Vec<SystemDisk> {
    let disks = Disks::new_with_refreshed_list();
    let mut disks_list = Vec::new();

    for disk in disks.list() {
        let mount = disk.mount_point().to_string_lossy().to_string();
        if mount == "/" || mount.starts_with("/Volumes") {
            disks_list.push(SystemDisk {
                name: disk.name().to_string_lossy().to_string(),
                mount_point: mount,
                total_space: disk.total_space(),
                available_space: disk.available_space(),
                is_removable: disk.is_removable(),
            });
        }
    }
    disks_list
}

#[tauri::command]
fn scan_directory(path: String) -> FileNode {
    // HashSet für Hardlink-Erkennung (Baobab Logik)
    let mut seen_inodes = HashSet::new();

    // Starte Scan mit max Tiefe 5 (Performance)
    // Wenn das Chart besser ist, können wir tiefer gehen.
    scan_recursive(Path::new(&path), 0, 5, &mut seen_inodes)
}

fn scan_recursive(path: &Path, depth: usize, max_depth: usize, seen: &mut HashSet<FileID>) -> FileNode {
    let name = path
        .file_name()
        .unwrap_or(path.as_os_str())
        .to_string_lossy()
        .to_string();

    // 1. Metadaten holen (Fehler ignorieren -> Größe 0)
    let meta = fs::symlink_metadata(path).ok();

    // 2. Größe berechnen (Baobab Style: Allocated Blocks)
    let mut size = 0;
    let mut is_dir = false;

    if let Some(m) = &meta {
        is_dir = m.is_dir();

        // HARDLINK CHECK
        // Auf macOS ist st_blocks die Anzahl der 512-Byte Blöcke
        let file_id = FileID {
            dev: m.dev(),
            ino: m.ino(),
        };

        // Wenn Inode noch nie gesehen ODER es ist ein Ordner (Ordner haben keine Hardlinks in dem Sinne)
        // (Dateien zählen wir nur einmal global)
        if is_dir || seen.insert(file_id) {
            size = m.blocks() * 512; // Wahre physikalische Größe
        } else {
            size = 0; // Schon gezählt -> 0 Bytes für diese Instanz
        }
    }

    // 3. Rekursion (nur wenn Ordner und Tiefe ok)
    let mut children = Vec::new();

    if is_dir && depth < max_depth {
        // Ignoriere Permission Errors beim Lesen des Ordners
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                let child_node = scan_recursive(&entry.path(), depth + 1, max_depth, seen);
                size += child_node.value; // Ordnergröße = Summe der Kinder + eigene Metadaten
                children.push(Box::new(child_node));
            }
        }
    }

    // 4. OPTIMIERUNG & "Polishing": Sortieren und Gruppieren
    // Wir sortieren absteigend nach Größe
    children.sort_by(|a, b| b.value.cmp(&a.value));

    // Baobab-Style: Alles < 1% des Elternordners wird zu "Sonstiges" gruppiert.
    if size > 0 {
        let threshold = size / 100;
        let mut keep = Vec::new();
        let mut other_sum: u64 = 0;

        for child in children.into_iter() {
            if child.value < threshold {
                other_sum += child.value;
            } else {
                keep.push(child);
            }
        }

        if other_sum > 0 {
            keep.push(Box::new(FileNode {
                name: "Sonstiges".to_string(),
                value: other_sum,
                children: None,
                display_size: format_bytes(other_sum),
            }));
        }

        children = keep;
    }

    FileNode {
        name,
        value: size,
        children: if children.is_empty() { None } else { Some(children) },
        display_size: format_bytes(size),
    }
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
        .invoke_handler(tauri::generate_handler![get_disks, scan_directory])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
