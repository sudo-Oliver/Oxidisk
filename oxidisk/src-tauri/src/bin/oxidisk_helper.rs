use plist::Value as PlistValue;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[path = "../partitioning/fs_driver.rs"]
mod fs_driver;

use fs_driver::{default_drivers, FileSystemDriver};

#[derive(Deserialize)]
struct HelperRequest {
    action: String,
    payload: Value,
}

#[derive(Serialize)]
struct HelperResponse {
    ok: bool,
    message: Option<String>,
    details: Option<Value>,
}

fn main() {
    let mut input = String::new();
    if std::io::stdin().read_to_string(&mut input).is_err() {
        write_response(false, Some("Failed to read request".to_string()), None);
        return;
    }

    let request: HelperRequest = match serde_json::from_str(&input) {
        Ok(req) => req,
        Err(err) => {
            write_response(false, Some(format!("Invalid request: {err}")), None);
            return;
        }
    };

    let result = match request.action.as_str() {
        "wipe_device" => handle_wipe_device(&request.payload),
        "create_partition_table" => handle_create_partition_table(&request.payload),
        "create_partition" => handle_create_partition(&request.payload),
        "delete_partition" => handle_delete_partition(&request.payload),
        "format_partition" => handle_format_partition(&request.payload),
        "check_partition" => handle_check_partition(&request.payload),
        "resize_partition" => handle_resize_partition(&request.payload),
        "move_partition" => handle_move_partition(&request.payload),
        "copy_partition" => handle_copy_partition(&request.payload),
        "set_label_uuid" => handle_set_label_uuid(&request.payload),
        "preflight_check" => handle_preflight_check(&request.payload),
        "force_unmount" => handle_force_unmount(&request.payload),
        "apfs_list_volumes" => handle_apfs_list_volumes(&request.payload),
        "apfs_add_volume" => handle_apfs_add_volume(&request.payload),
        "apfs_delete_volume" => handle_apfs_delete_volume(&request.payload),
        "flash_image" => handle_flash_image(&request.payload),
        "get_journal" => handle_get_journal(),
        "clear_journal" => handle_clear_journal(),
        _ => Err("Unknown action".to_string()),
    };

    match result {
        Ok(details) => write_response(true, None, details),
        Err(message) => write_response(false, Some(message), None),
    }
}

fn handle_wipe_device(payload: &Value) -> Result<Option<Value>, String> {
    let device_identifier = read_string(payload, "deviceIdentifier")?;
    let table_type = read_string(payload, "tableType")?;
    let format_type = read_string(payload, "formatType")?;
    let label = read_string(payload, "label")?;

    let scheme = match table_type.to_lowercase().as_str() {
        "gpt" => "GPT",
        "mbr" => "MBR",
        other => return Err(format!("Unsupported table type: {other}")),
    };

    let device = normalize_device(&device_identifier);

    force_unmount_disk(&device)?;

    let result = match format_type.to_lowercase().as_str() {
        "exfat" => {
            run_diskutil(["eraseDisk", "ExFAT", &label, scheme, &device])?;
            Ok(Some(json!({ "device": device, "format": "ExFAT", "scheme": scheme })))
        }
        "fat32" => {
            run_diskutil(["eraseDisk", "MS-DOS", &label, scheme, &device])?;
            Ok(Some(json!({ "device": device, "format": "MS-DOS", "scheme": scheme })))
        }
        "apfs" => {
            run_diskutil(["eraseDisk", "APFS", &label, scheme, &device])?;
            Ok(Some(json!({ "device": device, "format": "APFS", "scheme": scheme })))
        }
        "ext4" => wipe_linux_device(&device, scheme, "ext4", &label),
        "ntfs" => wipe_linux_device(&device, scheme, "ntfs", &label),
        "btrfs" => wipe_linux_device(&device, scheme, "btrfs", &label),
        "xfs" => wipe_linux_device(&device, scheme, "xfs", &label),
        "f2fs" => wipe_linux_device(&device, scheme, "f2fs", &label),
        "swap" => wipe_linux_device(&device, scheme, "swap", &label),
        other => Err(format!("Unsupported format type: {other}")),
    };

    if result.is_ok() {
        sync_kernel_table(&device);
    }
    result
}

fn handle_create_partition_table(payload: &Value) -> Result<Option<Value>, String> {
    let device_identifier = read_string(payload, "deviceIdentifier")?;
    let table_type = read_string(payload, "tableType")?;

    let scheme = match table_type.to_lowercase().as_str() {
        "gpt" => "GPT",
        "mbr" => "MBR",
        other => return Err(format!("Unsupported table type: {other}")),
    };

    let device = normalize_device(&device_identifier);

    force_unmount_disk(&device)?;
    run_diskutil([
        "partitionDisk",
        &device,
        "1",
        scheme,
        "free",
        "%noformat%",
        "100%",
    ])?;

    sync_kernel_table(&device);

    Ok(Some(json!({ "device": device, "scheme": scheme })))
}

fn handle_create_partition(payload: &Value) -> Result<Option<Value>, String> {
    let device_identifier = read_string(payload, "deviceIdentifier")?;
    let format_type = read_string(payload, "formatType")?;
    let label = read_string(payload, "label")?;
    let size = read_string(payload, "size")?;

    let device = normalize_device(&device_identifier);

    force_unmount_disk(&device)?;

    let result = match format_type.to_lowercase().as_str() {
        "exfat" => {
            run_diskutil(["addPartition", &device, "ExFAT", &label, &size])?;
            Ok(Some(json!({ "device": device, "format": "ExFAT", "size": size })))
        }
        "fat32" => {
            run_diskutil(["addPartition", &device, "MS-DOS", &label, &size])?;
            Ok(Some(json!({ "device": device, "format": "MS-DOS", "size": size })))
        }
        "ext4" => create_linux_partition(&device, "ext4", &label, &size),
        "ntfs" => create_linux_partition(&device, "ntfs", &label, &size),
        "btrfs" => create_linux_partition(&device, "btrfs", &label, &size),
        "xfs" => create_linux_partition(&device, "xfs", &label, &size),
        "f2fs" => create_linux_partition(&device, "f2fs", &label, &size),
        "swap" => create_linux_partition(&device, "swap", &label, &size),
        other => Err(format!("Unsupported format type: {other}")),
    };

    if result.is_ok() {
        sync_kernel_table(&device);
    }
    result
}

fn handle_delete_partition(payload: &Value) -> Result<Option<Value>, String> {
    let partition_identifier = read_string(payload, "partitionIdentifier")?;
    let device = normalize_device(&partition_identifier);

    maybe_swapoff(&device)?;
    force_unmount_disk(&device)?;

    run_diskutil(["eraseVolume", "free", "none", &device])?;

    sync_kernel_table(&device);

    Ok(Some(json!({ "partition": device })))
}

fn handle_format_partition(payload: &Value) -> Result<Option<Value>, String> {
    let partition_identifier = read_string(payload, "partitionIdentifier")?;
    let format_type = read_string(payload, "formatType")?;
    let label = read_string(payload, "label")?;

    let device = normalize_device(&partition_identifier);

    maybe_swapoff(&device)?;
    force_unmount_disk(&device)?;

    let result = match format_type.to_lowercase().as_str() {
        "exfat" => {
            run_diskutil(["eraseVolume", "ExFAT", &label, &device])?;
            Ok(Some(json!({ "device": device, "format": "ExFAT" })))
        }
        "fat32" => {
            run_diskutil(["eraseVolume", "MS-DOS", &label, &device])?;
            Ok(Some(json!({ "device": device, "format": "MS-DOS" })))
        }
        "apfs" => {
            run_diskutil(["eraseVolume", "APFS", &label, &device])?;
            Ok(Some(json!({ "device": device, "format": "APFS" })))
        }
        "ext4" => format_linux_partition(&device, "ext4", &label),
        "ntfs" => format_linux_partition(&device, "ntfs", &label),
        "btrfs" => format_linux_partition(&device, "btrfs", &label),
        "xfs" => format_linux_partition(&device, "xfs", &label),
        "f2fs" => format_linux_partition(&device, "f2fs", &label),
        "swap" => format_linux_partition(&device, "swap", &label),
        other => Err(format!("Unsupported format type: {other}")),
    };

    if result.is_ok() {
        sync_kernel_table(&device);
    }
    result
}

fn handle_set_label_uuid(payload: &Value) -> Result<Option<Value>, String> {
    let partition_identifier = read_string(payload, "partitionIdentifier")?;
    let device = normalize_device(&partition_identifier);

    let label = payload
        .get("label")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string());
    let uuid = payload
        .get("uuid")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string());

    if label.is_none() && uuid.is_none() {
        return Err("No label or UUID provided".to_string());
    }

    let fs_type = detect_fs_type(&device)?;
    match fs_type.as_str() {
        "apfs" => {
            if let Some(new_label) = label.as_ref() {
                run_diskutil(["renameVolume", &device, new_label])?;
            }
            if let Some(new_uuid) = uuid.as_ref() {
                run_diskutil(["apfs", "changeVolumeUUID", &device, new_uuid])?;
            }
        }
        "ext4" | "ntfs" | "btrfs" | "xfs" | "f2fs" | "swap" => {
            if let Some(driver) = driver_for(&fs_type) {
                if let Some(new_label) = label.as_ref() {
                    if let Some((bin, args)) = driver.label_command(&device, new_label) {
                        run_sidecar_stream(&bin, args)?;
                    } else {
                        return Err("Label change not supported".to_string());
                    }
                }
                if let Some(new_uuid) = uuid.as_ref() {
                    validate_uuid(new_uuid)?;
                    if let Some((bin, args)) = driver.uuid_command(&device, new_uuid) {
                        run_sidecar_stream(&bin, args)?;
                    } else {
                        return Err("UUID change not supported".to_string());
                    }
                }
            }
        }
        "exfat" | "fat32" => {
            if let Some(new_label) = label.as_ref() {
                run_diskutil(["renameVolume", &device, new_label])?;
            }
            if uuid.is_some() {
                return Err("FAT/ExFAT UUID change is not supported".to_string());
            }
        }
        _ => return Err("Unsupported filesystem for label/UUID".to_string()),
    }

    sync_kernel_table(&device);

    Ok(Some(json!({ "device": device, "label": label, "uuid": uuid, "fs": fs_type })))
}

fn handle_apfs_list_volumes(payload: &Value) -> Result<Option<Value>, String> {
    let container_identifier = read_string(payload, "containerIdentifier")?;
    let normalized = normalize_device(&container_identifier);
    let needle = strip_device_prefix(&normalized);

    let output = Command::new("diskutil")
        .args(["apfs", "list", "-plist"])
        .output()
        .map_err(|e| format!("diskutil failed: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("diskutil error: {stderr}"));
    }

    let plist = PlistValue::from_reader_xml(&output.stdout[..]).map_err(|e| e.to_string())?;
    let dict = plist
        .as_dictionary()
        .ok_or_else(|| "Invalid plist".to_string())?;

    let containers = dict
        .get("Containers")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "Invalid APFS plist structure".to_string())?;

    for container in containers {
        let container_dict = match container.as_dictionary() {
            Some(d) => d,
            None => continue,
        };

        if !container_matches(container_dict, &needle) {
            continue;
        }

        let container_identifier = plist_string(container_dict, &["ContainerReference", "DeviceIdentifier", "ContainerIdentifier"])
            .unwrap_or_else(|| needle.clone());
        let container_uuid = plist_string(container_dict, &["APFSContainerUUID", "ContainerUUID"]);
        let capacity = plist_u64(container_dict, &["CapacityCeiling", "Capacity"]);
        let capacity_free = plist_u64(container_dict, &["CapacityFree"]);
        let capacity_used = plist_u64(container_dict, &["CapacityInUse", "CapacityUsed"]);

        let mut volume_entries: Vec<&PlistValue> = Vec::new();
        if let Some(arr) = container_dict.get("Volumes").and_then(|v| v.as_array()) {
            volume_entries.extend(arr.iter());
        } else if let Some(arr) = container_dict.get("APFSVolumes").and_then(|v| v.as_array()) {
            volume_entries.extend(arr.iter());
        }

        let mut volumes = Vec::new();
        for volume in volume_entries {
            let volume_dict = match volume.as_dictionary() {
                Some(d) => d,
                None => continue,
            };

            let identifier = plist_string(volume_dict, &["DeviceIdentifier", "DeviceReference"]).unwrap_or_default();
            let name = plist_string(volume_dict, &["Name", "VolumeName"]).unwrap_or_default();
            let roles = plist_string_array(volume_dict, &["Roles", "APFSVolumeRoles"]);
            let size = plist_u64(volume_dict, &["CapacityInUse", "CapacityInUseBytes", "CapacityUsed"]).unwrap_or(0);
            let used = plist_u64(volume_dict, &["CapacityInUse", "CapacityInUseBytes", "CapacityUsed"]).unwrap_or(0);
            let mount_point = plist_string(volume_dict, &["MountPoint"]);

            volumes.push(json!({
                "identifier": identifier,
                "name": name,
                "roles": roles,
                "size": size,
                "used": used,
                "mountPoint": mount_point,
            }));
        }

        return Ok(Some(json!({
            "containerIdentifier": container_identifier,
            "containerUuid": container_uuid,
            "capacity": capacity,
            "capacityFree": capacity_free,
            "capacityUsed": capacity_used,
            "volumes": volumes,
        })));
    }

    Err("APFS container not found".to_string())
}

fn handle_apfs_add_volume(payload: &Value) -> Result<Option<Value>, String> {
    let container_identifier = read_string(payload, "containerIdentifier")?;
    let name = read_string(payload, "name")?;
    let role = payload
        .get("role")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_default();

    let container = normalize_device(&container_identifier);
    if role.trim().is_empty() || role == "None" {
        run_diskutil(["apfs", "addVolume", &container, "APFS", &name])?;
    } else {
        run_diskutil(["apfs", "addVolume", &container, "APFS", &name, "-role", &role])?;
    }

    Ok(Some(json!({ "container": container, "name": name, "role": role })))
}

fn handle_apfs_delete_volume(payload: &Value) -> Result<Option<Value>, String> {
    let volume_identifier = read_string(payload, "volumeIdentifier")?;
    let volume = normalize_device(&volume_identifier);
    run_diskutil(["apfs", "deleteVolume", &volume])?;
    Ok(Some(json!({ "volume": volume })))
}

fn handle_flash_image(payload: &Value) -> Result<Option<Value>, String> {
    let source_path = read_string(payload, "sourcePath")?;
    let target_device = read_string(payload, "targetDevice")?;
    let verify = payload
        .get("verify")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let device = normalize_device(&target_device);
    let raw_device = raw_device_path(&device);

    let file_size = std::fs::metadata(&source_path)
        .map_err(|e| format!("Image read failed: {e}"))?
        .len();

    let disk_size = read_disk_size(&device).unwrap_or(0);
    if disk_size > 0 && file_size > disk_size {
        return Err("Image is larger than target device".to_string());
    }

    emit_log("flash", "Unmounting target disk");
    force_unmount_disk(&device)?;

    emit_log("flash", "Writing image");
    let source_hash = flash_write_with_hash(&source_path, &raw_device, file_size)?;

    let mut verified_hash: Option<String> = None;
    if verify {
        emit_log("flash", "Verifying image");
        let hash = flash_verify_with_hash(&raw_device, file_size)?;
        if hash != source_hash {
            return Err("Verification failed: checksum mismatch".to_string());
        }
        verified_hash = Some(hash);
    }

    sync_kernel_table(&device);

    Ok(Some(json!({
        "target": device,
        "bytes": file_size,
        "sourceHash": source_hash,
        "verifiedHash": verified_hash,
        "verified": verify,
    })))
}

fn handle_preflight_check(payload: &Value) -> Result<Option<Value>, String> {
    let operation = payload
        .get("operation")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown")
        .to_string();
    let device_identifier = payload
        .get("partitionIdentifier")
        .and_then(|value| value.as_str())
        .or_else(|| payload.get("deviceIdentifier").and_then(|value| value.as_str()))
        .ok_or_else(|| "Missing device identifier".to_string())?;
    let format_type = payload
        .get("formatType")
        .and_then(|value| value.as_str())
        .map(|value| value.to_lowercase());
    let new_size = payload
        .get("newSize")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string());

    let device = normalize_device(device_identifier);
    let fs_type = match &format_type {
        Some(fs) => fs.clone(),
        None => detect_fs_type(&device).unwrap_or_else(|_| "unknown".to_string()),
    };

    let mut blockers: Vec<String> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();

    let battery = read_battery_status();
    if let Some(info) = &battery {
        if info.is_laptop && !info.on_ac {
            if let Some(percent) = info.percent {
                if percent < 30 {
                    blockers.push("Bitte Netzteil anschliessen (Akkustand zu niedrig).".to_string());
                }
            }
        }
    }

    let sidecars = required_sidecars(&operation, &fs_type);
    for sidecar in &sidecars {
        if !sidecar.found {
            blockers.push(format!("Sidecar fehlt: {}", sidecar.name));
        }
    }

    let mut busy_processes: Vec<Value> = Vec::new();
    if let Ok(Some(mount_point)) = read_mount_point(&device) {
        match list_open_processes(&mount_point) {
            Ok(processes) => {
                if !processes.is_empty() {
                    blockers.push("Volume ist noch in Benutzung.".to_string());
                }
                for proc_info in processes {
                    busy_processes.push(json!({
                        "pid": proc_info.pid,
                        "command": proc_info.command,
                    }));
                }
            }
            Err(err) => warnings.push(format!("lsof fehlgeschlagen: {err}")),
        }
    }

    let fs_check = if matches!(operation.as_str(), "resize" | "move") {
        run_quick_fs_check(&device, &fs_type).ok()
    } else {
        None
    };
    if let Some(check) = &fs_check {
        if !check.ok {
            warnings.push("Dateisystem-Pruefung meldet Fehler. Reparatur empfohlen.".to_string());
        }
    }

    if let Some(size) = &new_size {
        if let Ok(new_bytes) = parse_size_bytes(size) {
            if let Some(used_bytes) = volume_used_bytes(&device) {
                let min_bytes = ((used_bytes as f64) * 1.05).ceil() as u64;
                if new_bytes < min_bytes {
                    blockers.push("Zielgroesse ist kleiner als belegter Speicher (mit Puffer).".to_string());
                }
            }
        }
    }

    if is_boot_volume(&device) {
        warnings.push("Achtung: Partition gehoert zu einer macOS-Installation.".to_string());
    }

    let ok = blockers.is_empty();
    Ok(Some(json!({
        "ok": ok,
        "operation": operation,
        "device": device,
        "fs": fs_type,
        "blockers": blockers,
        "warnings": warnings,
        "busyProcesses": busy_processes,
        "battery": battery.map(|info| json!({
            "isLaptop": info.is_laptop,
            "onAc": info.on_ac,
            "percent": info.percent,
        })),
        "sidecars": sidecars.into_iter().map(|item| json!({
            "name": item.name,
            "found": item.found,
            "path": item.path,
        })).collect::<Vec<Value>>(),
        "fsCheck": fs_check.map(|check| json!({
            "ok": check.ok,
            "output": check.output,
        })),
    })))
}

fn handle_force_unmount(payload: &Value) -> Result<Option<Value>, String> {
    let device_identifier = payload
        .get("partitionIdentifier")
        .and_then(|value| value.as_str())
        .or_else(|| payload.get("deviceIdentifier").and_then(|value| value.as_str()))
        .ok_or_else(|| "Missing device identifier".to_string())?;
    let device = normalize_device(device_identifier);

    let mut killed: Vec<Value> = Vec::new();
    if let Ok(Some(mount_point)) = read_mount_point(&device) {
        if let Ok(processes) = list_open_processes(&mount_point) {
            for proc_info in processes {
                let _ = Command::new("kill")
                    .args(["-TERM", &proc_info.pid.to_string()])
                    .output();
                killed.push(json!({
                    "pid": proc_info.pid,
                    "command": proc_info.command,
                }));
            }
            std::thread::sleep(std::time::Duration::from_millis(400));
            for proc_info in &killed {
                if let Some(pid) = proc_info.get("pid").and_then(|v| v.as_i64()) {
                    let _ = Command::new("kill").args(["-KILL", &pid.to_string()]).output();
                }
            }
        }
    }

    force_unmount_disk(&device)?;

    Ok(Some(json!({ "device": device, "killed": killed })))
}

fn handle_get_journal() -> Result<Option<Value>, String> {
    let path = journal_path();
    if !path.exists() {
        return Ok(None);
    }
    let data = std::fs::read_to_string(&path).map_err(|e| format!("Journal read failed: {e}"))?;
    let value: Value = serde_json::from_str(&data).map_err(|e| format!("Journal parse failed: {e}"))?;
    Ok(Some(value))
}

fn handle_clear_journal() -> Result<Option<Value>, String> {
    clear_journal();
    Ok(Some(json!({ "cleared": true })))
}

fn handle_check_partition(payload: &Value) -> Result<Option<Value>, String> {
    let partition_identifier = read_string(payload, "partitionIdentifier")?;
    let repair = payload
        .get("repair")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let device = normalize_device(&partition_identifier);

    let fs_type = detect_fs_type(&device)?;
    let output = match fs_type.as_str() {
        "ext4" => run_sidecar_capture("e2fsck", ["-p", "-f", &device])?,
        "ntfs" => run_sidecar_capture("ntfsfix", [&device])?,
        "apfs" | "exfat" | "fat32" => {
            if repair {
                run_diskutil_capture(["repairVolume", &device])?
            } else {
                run_diskutil_capture(["verifyVolume", &device])?
            }
        }
        _ => return Err("Unsupported filesystem for check".to_string()),
    };

    Ok(Some(json!({ "device": device, "fs": fs_type, "output": output })))
}

fn handle_resize_partition(payload: &Value) -> Result<Option<Value>, String> {
    let partition_identifier = read_string(payload, "partitionIdentifier")?;
    let new_size = read_string(payload, "newSize")?;
    let device = normalize_device(&partition_identifier);

    maybe_swapoff(&device)?;
    force_unmount_disk(&device)?;

    let fs_type = detect_fs_type(&device)?;
    emit_progress("resize", 0, 100, Some("Start resize"));
    let result = match fs_type.as_str() {
        "apfs" | "hfs+" => {
            run_diskutil(["resizeVolume", &device, &new_size])?;
            emit_progress("resize", 100, 100, Some("Resize complete"));
            Ok(Some(json!({ "device": device, "fs": fs_type, "size": new_size })))
        }
        "exfat" | "fat32" => Err("Resize for FAT/exFAT not supported yet".to_string()),
        "ext4" => resize_linux_partition(&device, "ext4", &new_size),
        "ntfs" => resize_linux_partition(&device, "ntfs", &new_size),
        _ => Err("Unsupported filesystem for resize".to_string()),
    };

    if result.is_ok() {
        sync_kernel_table(&device);
    }
    result
}

fn handle_move_partition(payload: &Value) -> Result<Option<Value>, String> {
    let partition_identifier = read_string(payload, "partitionIdentifier")?;
    let new_start = read_string(payload, "newStart")?;
    let device = normalize_device(&partition_identifier);

    maybe_swapoff(&device)?;
    force_unmount_disk(&device)?;

    let target_start = parse_size_bytes(&new_start)?;
    emit_progress("move", 0, 100, Some("Start move"));
    let result = move_partition(&device, target_start)?;
    emit_progress("move", 100, 100, Some("Move complete"));
    sync_kernel_table(&device);
    Ok(result)
}

fn handle_copy_partition(payload: &Value) -> Result<Option<Value>, String> {
    let source_identifier = read_string(payload, "sourcePartition")?;
    let target_device = read_string(payload, "targetDevice")?;

    let source_device = normalize_device(&source_identifier);
    let target_disk = normalize_device(&target_device);
    let fs_type = detect_fs_type(&source_device)?;

    match fs_type.as_str() {
        "ext4" | "ntfs" | "exfat" | "fat32" => {}
        _ => return Err("Copy not supported for this filesystem".to_string()),
    }

    maybe_swapoff(&source_device)?;
    force_unmount_disk(&source_device)?;
    force_unmount_disk(&target_disk)?;

    emit_progress("copy", 0, 100, Some("Prepare target"));

    let source_info = read_partition_info(&source_device)?;
    let size_mib = (source_info.partition_size / (1024 * 1024)).max(1);
    let size_arg = format!("{size_mib}M");
    let temp_label = format!("OXI_COPY_{}", current_timestamp());
    run_diskutil(["addPartition", &target_disk, "MS-DOS", &temp_label, &size_arg])?;

    let new_partition = find_partition_by_label(&temp_label)?
        .ok_or_else(|| "Failed to locate new partition".to_string())?;
    let target_partition = normalize_device(&new_partition);

    run_diskutil(["unmount", "force", &target_partition])?;

    emit_progress("copy", 5, 100, Some("Copy blocks"));
    let copy_log = copy_partition_blocks(&source_device, &target_partition, source_info.partition_size)?;

    emit_progress("copy", 85, 100, Some("Update GPT type"));
    let type_warning = set_partition_typecode(&target_partition, &fs_type)?;

    let mut warnings = Vec::new();
    if let Some(warn) = type_warning {
        warnings.push(warn);
    }

    emit_progress("copy", 90, 100, Some("Refresh UUID"));
    match fs_type.as_str() {
        "ext4" => {
            if let Err(err) = run_sidecar("tune2fs", ["-U", "random", &target_partition]) {
                warnings.push(format!("UUID refresh failed: {err}"));
            }
        }
        "ntfs" => {
            if let Err(err) = run_sidecar_capture("ntfslabel", ["--new-serial", &target_partition]) {
                warnings.push(format!("UUID refresh failed: {err}"));
            }
        }
        "exfat" | "fat32" => {
            warnings.push("UUID refresh not supported for FAT/ExFAT".to_string());
        }
        _ => {}
    }

    emit_progress("copy", 100, 100, Some("Copy complete"));
    sync_kernel_table(&target_partition);
    Ok(Some(json!({
        "source": source_device,
        "target": target_partition,
        "fs": fs_type,
        "output": copy_log,
        "warnings": warnings,
    })))
}

fn read_string(payload: &Value, key: &str) -> Result<String, String> {
    payload
        .get(key)
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
        .ok_or_else(|| format!("Missing field: {key}"))
}

struct BatteryStatus {
    is_laptop: bool,
    on_ac: bool,
    percent: Option<u32>,
}

struct SidecarCheck {
    name: String,
    found: bool,
    path: Option<String>,
}

struct FsCheckResult {
    ok: bool,
    output: String,
}

struct ProcessInfo {
    pid: i32,
    command: String,
}

fn read_battery_status() -> Option<BatteryStatus> {
    let output = Command::new("pmset").args(["-g", "batt"]).output().ok()?;
    let text = String::from_utf8_lossy(&output.stdout).to_string();
    if text.to_lowercase().contains("no batteries") {
        return Some(BatteryStatus {
            is_laptop: false,
            on_ac: true,
            percent: None,
        });
    }

    let on_ac = text.contains("AC Power");
    let percent = text
        .split('%')
        .next()
        .and_then(|part| part.split_whitespace().last())
        .and_then(|digits| digits.parse::<u32>().ok());

    Some(BatteryStatus {
        is_laptop: true,
        on_ac,
        percent,
    })
}

fn read_mount_point(device: &str) -> Result<Option<String>, String> {
    let output = Command::new("diskutil")
        .args(["info", "-plist", device])
        .output()
        .map_err(|e| format!("diskutil failed: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("diskutil error: {stderr}"));
    }
    let plist = PlistValue::from_reader_xml(&output.stdout[..]).map_err(|e| e.to_string())?;
    let dict = plist
        .as_dictionary()
        .ok_or_else(|| "Invalid plist".to_string())?;
    Ok(dict
        .get("MountPoint")
        .and_then(|v| v.as_string())
        .map(|s| s.to_string()))
}

fn list_open_processes(mount_point: &str) -> Result<Vec<ProcessInfo>, String> {
    let output = Command::new("lsof")
        .args(["-Fpcn", "-f", "--", mount_point])
        .output()
        .map_err(|e| format!("lsof failed: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("lsof error: {stderr}"));
    }

    let mut processes: Vec<ProcessInfo> = Vec::new();
    let mut current_pid: Option<i32> = None;
    let mut current_cmd: Option<String> = None;
    let mut seen = std::collections::HashSet::new();

    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if let Some(rest) = line.strip_prefix('p') {
            current_pid = rest.parse::<i32>().ok();
        } else if let Some(rest) = line.strip_prefix('c') {
            current_cmd = Some(rest.to_string());
        }

        if let (Some(pid), Some(cmd)) = (current_pid, current_cmd.clone()) {
            if seen.insert(pid) {
                processes.push(ProcessInfo { pid, command: cmd });
            }
            current_pid = None;
            current_cmd = None;
        }
    }

    Ok(processes)
}

fn required_sidecars(operation: &str, fs_type: &str) -> Vec<SidecarCheck> {
    let mut names: Vec<String> = Vec::new();
    if matches!(operation, "wipe" | "create" | "format") {
        if let Some(bin) = mkfs_binary_for(fs_type) {
            names.push(bin.to_string());
        }
    }
    if matches!(operation, "resize") {
        if fs_type == "ext4" {
            names.push("sgdisk".to_string());
            names.push("resize2fs".to_string());
        } else if fs_type == "ntfs" {
            names.push("sgdisk".to_string());
            names.push("ntfsresize".to_string());
        }
    }
    if matches!(operation, "move") {
        names.push("sgdisk".to_string());
    }

    names
        .into_iter()
        .map(|name| {
            let path = find_sidecar(&name).ok();
            SidecarCheck {
                name: name.clone(),
                found: path.is_some(),
                path: path.map(|p| p.display().to_string()),
            }
        })
        .collect()
}

fn mkfs_binary_for(fs_type: &str) -> Option<&'static str> {
    match fs_type {
        "ext4" => Some("mkfs.ext4"),
        "ntfs" => Some("mkfs.ntfs"),
        "btrfs" => Some("mkfs.btrfs"),
        "xfs" => Some("mkfs.xfs"),
        "f2fs" => Some("mkfs.f2fs"),
        "swap" => Some("mkswap"),
        _ => None,
    }
}

fn run_quick_fs_check(device: &str, fs_type: &str) -> Result<FsCheckResult, String> {
    let output = match fs_type {
        "ext4" => run_sidecar_capture("e2fsck", ["-n", "-f", device])?,
        "ntfs" => run_sidecar_capture("ntfsfix", ["-n", device])?,
        "apfs" | "exfat" | "fat32" => run_diskutil_capture(["verifyVolume", device])?,
        _ => return Err("Unsupported filesystem for preflight check".to_string()),
    };
    Ok(FsCheckResult { ok: true, output })
}

fn volume_used_bytes(device: &str) -> Option<u64> {
    let output = Command::new("diskutil")
        .args(["info", "-plist", device])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let plist = PlistValue::from_reader_xml(&output.stdout[..]).ok()?;
    let dict = plist.as_dictionary()?;
    dict.get("VolumeUsedSpace")
        .and_then(|v| v.as_unsigned_integer())
        .or_else(|| dict.get("UsedSpace").and_then(|v| v.as_unsigned_integer()))
        .or_else(|| dict.get("VolumeAllocatedSpace").and_then(|v| v.as_unsigned_integer()))
}

fn is_boot_volume(device: &str) -> bool {
    let output = Command::new("diskutil")
        .args(["info", "-plist", device])
        .output();
    let output = match output {
        Ok(o) if o.status.success() => o,
        _ => return false,
    };
    let plist = match PlistValue::from_reader_xml(&output.stdout[..]) {
        Ok(p) => p,
        Err(_) => return false,
    };
    let dict = match plist.as_dictionary() {
        Some(d) => d,
        None => return false,
    };
    if let Some(PlistValue::Array(roles)) = dict.get("APFSVolumeRoles") {
        for role in roles {
            if let Some(role_name) = role.as_string() {
                if role_name == "System" || role_name == "Data" {
                    return true;
                }
            }
        }
    }
    false
}

fn force_unmount_disk(device: &str) -> Result<(), String> {
    let disk = parent_disk_identifier(device).unwrap_or_else(|| device.to_string());
    let _ = run_diskutil(["unmount", "force", device]);
    run_diskutil(["unmountDisk", "force", &disk])?;
    Ok(())
}

fn sync_kernel_table(device: &str) {
    let disk = parent_disk_identifier(device).unwrap_or_else(|| device.to_string());
    let _ = run_diskutil(["quiet", "repairDisk", &disk]);
    let _ = run_diskutil(["updateDefaultPartitionOrder", &disk]);
}

fn maybe_swapoff(device: &str) -> Result<(), String> {
    let fs_type = detect_fs_type(device).unwrap_or_else(|_| "unknown".to_string());
    if fs_type != "swap" {
        return Ok(());
    }

    if Command::new("swapoff").args(["-a"]).output().is_ok() {
        return Ok(());
    }
    if let Ok(path) = find_sidecar("swapoff") {
        Command::new(&path)
            .args(["-a"])
            .output()
            .map_err(|e| format!("swapoff failed: {e}"))?;
        return Ok(());
    }

    Err("swapoff not available".to_string())
}

fn journal_path() -> PathBuf {
    PathBuf::from("/Library/Application Support/com.oliverquick.oxidisk/operation_journal.json")
}

fn write_journal(value: &Value) -> Result<(), String> {
    let path = journal_path();
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("Journal mkdir failed: {e}"))?;
    }
    let data = serde_json::to_string_pretty(value).map_err(|e| format!("Journal encode failed: {e}"))?;
    std::fs::write(&path, data).map_err(|e| format!("Journal write failed: {e}"))?;
    Ok(())
}

fn update_journal_progress(copied: u64) -> Result<(), String> {
    let path = journal_path();
    if !path.exists() {
        return Ok(());
    }
    let data = std::fs::read_to_string(&path).map_err(|e| format!("Journal read failed: {e}"))?;
    let mut value: Value = serde_json::from_str(&data).map_err(|e| format!("Journal parse failed: {e}"))?;
    value["lastCopied"] = json!(copied);
    value["updatedAt"] = json!(current_timestamp());
    write_journal(&value)
}

fn clear_journal() {
    let path = journal_path();
    let _ = std::fs::remove_file(path);
}

fn normalize_device(identifier: &str) -> String {
    if identifier.starts_with("/dev/") {
        identifier.to_string()
    } else {
        format!("/dev/{identifier}")
    }
}

fn raw_device_path(device: &str) -> String {
    if device.contains("/dev/rdisk") {
        device.to_string()
    } else if let Some(stripped) = device.strip_prefix("/dev/disk") {
        format!("/dev/rdisk{stripped}")
    } else {
        device.replace("/dev/", "/dev/r")
    }
}

fn read_disk_size(device: &str) -> Option<u64> {
    let output = Command::new("diskutil")
        .args(["info", "-plist", device])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let plist = PlistValue::from_reader_xml(&output.stdout[..]).ok()?;
    let dict = plist.as_dictionary()?;
    dict.get("TotalSize")
        .and_then(|v| v.as_unsigned_integer())
        .or_else(|| dict.get("Size").and_then(|v| v.as_unsigned_integer()))
}

fn flash_write_with_hash(source_path: &str, target_device: &str, total_bytes: u64) -> Result<String, String> {
    if total_bytes == 0 {
        return Err("Image is empty".to_string());
    }

    let mut source = std::fs::OpenOptions::new()
        .read(true)
        .open(source_path)
        .map_err(|e| format!("Open image failed: {e}"))?;
    let mut target = std::fs::OpenOptions::new()
        .write(true)
        .open(target_device)
        .map_err(|e| format!("Open target failed: {e}"))?;

    let buffer_size = 4 * 1024 * 1024;
    let mut buffer = vec![0u8; buffer_size];
    let mut remaining = total_bytes;
    let mut copied: u64 = 0;
    let progress_step: u64 = 50 * 1024 * 1024;
    let mut next_progress = progress_step;
    let mut hasher = Sha256::new();

    while remaining > 0 {
        let chunk = std::cmp::min(buffer_size as u64, remaining) as usize;
        source.read_exact(&mut buffer[..chunk]).map_err(|e| e.to_string())?;
        target.write_all(&buffer[..chunk]).map_err(|e| e.to_string())?;
        hasher.update(&buffer[..chunk]);
        remaining -= chunk as u64;
        copied += chunk as u64;
        if copied >= next_progress || remaining == 0 {
            let percent = ((copied as f64 / total_bytes as f64) * 100.0).round() as u64;
            emit_progress_bytes("flash", percent, 100, Some("Writing image"), copied, total_bytes);
            next_progress += progress_step;
        }
    }

    target.flush().map_err(|e| format!("Flush failed: {e}"))?;

    let hash = hasher.finalize();
    Ok(format!("{:x}", hash))
}

fn flash_verify_with_hash(target_device: &str, total_bytes: u64) -> Result<String, String> {
    if total_bytes == 0 {
        return Err("Image is empty".to_string());
    }

    let mut target = std::fs::OpenOptions::new()
        .read(true)
        .open(target_device)
        .map_err(|e| format!("Open target failed: {e}"))?;

    let buffer_size = 4 * 1024 * 1024;
    let mut buffer = vec![0u8; buffer_size];
    let mut remaining = total_bytes;
    let mut copied: u64 = 0;
    let progress_step: u64 = 50 * 1024 * 1024;
    let mut next_progress = progress_step;
    let mut hasher = Sha256::new();

    while remaining > 0 {
        let chunk = std::cmp::min(buffer_size as u64, remaining) as usize;
        target.read_exact(&mut buffer[..chunk]).map_err(|e| e.to_string())?;
        hasher.update(&buffer[..chunk]);
        remaining -= chunk as u64;
        copied += chunk as u64;
        if copied >= next_progress || remaining == 0 {
            let percent = ((copied as f64 / total_bytes as f64) * 100.0).round() as u64;
            emit_progress_bytes("verify", percent, 100, Some("Verifying image"), copied, total_bytes);
            next_progress += progress_step;
        }
    }

    let hash = hasher.finalize();
    Ok(format!("{:x}", hash))
}

fn create_linux_partition(device: &str, fs: &str, label: &str, size: &str) -> Result<Option<Value>, String> {
    let temp_label = format!("OXI_TMP_{}", current_timestamp());
    run_diskutil(["addPartition", device, "MS-DOS", &temp_label, size])?;

    let new_partition = find_partition_by_label(&temp_label)?
        .ok_or_else(|| "Failed to locate new partition".to_string())?;
    let new_device = normalize_device(&new_partition);

    run_diskutil(["unmount", &new_device])?;

    if let Some(driver) = driver_for(fs) {
        if let Some((bin, args)) = driver.mkfs_command(&new_device, label) {
            run_sidecar_stream(&bin, args)?;
        } else {
            return Err("Unsupported filesystem".to_string());
        }
    } else {
        return Err("Unsupported filesystem".to_string());
    }

    let warning = set_partition_typecode(&new_device, fs)?;

    Ok(Some(json!({ "device": device, "partition": new_device, "format": fs, "size": size, "warning": warning })))
}

fn wipe_linux_device(device: &str, scheme: &str, fs: &str, label: &str) -> Result<Option<Value>, String> {
    let temp_label = format!("OXI_TMP_{}", current_timestamp());
    run_diskutil(["eraseDisk", "MS-DOS", &temp_label, scheme, device])?;

    let new_partition = find_partition_by_label(&temp_label)?
        .ok_or_else(|| "Failed to locate new partition".to_string())?;
    let new_device = normalize_device(&new_partition);

    run_diskutil(["unmount", &new_device])?;

    if let Some(driver) = driver_for(fs) {
        if let Some((bin, args)) = driver.mkfs_command(&new_device, label) {
            run_sidecar_stream(&bin, args)?;
        } else {
            return Err("Unsupported filesystem".to_string());
        }
    } else {
        return Err("Unsupported filesystem".to_string());
    }

    let warning = set_partition_typecode(&new_device, fs)?;

    Ok(Some(json!({ "device": device, "partition": new_device, "format": fs, "scheme": scheme, "warning": warning })))
}

fn format_linux_partition(device: &str, fs: &str, label: &str) -> Result<Option<Value>, String> {
    run_diskutil(["unmount", "force", device])?;

    if let Some(driver) = driver_for(fs) {
        if let Some((bin, args)) = driver.mkfs_command(device, label) {
            run_sidecar_stream(&bin, args)?;
        } else {
            return Err("Unsupported filesystem".to_string());
        }
    } else {
        return Err("Unsupported filesystem".to_string());
    }

    let warning = set_partition_typecode(device, fs)?;

    Ok(Some(json!({ "device": device, "format": fs, "warning": warning })))
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn set_partition_typecode(partition: &str, fs: &str) -> Result<Option<String>, String> {
    let part_number = partition_number(partition).ok_or_else(|| "Invalid partition identifier".to_string())?;
    let disk = parent_disk_identifier(partition).ok_or_else(|| "Invalid disk identifier".to_string())?;
    let typecode = match fs {
        "ext4" | "btrfs" | "xfs" | "f2fs" => "8300",
        "ntfs" => "0700",
        "swap" => "8200",
        _ => return Ok(None),
    };

    if find_sidecar("sgdisk").is_err() {
        return Ok(Some("sgdisk not found; GPT typecode not updated".to_string()));
    }

    run_sidecar("sgdisk", ["--typecode", &format!("{part_number}:{typecode}"), &disk])?;
    Ok(None)
}

fn partition_number(device: &str) -> Option<u32> {
    let cleaned = device.trim_start_matches("/dev/");
    let start = cleaned.rfind('s')? + 1;
    cleaned[start..].parse::<u32>().ok()
}

fn parent_disk_identifier(device: &str) -> Option<String> {
    let cleaned = device.trim_start_matches("/dev/");
    let idx = cleaned.rfind('s')?;
    Some(format!("/dev/{}", &cleaned[..idx]))
}

fn parse_size_bytes(value: &str) -> Result<u64, String> {
    let trimmed = value.trim().to_lowercase();
    let (num_part, suffix) = trimmed
        .chars()
        .partition::<String, _>(|c| c.is_ascii_digit() || *c == '.');
    let number: f64 = num_part.parse().map_err(|_| "Invalid size".to_string())?;
    let multiplier = match suffix.trim() {
        "b" | "" => 1.0,
        "k" | "kb" => 1024.0,
        "m" | "mb" => 1024.0 * 1024.0,
        "g" | "gb" => 1024.0 * 1024.0 * 1024.0,
        "t" | "tb" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return Err("Invalid size suffix".to_string()),
    };
    Ok((number * multiplier).floor() as u64)
}

fn align_mib(value: u64) -> u64 {
    let mib = 1024 * 1024;
    value / mib * mib
}

#[derive(Clone)]
struct PartitionInfo {
    device: String,
    disk: String,
    partition_offset: u64,
    partition_size: u64,
    block_size: u64,
    min_start: u64,
    max_end: u64,
}

fn read_partition_info(device: &str) -> Result<PartitionInfo, String> {
    let output = Command::new("diskutil")
        .args(["info", "-plist", device])
        .output()
        .map_err(|e| format!("diskutil failed: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("diskutil error: {stderr}"));
    }

    let plist = PlistValue::from_reader_xml(&output.stdout[..]).map_err(|e| e.to_string())?;
    let dict = plist
        .as_dictionary()
        .ok_or_else(|| "Invalid plist".to_string())?;

    let partition_offset = dict
        .get("PartitionOffset")
        .and_then(|v| v.as_unsigned_integer())
        .ok_or_else(|| "PartitionOffset missing".to_string())?;
    let partition_size = dict
        .get("PartitionSize")
        .and_then(|v| v.as_unsigned_integer())
        .ok_or_else(|| "PartitionSize missing".to_string())?;
    let block_size = dict
        .get("DeviceBlockSize")
        .and_then(|v| v.as_unsigned_integer())
        .unwrap_or(512);
    let disk = dict
        .get("ParentWholeDisk")
        .and_then(|v| v.as_string())
        .map(|s| format!("/dev/{s}"))
        .ok_or_else(|| "ParentWholeDisk missing".to_string())?;

    let device_id = dict
        .get("DeviceIdentifier")
        .and_then(|v| v.as_string())
        .map(|s| format!("/dev/{s}"))
        .ok_or_else(|| "DeviceIdentifier missing".to_string())?;

    let max_end = disk_max_end(&disk, &device_id)?;
    Ok(PartitionInfo {
        device: device_id,
        disk,
        partition_offset,
        partition_size,
        block_size,
        min_start: partition_offset,
        max_end,
    })
}

fn disk_max_end(disk: &str, device: &str) -> Result<u64, String> {
    let output = Command::new("diskutil")
        .args(["info", "-plist", disk])
        .output()
        .map_err(|e| format!("diskutil failed: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("diskutil error: {stderr}"));
    }

    let plist = PlistValue::from_reader_xml(&output.stdout[..]).map_err(|e| e.to_string())?;
    let dict = plist
        .as_dictionary()
        .ok_or_else(|| "Invalid plist".to_string())?;
    let disk_size = dict
        .get("TotalSize")
        .and_then(|v| v.as_unsigned_integer())
        .or_else(|| dict.get("DiskSize").and_then(|v| v.as_unsigned_integer()))
        .ok_or_else(|| "Disk size missing".to_string())?;

    let mut next_start: Option<u64> = None;
    for part_id in list_disk_partitions(disk)? {
        let part_device = format!("/dev/{part_id}");
        if part_device == device {
            continue;
        }
        let output = Command::new("diskutil")
            .args(["info", "-plist", &part_device])
            .output()
            .map_err(|e| format!("diskutil failed: {e}"))?;
        if !output.status.success() {
            continue;
        }
        let plist = match PlistValue::from_reader_xml(&output.stdout[..]) {
            Ok(p) => p,
            Err(_) => continue,
        };
        let dict = match plist.as_dictionary() {
            Some(d) => d,
            None => continue,
        };
        let offset = match dict.get("PartitionOffset").and_then(|v| v.as_unsigned_integer()) {
            Some(o) => o,
            None => continue,
        };
        if offset > 0 {
            if next_start.map(|current| offset < current).unwrap_or(true) {
                next_start = Some(offset);
            }
        }
    }

    Ok(next_start.unwrap_or(disk_size))
}

fn list_disk_partitions(disk: &str) -> Result<Vec<String>, String> {
    let output = Command::new("diskutil")
        .args(["list", "-plist", disk])
        .output()
        .map_err(|e| format!("diskutil failed: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("diskutil error: {stderr}"));
    }

    let plist = PlistValue::from_reader_xml(&output.stdout[..]).map_err(|e| e.to_string())?;
    let dict = plist
        .as_dictionary()
        .ok_or_else(|| "Invalid plist".to_string())?;
    let partitions = match dict.get("Partitions") {
        Some(PlistValue::Array(parts)) => parts,
        _ => return Ok(Vec::new()),
    };

    let mut identifiers = Vec::new();
    for part in partitions {
        if let Some(part_dict) = part.as_dictionary() {
            if let Some(id) = part_dict
                .get("DeviceIdentifier")
                .and_then(|v| v.as_string())
            {
                identifiers.push(id.to_string());
            }
        }
    }

    Ok(identifiers)
}

fn resize_linux_partition(device: &str, fs: &str, new_size: &str) -> Result<Option<Value>, String> {
    if find_sidecar("sgdisk").is_err() {
        return Err("sgdisk is required for ext4/ntfs resize".to_string());
    }

    let new_size_bytes = parse_size_bytes(new_size)?;
    let info = read_partition_info(device)?;
    let aligned_size = align_mib(new_size_bytes);
    if aligned_size == 0 {
        return Err("Invalid size".to_string());
    }

    let start = info.partition_offset;
    let current_end = start + info.partition_size;
    let new_end = start + aligned_size;

    if new_end > info.max_end {
        return Err("New size exceeds available space".to_string());
    }

    let mut output_log = String::new();
    if new_end < current_end {
        emit_progress("resize", 10, 100, Some("Shrink filesystem"));
        let size_mib = aligned_size / (1024 * 1024);
        let size_arg = format!("{size_mib}M");
        let log = match fs {
            "ext4" => run_sidecar_capture("resize2fs", [device, &size_arg])?,
            "ntfs" => run_sidecar_capture("ntfsresize", ["-s", &size_arg, device])?,
            _ => return Err("Unsupported filesystem".to_string()),
        };
        output_log.push_str(&log);
        output_log.push_str("\n");
        emit_progress("resize", 60, 100, Some("Update partition table"));
        let table_log = resize_partition_table(&info, new_end)?;
        output_log.push_str(&table_log);
    } else if new_end > current_end {
        emit_progress("resize", 40, 100, Some("Update partition table"));
        let table_log = resize_partition_table(&info, new_end)?;
        output_log.push_str(&table_log);
        output_log.push_str("\n");
        emit_progress("resize", 70, 100, Some("Grow filesystem"));
        let log = match fs {
            "ext4" => run_sidecar_capture("resize2fs", [device])?,
            "ntfs" => run_sidecar_capture("ntfsresize", [device])?,
            _ => return Err("Unsupported filesystem".to_string()),
        };
        output_log.push_str(&log);
    }

    emit_progress("resize", 100, 100, Some("Resize complete"));

    Ok(Some(json!({ "device": device, "fs": fs, "size": new_size, "output": output_log.trim() })))
}

fn resize_partition_table(info: &PartitionInfo, new_end: u64) -> Result<String, String> {
    let start_sector = info.partition_offset / info.block_size;
    let end_sector = (new_end / info.block_size) - 1;
    let part_number = partition_number(&info.device).ok_or_else(|| "Invalid partition".to_string())?;

    let output = run_sidecar_capture(
        "sgdisk",
        [
            "--delete",
            &part_number.to_string(),
            "--new",
            &format!("{part_number}:{start_sector}:{end_sector}"),
            &info.disk,
        ],
    )?;
    Ok(output)
}

fn move_partition(device: &str, new_start: u64) -> Result<Option<Value>, String> {
    if find_sidecar("sgdisk").is_err() {
        return Err("sgdisk is required for move".to_string());
    }

    let info = read_partition_info(device)?;
    let aligned_start = align_mib(new_start);
    if aligned_start < info.min_start || aligned_start >= info.max_end {
        return Err("Invalid target start".to_string());
    }

    let size = info.partition_size;
    let old_start = info.partition_offset;
    let old_end = old_start + size;
    let new_end = aligned_start + size;
    if new_end > info.max_end {
        return Err("Move exceeds available space".to_string());
    }
    if aligned_start < old_end && new_end > old_start {
        return Err("Move would overlap existing data".to_string());
    }

    let journal = json!({
        "operation": "move",
        "device": info.device,
        "disk": info.disk,
        "srcOffset": old_start,
        "dstOffset": aligned_start,
        "size": size,
        "blockSize": info.block_size,
        "lastCopied": 0,
        "updatedAt": current_timestamp(),
    });
    write_journal(&journal)?;

    let move_log = copy_blocks(&info.disk, old_start, aligned_start, size, true)?;

    let start_sector = aligned_start / info.block_size;
    let end_sector = (new_end / info.block_size) - 1;
    let part_number = partition_number(device).ok_or_else(|| "Invalid partition".to_string())?;
    let gpt_log = run_sidecar_capture(
        "sgdisk",
        [
            "--delete",
            &part_number.to_string(),
            "--new",
            &format!("{part_number}:{start_sector}:{end_sector}"),
            &info.disk,
        ],
    )?;

    clear_journal();
    Ok(Some(json!({ "device": device, "newStart": aligned_start, "output": format!("{move_log}\n{gpt_log}").trim() })))
}

fn copy_blocks(disk: &str, src_offset: u64, dst_offset: u64, size: u64, journal: bool) -> Result<String, String> {
    let mut reader = std::fs::OpenOptions::new()
        .read(true)
        .open(disk)
        .map_err(|e| format!("Open source failed: {e}"))?;
    let mut writer = std::fs::OpenOptions::new()
        .write(true)
        .open(disk)
        .map_err(|e| format!("Open target failed: {e}"))?;

    let buffer_size = 4 * 1024 * 1024;
    let mut buffer = vec![0u8; buffer_size];
    let mut remaining = size;

    let mut copied: u64 = 0;
    let progress_step: u64 = 50 * 1024 * 1024;
    let mut next_progress = progress_step;

    if dst_offset > src_offset {
        let mut position = size;
        while position > 0 {
            let chunk = std::cmp::min(buffer_size as u64, position) as usize;
            position -= chunk as u64;
            let read_pos = src_offset + position;
            let write_pos = dst_offset + position;
            reader.seek(SeekFrom::Start(read_pos)).map_err(|e| e.to_string())?;
            reader.read_exact(&mut buffer[..chunk]).map_err(|e| e.to_string())?;
            writer.seek(SeekFrom::Start(write_pos)).map_err(|e| e.to_string())?;
            writer.write_all(&buffer[..chunk]).map_err(|e| e.to_string())?;
            remaining -= chunk as u64;
            copied += chunk as u64;
            if copied >= next_progress {
                let percent = ((copied as f64 / size as f64) * 100.0).round() as u64;
                emit_progress_bytes("move", percent, 100, Some("Copying blocks"), copied, size);
                if journal {
                    let _ = update_journal_progress(copied);
                }
                next_progress += progress_step;
            }
        }
    } else {
        let mut position = 0u64;
        while position < size {
            let chunk = std::cmp::min(buffer_size as u64, size - position) as usize;
            let read_pos = src_offset + position;
            let write_pos = dst_offset + position;
            reader.seek(SeekFrom::Start(read_pos)).map_err(|e| e.to_string())?;
            reader.read_exact(&mut buffer[..chunk]).map_err(|e| e.to_string())?;
            writer.seek(SeekFrom::Start(write_pos)).map_err(|e| e.to_string())?;
            writer.write_all(&buffer[..chunk]).map_err(|e| e.to_string())?;
            position += chunk as u64;
            remaining -= chunk as u64;
            copied += chunk as u64;
            if copied >= next_progress {
                let percent = ((copied as f64 / size as f64) * 100.0).round() as u64;
                emit_progress_bytes("move", percent, 100, Some("Copying blocks"), copied, size);
                if journal {
                    let _ = update_journal_progress(copied);
                }
                next_progress += progress_step;
            }
        }
    }

    Ok(format!("Smart copy completed. Bytes moved: {size}"))
}

fn copy_partition_blocks(source_device: &str, target_device: &str, size: u64) -> Result<String, String> {
    let source_info = read_partition_info(source_device)?;
    let target_info = read_partition_info(target_device)?;

    if source_info.disk == target_info.disk {
        return copy_blocks(
            &source_info.disk,
            source_info.partition_offset,
            target_info.partition_offset,
            size,
            false,
        );
    }

    let mut reader = std::fs::OpenOptions::new()
        .read(true)
        .open(source_device)
        .map_err(|e| format!("Open source failed: {e}"))?;
    let mut writer = std::fs::OpenOptions::new()
        .write(true)
        .open(target_device)
        .map_err(|e| format!("Open target failed: {e}"))?;

    let buffer_size = 4 * 1024 * 1024;
    let mut buffer = vec![0u8; buffer_size];
    let mut remaining = size;
    let mut copied: u64 = 0;
    let progress_step: u64 = 50 * 1024 * 1024;
    let mut next_progress = progress_step;

    while remaining > 0 {
        let chunk = std::cmp::min(buffer_size as u64, remaining) as usize;
        reader.read_exact(&mut buffer[..chunk]).map_err(|e| e.to_string())?;
        writer.write_all(&buffer[..chunk]).map_err(|e| e.to_string())?;
        remaining -= chunk as u64;
        copied += chunk as u64;
        if copied >= next_progress {
            let percent = ((copied as f64 / size as f64) * 100.0).round() as u64;
            emit_progress_bytes("copy", percent, 100, Some("Copying blocks"), copied, size);
            next_progress += progress_step;
        }
    }

    Ok(format!("Copy completed. Bytes copied: {size}"))
}

fn emit_progress(phase: &str, percent: u64, total: u64, message: Option<&str>) {
    emit_progress_bytes(phase, percent, total, message, 0, 0);
}

fn emit_progress_bytes(phase: &str, percent: u64, total: u64, message: Option<&str>, bytes: u64, total_bytes: u64) {
    let payload = json!({
        "type": "progress",
        "phase": phase,
        "percent": percent,
        "total": total,
        "message": message,
        "bytes": bytes,
        "totalBytes": total_bytes,
    });
    if let Ok(line) = serde_json::to_string(&payload) {
        println!("{line}");
        let _ = std::io::stdout().flush();
    }
}

fn emit_log(source: &str, line: &str) {
    let payload = json!({
        "type": "log",
        "source": source,
        "line": line,
    });
    if let Ok(line) = serde_json::to_string(&payload) {
        println!("{line}");
        let _ = std::io::stdout().flush();
    }
}

fn find_partition_by_label(label: &str) -> Result<Option<String>, String> {
    let output = Command::new("diskutil")
        .args(["list", "-plist"])
        .output()
        .map_err(|e| format!("diskutil failed: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("diskutil error: {stderr}"));
    }

    let plist = PlistValue::from_reader_xml(&output.stdout[..]).map_err(|e| e.to_string())?;
    let dict = plist
        .as_dictionary()
        .ok_or_else(|| "Invalid plist".to_string())?;

    let all_disks = match dict.get("AllDisksAndPartitions") {
        Some(PlistValue::Array(arr)) => arr,
        _ => return Err("Invalid plist structure".to_string()),
    };

    for entry in all_disks {
        if let Some(disk_dict) = entry.as_dictionary() {
            if let Some(PlistValue::Array(parts)) = disk_dict.get("Partitions") {
                for part in parts {
                    if let Some(part_dict) = part.as_dictionary() {
                        let volume_name = part_dict
                            .get("VolumeName")
                            .and_then(|v| v.as_string())
                            .unwrap_or("");
                        if volume_name == label {
                            let identifier = part_dict
                                .get("DeviceIdentifier")
                                .and_then(|v| v.as_string())
                                .unwrap_or("")
                                .to_string();
                            if !identifier.is_empty() {
                                return Ok(Some(identifier));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(None)
}

fn detect_fs_type(device: &str) -> Result<String, String> {
    let output = Command::new("diskutil")
        .args(["info", "-plist", device])
        .output()
        .map_err(|e| format!("diskutil failed: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("diskutil error: {stderr}"));
    }

    let plist = PlistValue::from_reader_xml(&output.stdout[..]).map_err(|e| e.to_string())?;
    let dict = plist
        .as_dictionary()
        .ok_or_else(|| "Invalid plist".to_string())?;

    let mut candidates = Vec::new();
    if let Some(value) = dict.get("FilesystemType").and_then(|v| v.as_string()) {
        candidates.push(value.to_lowercase());
    }
    if let Some(value) = dict.get("Type").and_then(|v| v.as_string()) {
        candidates.push(value.to_lowercase());
    }
    if let Some(value) = dict.get("Content").and_then(|v| v.as_string()) {
        candidates.push(value.to_lowercase());
    }

    for candidate in candidates {
        if candidate.contains("apfs") {
            return Ok("apfs".to_string());
        }
        if candidate.contains("exfat") {
            return Ok("exfat".to_string());
        }
        if candidate.contains("msdos") || candidate.contains("fat32") || candidate.contains("fat") {
            return Ok("fat32".to_string());
        }
        if candidate.contains("ntfs") {
            return Ok("ntfs".to_string());
        }
        if candidate.contains("ext4") || candidate.contains("linux") {
            return Ok("ext4".to_string());
        }
        if candidate.contains("btrfs") {
            return Ok("btrfs".to_string());
        }
        if candidate.contains("xfs") {
            return Ok("xfs".to_string());
        }
        if candidate.contains("f2fs") {
            return Ok("f2fs".to_string());
        }
        if candidate.contains("swap") {
            return Ok("swap".to_string());
        }
    }

    Ok("unknown".to_string())
}

fn validate_uuid(uuid: &str) -> Result<(), String> {
    if uuid == "random" {
        return Ok(());
    }
    let parts: Vec<&str> = uuid.split('-').collect();
    if parts.len() != 5 {
        return Err("Invalid UUID format".to_string());
    }
    let lengths = [8, 4, 4, 4, 12];
    for (idx, part) in parts.iter().enumerate() {
        if part.len() != lengths[idx] || !part.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err("Invalid UUID format".to_string());
        }
    }
    Ok(())
}

fn strip_device_prefix(identifier: &str) -> String {
    identifier.trim_start_matches("/dev/").to_string()
}

fn plist_string(dict: &std::collections::BTreeMap<String, PlistValue>, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(value) = dict.get(*key).and_then(|v| v.as_string()) {
            return Some(value.to_string());
        }
    }
    None
}

fn plist_u64(dict: &std::collections::BTreeMap<String, PlistValue>, keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Some(value) = dict.get(*key) {
            if let Some(u) = value.as_unsigned_integer() {
                return Some(u);
            }
            if let Some(i) = value.as_integer() {
                if i >= 0 {
                    return Some(i as u64);
                }
            }
        }
    }
    None
}

fn plist_string_array(dict: &std::collections::BTreeMap<String, PlistValue>, keys: &[&str]) -> Vec<String> {
    for key in keys {
        if let Some(arr) = dict.get(*key).and_then(|v| v.as_array()) {
            return arr
                .iter()
                .filter_map(|v| v.as_string())
                .map(|s| s.to_string())
                .collect();
        }
    }
    Vec::new()
}

fn container_matches(container_dict: &std::collections::BTreeMap<String, PlistValue>, needle: &str) -> bool {
    if let Some(reference) = plist_string(container_dict, &["ContainerReference", "DeviceIdentifier", "ContainerIdentifier"]) {
        if strip_device_prefix(&reference) == needle {
            return true;
        }
    }

    let mut store_entries: Vec<&PlistValue> = Vec::new();
    if let Some(arr) = container_dict.get("PhysicalStores").and_then(|v| v.as_array()) {
        store_entries.extend(arr.iter());
    } else if let Some(arr) = container_dict.get("APFSPhysicalStores").and_then(|v| v.as_array()) {
        store_entries.extend(arr.iter());
    }

    for store in store_entries {
        if let Some(store_dict) = store.as_dictionary() {
            if let Some(identifier) = plist_string(store_dict, &["DeviceIdentifier"]) {
                if strip_device_prefix(&identifier) == needle {
                    return true;
                }
            }
        }
    }

    let mut volume_entries: Vec<&PlistValue> = Vec::new();
    if let Some(arr) = container_dict.get("Volumes").and_then(|v| v.as_array()) {
        volume_entries.extend(arr.iter());
    } else if let Some(arr) = container_dict.get("APFSVolumes").and_then(|v| v.as_array()) {
        volume_entries.extend(arr.iter());
    }

    for volume in volume_entries {
        if let Some(volume_dict) = volume.as_dictionary() {
            if let Some(identifier) = plist_string(volume_dict, &["DeviceIdentifier", "DeviceReference"]) {
                if strip_device_prefix(&identifier) == needle {
                    return true;
                }
            }
        }
    }

    false
}

fn run_sidecar<I, S>(binary: &str, args: I) -> Result<(), String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let path = find_sidecar(binary)?;
    let output = Command::new(&path)
        .args(args)
        .output()
        .map_err(|e| format!("Sidecar failed: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Sidecar error: {stderr}"));
    }

    Ok(())
}

fn run_sidecar_stream(binary: &str, args: Vec<String>) -> Result<String, String> {
    let path = find_sidecar(binary)?;
    let output = Command::new(&path)
        .args(args)
        .output()
        .map_err(|e| format!("Sidecar failed: {e}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        emit_log(binary, line);
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    for line in stderr.lines() {
        emit_log(binary, line);
    }

    if !output.status.success() {
        let combined = format!("{stdout}\n{stderr}").trim().to_string();
        return Err(format!("Sidecar error: {combined}"));
    }

    Ok(format!("{stdout}\n{stderr}").trim().to_string())
}

fn driver_for(fs: &str) -> Option<Box<dyn FileSystemDriver>> {
    for driver in default_drivers() {
        if driver.id() == fs {
            return Some(driver);
        }
    }
    None
}

fn find_sidecar(binary: &str) -> Result<PathBuf, String> {
    let mut candidates = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join(binary));
            if let Some(parent) = dir.parent() {
                candidates.push(parent.join("Resources").join("sidecars").join(binary));
            }
        }
    }
    candidates.push(PathBuf::from("/usr/local/bin").join(binary));
    candidates.push(PathBuf::from("/opt/homebrew/bin").join(binary));

    for path in candidates {
        if path.exists() {
            return Ok(path);
        }
    }

    Err(format!("Sidecar not found: {binary}"))
}

fn run_diskutil<I, S>(args: I) -> Result<(), String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let output = Command::new("diskutil")
        .args(args)
        .output()
        .map_err(|e| format!("diskutil failed: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("diskutil error: {stderr}"));
    }

    Ok(())
}

fn run_diskutil_capture<I, S>(args: I) -> Result<String, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let output = Command::new("diskutil")
        .args(args)
        .output()
        .map_err(|e| format!("diskutil failed: {e}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        let combined = format!("{stdout}\n{stderr}").trim().to_string();
        return Err(format!("diskutil error: {combined}"));
    }

    Ok(format!("{stdout}\n{stderr}").trim().to_string())
}

fn run_sidecar_capture<I, S>(binary: &str, args: I) -> Result<String, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let path = find_sidecar(binary)?;
    let output = Command::new(&path)
        .args(args)
        .output()
        .map_err(|e| format!("Sidecar failed: {e}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        let combined = format!("{stdout}\n{stderr}").trim().to_string();
        return Err(format!("Sidecar error: {combined}"));
    }

    Ok(format!("{stdout}\n{stderr}").trim().to_string())
}

fn write_response(ok: bool, message: Option<String>, details: Option<Value>) {
    let response = HelperResponse { ok, message, details };
    if let Ok(json) = serde_json::to_string(&response) {
        let _ = std::io::stdout().write_all(json.as_bytes());
    }
}
