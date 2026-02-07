use plist::Value as PlistValue;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
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

    match format_type.to_lowercase().as_str() {
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
    }
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

    run_diskutil([
        "partitionDisk",
        &device,
        "1",
        scheme,
        "free",
        "%noformat%",
        "100%",
    ])?;

    Ok(Some(json!({ "device": device, "scheme": scheme })))
}

fn handle_create_partition(payload: &Value) -> Result<Option<Value>, String> {
    let device_identifier = read_string(payload, "deviceIdentifier")?;
    let format_type = read_string(payload, "formatType")?;
    let label = read_string(payload, "label")?;
    let size = read_string(payload, "size")?;

    let device = normalize_device(&device_identifier);

    match format_type.to_lowercase().as_str() {
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
    }
}

fn handle_delete_partition(payload: &Value) -> Result<Option<Value>, String> {
    let partition_identifier = read_string(payload, "partitionIdentifier")?;
    let device = normalize_device(&partition_identifier);

    run_diskutil(["eraseVolume", "free", "none", &device])?;

    Ok(Some(json!({ "partition": device })))
}

fn handle_format_partition(payload: &Value) -> Result<Option<Value>, String> {
    let partition_identifier = read_string(payload, "partitionIdentifier")?;
    let format_type = read_string(payload, "formatType")?;
    let label = read_string(payload, "label")?;

    let device = normalize_device(&partition_identifier);

    run_diskutil(["unmount", "force", &device])?;

    match format_type.to_lowercase().as_str() {
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
    }
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

    Ok(Some(json!({ "device": device, "label": label, "uuid": uuid, "fs": fs_type })))
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

    let fs_type = detect_fs_type(&device)?;
    emit_progress("resize", 0, 100, Some("Start resize"));
    match fs_type.as_str() {
        "apfs" | "hfs+" => {
            run_diskutil(["resizeVolume", &device, &new_size])?;
            emit_progress("resize", 100, 100, Some("Resize complete"));
            Ok(Some(json!({ "device": device, "fs": fs_type, "size": new_size })))
        }
        "exfat" | "fat32" => Err("Resize for FAT/exFAT not supported yet".to_string()),
        "ext4" => resize_linux_partition(&device, "ext4", &new_size),
        "ntfs" => resize_linux_partition(&device, "ntfs", &new_size),
        _ => Err("Unsupported filesystem for resize".to_string()),
    }
}

fn handle_move_partition(payload: &Value) -> Result<Option<Value>, String> {
    let partition_identifier = read_string(payload, "partitionIdentifier")?;
    let new_start = read_string(payload, "newStart")?;
    let device = normalize_device(&partition_identifier);

    let target_start = parse_size_bytes(&new_start)?;
    emit_progress("move", 0, 100, Some("Start move"));
    let result = move_partition(&device, target_start)?;
    emit_progress("move", 100, 100, Some("Move complete"));
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

fn normalize_device(identifier: &str) -> String {
    if identifier.starts_with("/dev/") {
        identifier.to_string()
    } else {
        format!("/dev/{identifier}")
    }
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

    run_diskutil(["unmount", "force", device])?;
    let move_log = copy_blocks(&info.disk, old_start, aligned_start, size)?;

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

    Ok(Some(json!({ "device": device, "newStart": aligned_start, "output": format!("{move_log}\n{gpt_log}").trim() })))
}

fn copy_blocks(disk: &str, src_offset: u64, dst_offset: u64, size: u64) -> Result<String, String> {
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
