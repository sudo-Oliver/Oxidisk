use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};
use tauri::path::BaseDirectory;
use tauri::{Emitter, Manager};

#[derive(Serialize)]
pub struct PartitionDevice {
    identifier: String,
    size: u64,
    internal: bool,
    is_solid_state: bool,
    bus_protocol: Option<String>,
    content: String,
    parent_device: Option<String>,
    partitions: Vec<PartitionEntry>,
    is_protected: bool,
    protection_reason: Option<String>,
}

#[derive(Serialize)]
pub struct PartitionEntry {
    identifier: String,
    name: String,
    size: u64,
    offset: Option<u64>,
    content: String,
    mount_point: Option<String>,
    is_protected: bool,
    protection_reason: Option<String>,
    fs_type: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct HelperRequest {
    action: String,
    payload: Value,
}

#[derive(Serialize, Deserialize)]
pub struct HelperResponse {
    ok: bool,
    message: Option<String>,
    details: Option<Value>,
}

#[derive(Deserialize)]
pub struct WipeDeviceRequest {
    device_identifier: String,
    table_type: String,
    format_type: String,
    label: String,
}

#[derive(Deserialize)]
pub struct SecureEraseRequest {
    device_identifier: String,
    level: u64,
}

#[derive(Deserialize)]
pub struct PartitionTableRequest {
    device_identifier: String,
    table_type: String,
}

#[derive(Deserialize)]
pub struct CreatePartitionRequest {
    device_identifier: String,
    format_type: String,
    label: String,
    size: String,
}

#[derive(Deserialize)]
pub struct DeletePartitionRequest {
    partition_identifier: String,
}

#[derive(Deserialize)]
pub struct FormatPartitionRequest {
    partition_identifier: String,
    format_type: String,
    label: String,
}

#[derive(Deserialize)]
pub struct SetLabelUuidRequest {
    partition_identifier: String,
    label: Option<String>,
    uuid: Option<String>,
}

#[derive(Deserialize)]
pub struct CheckPartitionRequest {
    partition_identifier: String,
    repair: Option<bool>,
}

#[derive(Deserialize)]
pub struct ResizePartitionRequest {
    partition_identifier: String,
    new_size: String,
}

#[derive(Deserialize)]
pub struct MovePartitionRequest {
    partition_identifier: String,
    new_start: String,
}

#[derive(Deserialize)]
pub struct CopyPartitionRequest {
    source_partition: String,
    target_device: String,
}

#[derive(Deserialize)]
pub struct FlashImageRequest {
    source_path: String,
    target_device: String,
    verify: Option<bool>,
}

#[derive(Deserialize)]
pub struct InspectImageRequest {
    source_path: String,
}

#[derive(Deserialize)]
pub struct HashImageRequest {
    source_path: String,
}

#[derive(Deserialize)]
pub struct BackupImageRequest {
    source_device: String,
    target_path: String,
    compress: Option<bool>,
}

#[derive(Deserialize)]
pub struct WindowsInstallRequest {
    source_path: String,
    target_device: String,
    label: Option<String>,
    tpm_bypass: Option<bool>,
    local_account: Option<bool>,
    privacy_defaults: Option<bool>,
}

#[derive(Deserialize)]
pub struct PreflightRequest {
    device_identifier: Option<String>,
    partition_identifier: Option<String>,
    operation: String,
    format_type: Option<String>,
    new_size: Option<String>,
}

#[derive(Deserialize)]
pub struct ForceUnmountRequest {
    device_identifier: Option<String>,
    partition_identifier: Option<String>,
}

#[derive(Deserialize)]
pub struct ApfsAddVolumeRequest {
    container_identifier: String,
    name: String,
    role: Option<String>,
}

#[derive(Deserialize)]
pub struct ApfsDeleteVolumeRequest {
    volume_identifier: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApfsVolumeInfo {
    identifier: String,
    name: String,
    roles: Vec<String>,
    volume_group_uuid: Option<String>,
    volume_group_role: Option<String>,
    volume_group_name: Option<String>,
    sealed: Option<bool>,
    size: u64,
    used: u64,
    mount_point: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApfsContainerInfo {
    container_identifier: String,
    container_uuid: Option<String>,
    capacity: Option<u64>,
    capacity_free: Option<u64>,
    capacity_used: Option<u64>,
    volumes: Vec<ApfsVolumeInfo>,
}

#[derive(Serialize)]
pub struct SidecarStatus {
    name: String,
    found: bool,
    path: Option<String>,
    version: Option<String>,
}

#[derive(Serialize)]
pub struct PartitionBounds {
    offset: u64,
    size: u64,
    min_start: u64,
    max_start: u64,
    block_size: u64,
}

#[derive(Serialize)]
struct SudoersInstallResult {
    helper_path: String,
    sudoers_path: String,
}

static ACTIVE_HELPER_PID: OnceLock<Mutex<Option<u32>>> = OnceLock::new();

fn set_active_helper_pid(pid: Option<u32>) {
    let lock = ACTIVE_HELPER_PID.get_or_init(|| Mutex::new(None));
    if let Ok(mut guard) = lock.lock() {
        *guard = pid;
    }
}

fn get_active_helper_pid() -> Option<u32> {
    let lock = ACTIVE_HELPER_PID.get_or_init(|| Mutex::new(None));
    lock.lock().ok().and_then(|guard| *guard)
}

#[tauri::command]
pub fn get_partition_devices() -> Vec<PartitionDevice> {
    #[cfg(target_os = "macos")]
    {
        use plist::Value;

        let output = Command::new("diskutil").args(["list", "-plist"]).output();
        let output = match output {
            Ok(o) if o.status.success() => o,
            _ => return Vec::new(),
        };

        let plist = match Value::from_reader_xml(&output.stdout[..]) {
            Ok(p) => p,
            Err(_) => return Vec::new(),
        };

        let dict = match plist.as_dictionary() {
            Some(d) => d,
            None => return Vec::new(),
        };

        let all_disks = match dict.get("AllDisksAndPartitions") {
            Some(Value::Array(arr)) => arr,
            _ => return Vec::new(),
        };

        let mut devices = Vec::new();

        for entry in all_disks {
            let disk_dict = match entry.as_dictionary() {
                Some(d) => d,
                None => continue,
            };

            let identifier = disk_dict
                .get("DeviceIdentifier")
                .and_then(|v| v.as_string())
                .unwrap_or("unknown")
                .to_string();

            let size = disk_dict.get("Size").and_then(|v| v.as_unsigned_integer()).unwrap_or(0);
            let internal = !disk_external_flag(&identifier, disk_dict);
            let is_solid_state = disk_dict
                .get("SolidState")
                .and_then(|v| v.as_boolean())
                .unwrap_or(false);
            let bus_protocol = disk_dict
                .get("BusProtocol")
                .and_then(|v| v.as_string())
                .map(|s| s.to_string());
            let content = disk_dict
                .get("Content")
                .and_then(|v| v.as_string())
                .unwrap_or("unknown")
                .to_string();

            let mut partitions = Vec::new();
            let partition_offsets = partition_offsets_for_disk(&identifier);
            let mut device_protected = false;
            let mut device_protection_reason: Option<String> = None;
            let parent_device = disk_dict
                .get("APFSPhysicalStores")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.get(0))
                .and_then(|v| v.as_dictionary())
                .and_then(|d| d.get("DeviceIdentifier"))
                .and_then(|v| v.as_string())
                .map(|s| s.to_string());
            if let Some(Value::Array(parts)) = disk_dict.get("Partitions") {
                for part in parts {
                    let part_dict = match part.as_dictionary() {
                        Some(d) => d,
                        None => continue,
                    };

                    let part_id = part_dict
                        .get("DeviceIdentifier")
                        .and_then(|v| v.as_string())
                        .unwrap_or("unknown")
                        .to_string();

                    let part_name = part_dict
                        .get("VolumeName")
                        .and_then(|v| v.as_string())
                        .unwrap_or("")
                        .to_string();

                    let part_size = part_dict
                        .get("Size")
                        .and_then(|v| v.as_unsigned_integer())
                        .unwrap_or(0);

                    let part_content = part_dict
                        .get("Content")
                        .and_then(|v| v.as_string())
                        .unwrap_or("unknown")
                        .to_string();

                    let part_offset = partition_offsets.get(&part_id).map(|entry| entry.0);

                    let mount_point = part_dict
                        .get("MountPoint")
                        .and_then(|v| v.as_string())
                        .map(|s| s.to_string());

                    let protection = partition_protection(&part_id, internal);
                    let fs_type = partition_fs_type(&part_id);
                    if protection.0 {
                        device_protected = true;
                        if device_protection_reason.is_none() {
                            device_protection_reason = protection.1.clone();
                        }
                    }

                    partitions.push(PartitionEntry {
                        identifier: part_id,
                        name: part_name,
                        size: part_size,
                        offset: part_offset,
                        content: part_content,
                        mount_point,
                        is_protected: protection.0,
                        protection_reason: protection.1,
                        fs_type,
                    });
                }
            }

            devices.push(PartitionDevice {
                identifier,
                size,
                internal,
                is_solid_state,
                bus_protocol,
                content,
                parent_device,
                partitions,
                is_protected: device_protected,
                protection_reason: device_protection_reason,
            });
        }

        devices
    }

    #[cfg(not(target_os = "macos"))]
    {
        Vec::new()
    }
}

#[cfg(target_os = "macos")]
fn partition_fs_type(identifier: &str) -> Option<String> {
    let device = if identifier.starts_with("/dev/") {
        identifier.to_string()
    } else {
        format!("/dev/{identifier}")
    };

    let output = Command::new("diskutil")
        .args(["info", "-plist", &device])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let plist = plist::Value::from_reader_xml(&output.stdout[..]).ok()?;
    let dict = plist.as_dictionary()?;

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
            return Some("apfs".to_string());
        }
        if candidate.contains("exfat") {
            return Some("exfat".to_string());
        }
        if candidate.contains("msdos") || candidate.contains("fat32") || candidate.contains("fat") {
            return Some("fat32".to_string());
        }
        if candidate.contains("ntfs") {
            return Some("ntfs".to_string());
        }
        if candidate.contains("ext4") || candidate.contains("linux") {
            return Some("ext4".to_string());
        }
    }

    None
}

#[cfg(target_os = "macos")]
fn partition_offsets_for_disk(disk_identifier: &str) -> HashMap<String, (u64, u64)> {
    use plist::Value;

    let device = if disk_identifier.starts_with("/dev/") {
        disk_identifier.to_string()
    } else {
        format!("/dev/{disk_identifier}")
    };

    let output = Command::new("diskutil")
        .args(["list", "-plist", &device])
        .output();
    let output = match output {
        Ok(o) if o.status.success() => o,
        _ => return HashMap::new(),
    };

    let plist = match Value::from_reader_xml(&output.stdout[..]) {
        Ok(p) => p,
        Err(_) => return HashMap::new(),
    };

    let dict = match plist.as_dictionary() {
        Some(d) => d,
        None => return HashMap::new(),
    };

    let partitions = match dict.get("Partitions") {
        Some(Value::Array(parts)) => parts,
        _ => return HashMap::new(),
    };

    let mut offsets = HashMap::new();
    for part in partitions {
        if let Some(part_dict) = part.as_dictionary() {
            let identifier = part_dict
                .get("DeviceIdentifier")
                .and_then(|v| v.as_string())
                .unwrap_or("")
                .to_string();
            let offset = part_dict
                .get("PartitionOffset")
                .and_then(|v| v.as_unsigned_integer())
                .unwrap_or(0);
            let size = part_dict
                .get("PartitionSize")
                .and_then(|v| v.as_unsigned_integer())
                .unwrap_or(0);

            if !identifier.is_empty() && size > 0 {
                offsets.insert(identifier, (offset, size));
            }
        }
    }

    offsets
}

#[cfg(not(target_os = "macos"))]
fn partition_offsets_for_disk(_disk_identifier: &str) -> HashMap<String, (u64, u64)> {
    HashMap::new()
}

#[cfg(target_os = "macos")]
fn disk_external_flag(identifier: &str, disk_dict: &plist::Dictionary) -> bool {
    if let Some(external) = disk_external_flag_from_info(identifier) {
        return external;
    }

    disk_external_flag_from_dict(disk_dict)
}

#[cfg(target_os = "macos")]
fn disk_external_flag_from_info(identifier: &str) -> Option<bool> {
    let device = if identifier.starts_with("/dev/") {
        identifier.to_string()
    } else {
        format!("/dev/{identifier}")
    };

    let output = Command::new("diskutil")
        .args(["info", "-plist", &device])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let plist = plist::Value::from_reader_xml(&output.stdout[..]).ok()?;
    let dict = plist.as_dictionary()?;
    Some(disk_external_flag_from_dict(dict))
}

#[cfg(target_os = "macos")]
fn disk_external_flag_from_dict(dict: &plist::Dictionary) -> bool {
    let bus_protocol = dict
        .get("BusProtocol")
        .and_then(|v| v.as_string())
        .unwrap_or("")
        .to_lowercase();
    let ejectable = dict
        .get("Ejectable")
        .and_then(|v| v.as_boolean())
        .unwrap_or(false);
    let removable = dict
        .get("RemovableMedia")
        .and_then(|v| v.as_boolean())
        .unwrap_or(false);
    let removable_or_external = dict
        .get("RemovableMediaOrExternalDevice")
        .and_then(|v| v.as_boolean())
        .unwrap_or(false);
    let internal = dict
        .get("Internal")
        .and_then(|v| v.as_boolean())
        .unwrap_or(true);
    let virtual_or_physical = dict
        .get("VirtualOrPhysical")
        .and_then(|v| v.as_string())
        .unwrap_or("")
        .to_lowercase();
    let is_virtual = virtual_or_physical == "virtual";
    let external_bus = ["usb", "thunderbolt", "firewire", "sd", "sdc"]
        .iter()
        .any(|hint| bus_protocol.contains(hint));

    !is_virtual && (external_bus || ejectable || removable || removable_or_external || !internal)
}

#[cfg(not(target_os = "macos"))]
fn partition_fs_type(_identifier: &str) -> Option<String> {
    None
}

#[cfg(target_os = "macos")]
fn partition_protection(identifier: &str, internal: bool) -> (bool, Option<String>) {
    if !internal {
        return (false, None);
    }

    let device = if identifier.starts_with("/dev/") {
        identifier.to_string()
    } else {
        format!("/dev/{identifier}")
    };

    let output = Command::new("diskutil")
        .args(["info", "-plist", &device])
        .output();

    let output = match output {
        Ok(o) if o.status.success() => o,
        _ => return (false, None),
    };

    let plist = match plist::Value::from_reader_xml(&output.stdout[..]) {
        Ok(p) => p,
        Err(_) => return (false, None),
    };

    let dict = match plist.as_dictionary() {
        Some(d) => d,
        None => return (false, None),
    };

    let roles = dict
        .get("APFSVolumeRoles")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_string())
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
        })
        .unwrap_or_default();

    let role_set: std::collections::HashSet<String> = roles.iter().cloned().collect();
    let protected_roles = ["System", "Data", "Preboot", "Recovery", "VM"];
    let is_protected = protected_roles.iter().any(|role| role_set.contains(*role));
    if is_protected {
        return (
            true,
            Some("System-Volume (SIP geschuetzt)".to_string()),
        );
    }

    (false, None)
}

#[cfg(not(target_os = "macos"))]
fn partition_protection(_identifier: &str, _internal: bool) -> (bool, Option<String>) {
    (false, None)
}

#[tauri::command]
pub fn mount_disk(device_identifier: String) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        let device = if device_identifier.starts_with("/dev/") {
            device_identifier
        } else {
            format!("/dev/{device_identifier}")
        };

        let output = Command::new("diskutil")
            .args(["mountDisk", &device])
            .output()
            .map_err(|e| format!("diskutil failed: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("diskutil error: {stderr}"));
        }

        return Ok(());
    }

    #[cfg(not(target_os = "macos"))]
    {
        Err("Mount not supported on this platform".to_string())
    }
}

#[tauri::command]
pub fn mount_volume(device_identifier: String) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        let device = if device_identifier.starts_with("/dev/") {
            device_identifier
        } else {
            format!("/dev/{device_identifier}")
        };

        let output = Command::new("diskutil")
            .args(["mount", &device])
            .output()
            .map_err(|e| format!("diskutil failed: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("diskutil error: {stderr}"));
        }

        return Ok(());
    }

    #[cfg(not(target_os = "macos"))]
    {
        Err("Mount not supported on this platform".to_string())
    }
}

fn helper_paths(app: &tauri::AppHandle) -> Vec<std::path::PathBuf> {
    let mut paths = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            paths.push(dir.join("oxidisk_helper"));
        }
    }
    if let Ok(path) = app
        .path()
        .resolve("helper/oxidisk_helper", BaseDirectory::Resource)
    {
        paths.push(path);
    }
    paths.push(std::path::PathBuf::from(
        "/Library/PrivilegedHelperTools/com.oliverquick.oxidisk.helper",
    ));
    paths.push(std::path::PathBuf::from("/usr/local/bin/oxidisk_helper"));
    paths.push(std::path::PathBuf::from("/opt/homebrew/bin/oxidisk_helper"));
    paths
}

fn run_helper(app: &tauri::AppHandle, request: HelperRequest) -> Result<HelperResponse, String> {
    let request_json = serde_json::to_vec(&request).map_err(|e| e.to_string())?;

    for path in helper_paths(app) {
        if !path.exists() {
            continue;
        }

        let mut child = Command::new("sudo")
            .arg("-n")
            .arg(&path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("Helper start failed: {e}"))?;

        set_active_helper_pid(Some(child.id()));

        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(&request_json)
                .map_err(|e| format!("Helper stdin failed: {e}"))?;
        }

        let output = child
            .wait_with_output()
            .map_err(|e| format!("Helper run failed: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("a password is required") || stderr.contains("a password is required") {
                return Err("Helper requires sudoers setup. Please run setup first.".to_string());
            }
            return Err(format!("Helper error: {stderr}"));
        }

        let response: HelperResponse = serde_json::from_slice(&output.stdout)
            .map_err(|e| format!("Helper response parse failed: {e}"))?;
        return Ok(response);
    }

    Err("Privileged helper not found. Please install the helper tool.".to_string())
}

fn run_helper_stream(
    app: &tauri::AppHandle,
    window: &tauri::Window,
    request: HelperRequest,
) -> Result<HelperResponse, String> {
    let request_json = serde_json::to_vec(&request).map_err(|e| e.to_string())?;

    for path in helper_paths(app) {
        if !path.exists() {
            continue;
        }

        let mut child = Command::new("sudo")
            .arg("-n")
            .arg(&path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("Helper start failed: {e}"))?;

        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(&request_json)
                .map_err(|e| format!("Helper stdin failed: {e}"))?;
        }

        let stdout = child.stdout.take().ok_or_else(|| "Failed to read helper stdout".to_string())?;
        let stderr = child.stderr.take().ok_or_else(|| "Failed to read helper stderr".to_string())?;
        let mut stdout_reader = BufReader::new(stdout);
        let mut stderr_reader = BufReader::new(stderr);

        let mut buffer = String::new();
        let mut last_json: Option<String> = None;
        loop {
            buffer.clear();
            let bytes = stdout_reader
                .read_line(&mut buffer)
                .map_err(|e| format!("Helper stdout failed: {e}"))?;
            if bytes == 0 {
                break;
            }
            let line = buffer.trim().to_string();
            if line.is_empty() {
                continue;
            }
            if let Ok(value) = serde_json::from_str::<Value>(&line) {
                if value.get("type").and_then(|v| v.as_str()) == Some("progress") {
                    let _ = window.emit("partition-operation-progress", value);
                    continue;
                }
                if value.get("type").and_then(|v| v.as_str()) == Some("log") {
                    let _ = window.emit("partition-operation-log", value);
                    continue;
                }
            }
            last_json = Some(line);
        }

        let status = child.wait().map_err(|e| format!("Helper run failed: {e}"))?;
        let mut stderr_text = String::new();
        let _ = stderr_reader.read_to_string(&mut stderr_text);

        set_active_helper_pid(None);

        if !status.success() {
            if stderr_text.contains("a password is required") {
                return Err("Helper requires sudoers setup. Please run setup first.".to_string());
            }
            return Err(format!("Helper error: {stderr_text}"));
        }

        let last_json = last_json.ok_or_else(|| "No helper response".to_string())?;
        let response: HelperResponse = serde_json::from_str(&last_json)
            .map_err(|e| format!("Helper response parse failed: {e}"))?;
        return Ok(response);
    }

    Err("Privileged helper not found. Please install the helper tool.".to_string())
}

fn read_id_username() -> Result<String, String> {
    let output = Command::new("id").arg("-un").output().map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err("Failed to read username".to_string());
    }
    let username = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if username.is_empty() {
        return Err("Failed to read username".to_string());
    }
    Ok(username)
}

fn validate_token(value: &str, field: &str, allow_slash: bool) -> Result<(), String> {
    let ok = value.chars().all(|ch| {
        ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.' || (allow_slash && ch == '/')
    });
    if ok {
        Ok(())
    } else {
        Err(format!("Invalid characters in {field}"))
    }
}

#[tauri::command]
pub fn install_sudoers_helper(app: tauri::AppHandle) -> Result<HelperResponse, String> {
    #[cfg(target_os = "macos")]
    {
        let username = read_id_username()?;
        validate_token(&username, "username", false)?;

        let helper_path = helper_paths(&app)
            .into_iter()
            .find(|path| path.exists())
            .ok_or_else(|| "Helper not found on this system".to_string())?;

        let helper_path_str = helper_path
            .to_str()
            .ok_or_else(|| "Invalid helper path".to_string())?
            .to_string();

        validate_token(&helper_path_str, "helper path", true)?;

        let sudoers_path = "/etc/sudoers.d/oxidisk";
        let sudoers_line = format!("{username} ALL=(root) NOPASSWD: {helper_path_str}");

        let command = format!(
            "/bin/sh -c \"/usr/bin/printf '%s\\n' '{sudoers_line}' > {sudoers_path} && /bin/chmod 440 {sudoers_path}\""
        );

        let output = Command::new("osascript")
            .arg("-e")
            .arg(format!("do shell script \"{command}\" with administrator privileges"))
            .output()
            .map_err(|e| format!("Failed to run osascript: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("Failed to install sudoers: {stderr}"));
        }

        return Ok(HelperResponse {
            ok: true,
            message: Some("Sudoers installed".to_string()),
            details: Some(
                json!(SudoersInstallResult { helper_path: helper_path_str, sudoers_path: sudoers_path.to_string() })
            ),
        });
    }

    #[cfg(not(target_os = "macos"))]
    {
        Err("Sudoers setup is only supported on macOS.".to_string())
    }
}

fn ok_or_message(response: HelperResponse) -> Result<HelperResponse, String> {
    if response.ok {
        Ok(response)
    } else {
        Err(response
            .message
            .unwrap_or("Helper reported failure.".to_string()))
    }
}

#[tauri::command]
pub fn wipe_device(app: tauri::AppHandle, request: WipeDeviceRequest) -> Result<HelperResponse, String> {
    let payload = json!({
        "deviceIdentifier": request.device_identifier,
        "tableType": request.table_type,
        "formatType": request.format_type,
        "label": request.label,
    });

    let response = run_helper(
        &app,
        HelperRequest {
            action: "wipe_device".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn secure_erase(app: tauri::AppHandle, request: SecureEraseRequest) -> Result<HelperResponse, String> {
    let payload = json!({
        "deviceIdentifier": request.device_identifier,
        "level": request.level,
    });

    let response = run_helper(
        &app,
        HelperRequest {
            action: "secure_erase".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn create_partition_table(
    app: tauri::AppHandle,
    request: PartitionTableRequest,
) -> Result<HelperResponse, String> {
    let payload = json!({
        "deviceIdentifier": request.device_identifier,
        "tableType": request.table_type,
    });

    let response = run_helper(
        &app,
        HelperRequest {
            action: "create_partition_table".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn create_partition(
    app: tauri::AppHandle,
    request: CreatePartitionRequest,
) -> Result<HelperResponse, String> {
    let payload = json!({
        "deviceIdentifier": request.device_identifier,
        "formatType": request.format_type,
        "label": request.label,
        "size": request.size,
    });

    let response = run_helper(
        &app,
        HelperRequest {
            action: "create_partition".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn delete_partition(
    app: tauri::AppHandle,
    request: DeletePartitionRequest,
) -> Result<HelperResponse, String> {
    let payload = json!({
        "partitionIdentifier": request.partition_identifier,
    });

    let response = run_helper(
        &app,
        HelperRequest {
            action: "delete_partition".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn format_partition(
    app: tauri::AppHandle,
    request: FormatPartitionRequest,
) -> Result<HelperResponse, String> {
    let payload = json!({
        "partitionIdentifier": request.partition_identifier,
        "formatType": request.format_type,
        "label": request.label,
    });

    let response = run_helper(
        &app,
        HelperRequest {
            action: "format_partition".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn set_label_uuid(
    app: tauri::AppHandle,
    request: SetLabelUuidRequest,
) -> Result<HelperResponse, String> {
    let payload = json!({
        "partitionIdentifier": request.partition_identifier,
        "label": request.label,
        "uuid": request.uuid,
    });

    let response = run_helper(
        &app,
        HelperRequest {
            action: "set_label_uuid".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn check_partition(
    app: tauri::AppHandle,
    request: CheckPartitionRequest,
) -> Result<HelperResponse, String> {
    let payload = json!({
        "partitionIdentifier": request.partition_identifier,
        "repair": request.repair.unwrap_or(false),
    });

    let response = run_helper(
        &app,
        HelperRequest {
            action: "check_partition".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn resize_partition(
    app: tauri::AppHandle,
    window: tauri::Window,
    request: ResizePartitionRequest,
) -> Result<HelperResponse, String> {
    let payload = json!({
        "partitionIdentifier": request.partition_identifier,
        "newSize": request.new_size,
    });

    let response = run_helper_stream(
        &app,
        &window,
        HelperRequest {
            action: "resize_partition".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn move_partition(
    app: tauri::AppHandle,
    window: tauri::Window,
    request: MovePartitionRequest,
) -> Result<HelperResponse, String> {
    let payload = json!({
        "partitionIdentifier": request.partition_identifier,
        "newStart": request.new_start,
    });

    let response = run_helper_stream(
        &app,
        &window,
        HelperRequest {
            action: "move_partition".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn copy_partition(
    app: tauri::AppHandle,
    window: tauri::Window,
    request: CopyPartitionRequest,
) -> Result<HelperResponse, String> {
    let payload = json!({
        "sourcePartition": request.source_partition,
        "targetDevice": request.target_device,
    });

    let response = run_helper_stream(
        &app,
        &window,
        HelperRequest {
            action: "copy_partition".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn flash_image(
    app: tauri::AppHandle,
    window: tauri::Window,
    request: FlashImageRequest,
) -> Result<HelperResponse, String> {
    let payload = json!({
        "sourcePath": request.source_path,
        "targetDevice": request.target_device,
        "verify": request.verify.unwrap_or(true),
    });

    let response = run_helper_stream(
        &app,
        &window,
        HelperRequest {
            action: "flash_image".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn inspect_image(app: tauri::AppHandle, request: InspectImageRequest) -> Result<HelperResponse, String> {
    let payload = json!({
        "sourcePath": request.source_path,
    });

    let response = run_helper(
        &app,
        HelperRequest {
            action: "inspect_image".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn hash_image(
    app: tauri::AppHandle,
    window: tauri::Window,
    request: HashImageRequest,
) -> Result<HelperResponse, String> {
    let payload = json!({
        "sourcePath": request.source_path,
    });

    let response = run_helper_stream(
        &app,
        &window,
        HelperRequest {
            action: "hash_image".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn backup_image(
    app: tauri::AppHandle,
    window: tauri::Window,
    request: BackupImageRequest,
) -> Result<HelperResponse, String> {
    let payload = json!({
        "sourceDevice": request.source_device,
        "targetPath": request.target_path,
        "compress": request.compress.unwrap_or(false),
    });

    let response = run_helper_stream(
        &app,
        &window,
        HelperRequest {
            action: "backup_image".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn windows_install(
    app: tauri::AppHandle,
    window: tauri::Window,
    request: WindowsInstallRequest,
) -> Result<HelperResponse, String> {
    let payload = json!({
        "sourcePath": request.source_path,
        "targetDevice": request.target_device,
        "label": request.label,
        "tpmBypass": request.tpm_bypass.unwrap_or(false),
        "localAccount": request.local_account.unwrap_or(false),
        "privacyDefaults": request.privacy_defaults.unwrap_or(false),
    });

    let response = run_helper_stream(
        &app,
        &window,
        HelperRequest {
            action: "windows_install".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn cancel_helper_operation() -> Result<(), String> {
    if let Some(pid) = get_active_helper_pid() {
        let output = Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .output()
            .map_err(|e| format!("Cancel failed: {e}"))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("Cancel error: {stderr}"));
        }
        set_active_helper_pid(None);
        return Ok(());
    }

    Err("No active operation to cancel".to_string())
}

#[tauri::command]
pub fn preflight_partition(
    app: tauri::AppHandle,
    request: PreflightRequest,
) -> Result<HelperResponse, String> {
    let payload = json!({
        "deviceIdentifier": request.device_identifier,
        "partitionIdentifier": request.partition_identifier,
        "operation": request.operation,
        "formatType": request.format_type,
        "newSize": request.new_size,
    });

    let response = run_helper(
        &app,
        HelperRequest {
            action: "preflight_check".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn force_unmount_partition(
    app: tauri::AppHandle,
    request: ForceUnmountRequest,
) -> Result<HelperResponse, String> {
    let payload = json!({
        "deviceIdentifier": request.device_identifier,
        "partitionIdentifier": request.partition_identifier,
    });

    let response = run_helper(
        &app,
        HelperRequest {
            action: "force_unmount".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn get_operation_journal(app: tauri::AppHandle) -> Result<HelperResponse, String> {
    let response = run_helper(
        &app,
        HelperRequest {
            action: "get_journal".to_string(),
            payload: json!({}),
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn clear_operation_journal(app: tauri::AppHandle) -> Result<HelperResponse, String> {
    let response = run_helper(
        &app,
        HelperRequest {
            action: "clear_journal".to_string(),
            payload: json!({}),
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn apfs_list_volumes(app: tauri::AppHandle, container_identifier: String) -> Result<ApfsContainerInfo, String> {
    let payload = json!({
        "containerIdentifier": container_identifier,
    });

    let response = run_helper(
        &app,
        HelperRequest {
            action: "apfs_list_volumes".to_string(),
            payload,
        },
    )?;

    let response = ok_or_message(response)?;
    let details = response
        .details
        .ok_or_else(|| "APFS details missing".to_string())?;
    let info: ApfsContainerInfo = serde_json::from_value(details)
        .map_err(|e| format!("Invalid APFS details: {e}"))?;
    Ok(info)
}

#[tauri::command]
pub fn apfs_add_volume(app: tauri::AppHandle, request: ApfsAddVolumeRequest) -> Result<HelperResponse, String> {
    let payload = json!({
        "containerIdentifier": request.container_identifier,
        "name": request.name,
        "role": request.role,
    });

    let response = run_helper(
        &app,
        HelperRequest {
            action: "apfs_add_volume".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn apfs_delete_volume(
    app: tauri::AppHandle,
    request: ApfsDeleteVolumeRequest,
) -> Result<HelperResponse, String> {
    let payload = json!({
        "volumeIdentifier": request.volume_identifier,
    });

    let response = run_helper(
        &app,
        HelperRequest {
            action: "apfs_delete_volume".to_string(),
            payload,
        },
    )?;

    ok_or_message(response)
}

#[tauri::command]
pub fn get_sidecar_status(app: tauri::AppHandle) -> Vec<SidecarStatus> {
    let binaries = [
        "sgdisk",
        "resize2fs",
        "ntfsresize",
        "mkfs.ext4",
        "mkfs.ntfs",
        "mkfs.btrfs",
        "mkfs.xfs",
        "mkfs.f2fs",
        "mkswap",
        "e2fsck",
        "ntfsfix",
        "e2label",
        "tune2fs",
        "ntfslabel",
        "wipefs",
    ];

    binaries
        .iter()
        .map(|binary| sidecar_status_for(&app, binary))
        .collect()
}

#[tauri::command]
pub fn get_partition_bounds(device_identifier: String) -> Result<PartitionBounds, String> {
    #[cfg(target_os = "macos")]
    {
        use plist::Value;

        let device = if device_identifier.starts_with("/dev/") {
            device_identifier
        } else {
            format!("/dev/{device_identifier}")
        };

        let output = Command::new("diskutil")
            .args(["info", "-plist", &device])
            .output()
            .map_err(|e| format!("diskutil failed: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("diskutil error: {stderr}"));
        }

        let plist = Value::from_reader_xml(&output.stdout[..]).map_err(|e| e.to_string())?;
        let dict = plist.as_dictionary().ok_or_else(|| "Invalid plist".to_string())?;

        let offset = dict
            .get("PartitionOffset")
            .and_then(|v| v.as_unsigned_integer())
            .ok_or_else(|| "PartitionOffset missing".to_string())?;
        let size = dict
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
            .ok_or_else(|| "ParentWholeDisk missing".to_string())?;
        let disk_path = format!("/dev/{disk}");

        let (min_start, max_start) = partition_bounds_for_disk(&disk_path, &device, size)?;

        return Ok(PartitionBounds {
            offset,
            size,
            min_start,
            max_start,
            block_size,
        });
    }

    #[cfg(not(target_os = "macos"))]
    {
        Err("Partition bounds are only supported on macOS.".to_string())
    }
}

#[tauri::command]
pub fn eject_disk(device_identifier: String) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        let device = if device_identifier.starts_with("/dev/") {
            device_identifier
        } else {
            format!("/dev/{device_identifier}")
        };

        let output = Command::new("diskutil")
            .args(["eject", &device])
            .output()
            .map_err(|e| format!("diskutil failed: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("diskutil error: {stderr}"));
        }

        return Ok(());
    }

    #[cfg(not(target_os = "macos"))]
    {
        Err("Eject not supported on this platform".to_string())
    }
}

fn sidecar_status_for(app: &tauri::AppHandle, binary: &str) -> SidecarStatus {
    let path = find_sidecar(app, binary);
    let mut status = SidecarStatus {
        name: binary.to_string(),
        found: path.is_some(),
        path: path.as_ref().and_then(|p| p.to_str().map(|s| s.to_string())),
        version: None,
    };

    if let Some(path) = path {
        let output = Command::new(&path).arg("--version").output();
        if let Ok(output) = output {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let first = stdout.lines().next().map(|s| s.to_string());
                status.version = first;
            }
        }
    }

    status
}

fn find_sidecar(app: &tauri::AppHandle, binary: &str) -> Option<std::path::PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join(binary));
        }
    }
    if let Ok(path) = app
        .path()
        .resolve(format!("sidecars/{binary}"), BaseDirectory::Resource)
    {
        candidates.push(path);
    }
    candidates.push(std::path::PathBuf::from("/usr/local/bin").join(binary));
    candidates.push(std::path::PathBuf::from("/opt/homebrew/bin").join(binary));

    candidates.into_iter().find(|path| path.exists())
}

#[cfg(target_os = "macos")]
fn partition_bounds_for_disk(disk: &str, device: &str, size: u64) -> Result<(u64, u64), String> {
    use plist::Value;

    let output = Command::new("diskutil")
        .args(["list", "-plist", disk])
        .output()
        .map_err(|e| format!("diskutil failed: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("diskutil error: {stderr}"));
    }

    let plist = Value::from_reader_xml(&output.stdout[..]).map_err(|e| e.to_string())?;
    let dict = plist.as_dictionary().ok_or_else(|| "Invalid plist".to_string())?;
    let partitions = match dict.get("Partitions") {
        Some(Value::Array(parts)) => parts,
        _ => return Err("No partitions".to_string()),
    };

    let mut entries = Vec::new();
    for part in partitions {
        if let Some(part_dict) = part.as_dictionary() {
            let identifier = part_dict
                .get("DeviceIdentifier")
                .and_then(|v| v.as_string())
                .unwrap_or("")
                .to_string();
            let offset = part_dict
                .get("PartitionOffset")
                .and_then(|v| v.as_unsigned_integer())
                .unwrap_or(0);
            let psize = part_dict
                .get("PartitionSize")
                .and_then(|v| v.as_unsigned_integer())
                .unwrap_or(0);
            entries.push((identifier, offset, psize));
        }
    }

    entries.sort_by_key(|entry| entry.1);

    let mut prev_end = 1024 * 1024;
    let mut next_start: Option<u64> = None;
    let current_id = device.trim_start_matches("/dev/");

    for (idx, (identifier, _offset, _psize)) in entries.iter().enumerate() {
        if identifier == current_id {
            if idx > 0 {
                let (.., prev_offset, prev_size) = entries[idx - 1];
                prev_end = prev_offset + prev_size;
            }
            if idx + 1 < entries.len() {
                next_start = Some(entries[idx + 1].1);
            }
            break;
        }
    }

    let max_start = match next_start {
        Some(ns) if ns > size => ns - size,
        _ => prev_end.max(1024 * 1024),
    };

    Ok((prev_end.max(1024 * 1024), max_start))
}
