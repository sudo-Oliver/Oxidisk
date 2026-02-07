pub trait FileSystemDriver {
    fn id(&self) -> &'static str;
    fn mkfs_command(&self, device: &str, label: &str) -> Option<(String, Vec<String>)>;
    fn label_command(&self, device: &str, label: &str) -> Option<(String, Vec<String>)> {
        let _ = device;
        let _ = label;
        None
    }
    fn uuid_command(&self, device: &str, uuid: &str) -> Option<(String, Vec<String>)> {
        let _ = device;
        let _ = uuid;
        None
    }
}

pub struct Ext4Driver;

impl FileSystemDriver for Ext4Driver {
    fn id(&self) -> &'static str {
        "ext4"
    }

    fn mkfs_command(&self, device: &str, label: &str) -> Option<(String, Vec<String>)> {
        Some((
            "mkfs.ext4".to_string(),
            vec!["-F".to_string(), "-L".to_string(), label.to_string(), device.to_string()],
        ))
    }

    fn label_command(&self, device: &str, label: &str) -> Option<(String, Vec<String>)> {
        Some((
            "e2label".to_string(),
            vec![device.to_string(), label.to_string()],
        ))
    }

    fn uuid_command(&self, device: &str, uuid: &str) -> Option<(String, Vec<String>)> {
        Some((
            "tune2fs".to_string(),
            vec!["-U".to_string(), uuid.to_string(), device.to_string()],
        ))
    }
}

pub struct NtfsDriver;

impl FileSystemDriver for NtfsDriver {
    fn id(&self) -> &'static str {
        "ntfs"
    }

    fn mkfs_command(&self, device: &str, label: &str) -> Option<(String, Vec<String>)> {
        Some((
            "mkfs.ntfs".to_string(),
            vec!["-F".to_string(), "-L".to_string(), label.to_string(), device.to_string()],
        ))
    }

    fn label_command(&self, device: &str, label: &str) -> Option<(String, Vec<String>)> {
        Some((
            "ntfslabel".to_string(),
            vec![device.to_string(), label.to_string()],
        ))
    }
}

pub struct BtrfsDriver;

impl FileSystemDriver for BtrfsDriver {
    fn id(&self) -> &'static str {
        "btrfs"
    }

    fn mkfs_command(&self, device: &str, label: &str) -> Option<(String, Vec<String>)> {
        Some((
            "mkfs.btrfs".to_string(),
            vec!["-f".to_string(), "-L".to_string(), label.to_string(), device.to_string()],
        ))
    }

    fn label_command(&self, device: &str, label: &str) -> Option<(String, Vec<String>)> {
        Some((
            "btrfs".to_string(),
            vec![
                "filesystem".to_string(),
                "label".to_string(),
                device.to_string(),
                label.to_string(),
            ],
        ))
    }
}

pub struct XfsDriver;

impl FileSystemDriver for XfsDriver {
    fn id(&self) -> &'static str {
        "xfs"
    }

    fn mkfs_command(&self, device: &str, label: &str) -> Option<(String, Vec<String>)> {
        Some((
            "mkfs.xfs".to_string(),
            vec!["-f".to_string(), "-L".to_string(), label.to_string(), device.to_string()],
        ))
    }

    fn label_command(&self, device: &str, label: &str) -> Option<(String, Vec<String>)> {
        Some((
            "xfs_admin".to_string(),
            vec!["-L".to_string(), label.to_string(), device.to_string()],
        ))
    }
}

pub struct F2fsDriver;

impl FileSystemDriver for F2fsDriver {
    fn id(&self) -> &'static str {
        "f2fs"
    }

    fn mkfs_command(&self, device: &str, _label: &str) -> Option<(String, Vec<String>)> {
        Some(("mkfs.f2fs".to_string(), vec![device.to_string()]))
    }
}

pub struct SwapDriver;

impl FileSystemDriver for SwapDriver {
    fn id(&self) -> &'static str {
        "swap"
    }

    fn mkfs_command(&self, device: &str, label: &str) -> Option<(String, Vec<String>)> {
        Some((
            "mkswap".to_string(),
            vec!["-L".to_string(), label.to_string(), device.to_string()],
        ))
    }

    fn label_command(&self, device: &str, label: &str) -> Option<(String, Vec<String>)> {
        Some((
            "swaplabel".to_string(),
            vec!["-L".to_string(), label.to_string(), device.to_string()],
        ))
    }

    fn uuid_command(&self, device: &str, uuid: &str) -> Option<(String, Vec<String>)> {
        Some((
            "swaplabel".to_string(),
            vec!["-U".to_string(), uuid.to_string(), device.to_string()],
        ))
    }
}

pub fn default_drivers() -> Vec<Box<dyn FileSystemDriver>> {
    vec![
        Box::new(Ext4Driver),
        Box::new(NtfsDriver),
        Box::new(BtrfsDriver),
        Box::new(XfsDriver),
        Box::new(F2fsDriver),
        Box::new(SwapDriver),
    ]
}
