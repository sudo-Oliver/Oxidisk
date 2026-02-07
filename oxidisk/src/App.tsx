import { useEffect, useRef, useState } from "react";
import type { DragEvent } from "react";
import { listen } from "@tauri-apps/api/event";
import { invoke } from "@tauri-apps/api/core";
import { sendNotification } from "@tauri-apps/plugin-notification";
import { open as openDialog } from "@tauri-apps/plugin-dialog";
import {
  AppShell,
  Burger,
  Group,
  NavLink,
  Text,
  Button,
  ThemeIcon,
  ScrollArea,
  Loader,
  Center,
  Breadcrumbs,
  Anchor,
  RingProgress,
  Paper,
  Stack,
  Title,
  Menu,
  Switch,
  useMantineColorScheme,
  ActionIcon,
  Modal,
  Divider,
  NativeSelect,
  TextInput,
  Tooltip,
  Slider,
  NumberInput,
  Badge,
  Table,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import {
  IconChartPie,
  IconSettings,
  IconRefresh,
  IconDeviceFloppy,
  IconSun,
  IconMoon,
  IconFolder,
  IconTrash,
  IconLock,
  IconDatabase,
  IconBrandWindows,
} from "@tabler/icons-react";
import { ResponsiveSunburst } from "@nivo/sunburst";
import {
  SiUbuntu,
  SiLinuxmint,
  SiArchlinux,
  SiApple,
  SiFedora,
  SiDebian,
  SiOpensuse,
  SiManjaro,
  SiKalilinux,
} from "react-icons/si";
import "./App.css";

// --- TYPEN ---
interface SystemDisk {
  name: string;
  mount_point: string;
  total_space: number;
  available_space: number;
  is_removable: boolean;
  is_mounted: boolean;
  device?: string | null;
}

interface FileNode {
  name: string;
  path: string;
  value: number;
  children?: FileNode[];
  fileCount: number;
  modifiedAt?: number;
}

interface PartitionEntry {
  identifier: string;
  name: string;
  size: number;
  offset?: number | null;
  content: string;
  mount_point?: string | null;
  is_protected: boolean;
  protection_reason?: string | null;
  fs_type?: string | null;
}

interface PartitionDevice {
  identifier: string;
  size: number;
  internal: boolean;
  content: string;
  parent_device?: string | null;
  partitions: PartitionEntry[];
  is_protected: boolean;
  protection_reason?: string | null;
}

interface PreflightBattery {
  isLaptop: boolean;
  onAc: boolean;
  percent?: number | null;
}

interface PreflightSidecar {
  name: string;
  found: boolean;
  path?: string | null;
}

interface PreflightProcess {
  pid: number;
  command: string;
}

interface PreflightResult {
  ok: boolean;
  operation?: string;
  device?: string;
  fs?: string;
  blockers: string[];
  warnings: string[];
  busyProcesses: PreflightProcess[];
  battery?: PreflightBattery | null;
  sidecars?: PreflightSidecar[];
  fsCheck?: { ok: boolean; output?: string } | null;
}

interface OperationJournal {
  operation: string;
  device: string;
  disk: string;
  srcOffset?: number;
  dstOffset?: number;
  size?: number;
  blockSize?: number;
  lastCopied?: number;
  updatedAt?: number;
}

interface ApfsVolumeInfo {
  identifier: string;
  name: string;
  roles: string[];
  size: number;
  used: number;
  mountPoint?: string | null;
}

interface ApfsContainerInfo {
  containerIdentifier: string;
  containerUuid?: string | null;
  capacity?: number | null;
  capacityFree?: number | null;
  capacityUsed?: number | null;
  volumes: ApfsVolumeInfo[];
}

const CHART_COLORS = ["#0A84FF", "#5E5CE6", "#64D2FF", "#30D158", "#40CBE0", "#7DDBEE"];

// --- HELPER ---
function formatBytes(bytes: number, decimals = 1) {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB", "TB", "PB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(decimals))} ${sizes[i]}`;
}

function formatDate(seconds: number) {
  return new Date(seconds * 1000).toLocaleString();
}

function truncateMiddle(value: string, maxLength: number) {
  if (value.length <= maxLength) return value;
  const keep = Math.max(4, Math.floor((maxLength - 3) / 2));
  const start = value.slice(0, keep);
  const end = value.slice(value.length - keep);
  return `${start}...${end}`;
}

function formatEta(seconds: number | null) {
  if (seconds == null || !Number.isFinite(seconds)) return "";
  const total = Math.max(0, Math.round(seconds));
  const mins = Math.floor(total / 60);
  const secs = total % 60;
  if (mins <= 0) return `${secs}s`;
  return `${mins}m ${secs}s`;
}

function fsTypeFromPartition(partition: PartitionEntry | null) {
  if (!partition) return "unknown";
  const normalized = (partition.fs_type ?? partition.content ?? "").toLowerCase();
  if (normalized.includes("apfs")) return "apfs";
  if (normalized.includes("exfat")) return "exfat";
  if (normalized.includes("ms-dos") || normalized.includes("fat32") || normalized.includes("fat")) return "fat32";
  if (normalized.includes("ntfs")) return "ntfs";
  if (normalized.includes("ext4") || normalized.includes("linux")) return "ext4";
  if (normalized.includes("btrfs")) return "btrfs";
  if (normalized.includes("xfs")) return "xfs";
  if (normalized.includes("f2fs")) return "f2fs";
  if (normalized.includes("swap")) return "swap";
  return "unknown";
}

type PartitionSegment = {
  key: string;
  label: string;
  size: number;
  offset: number;
  fsType: string;
  kind: "partition" | "unallocated";
  partition?: PartitionEntry;
};

function fsColorForSegment(fsType: string, kind: "partition" | "unallocated") {
  if (kind === "unallocated") return "var(--oxidisk-unallocated)";
  switch (fsType) {
    case "apfs":
      return "var(--oxidisk-apfs)";
    case "exfat":
      return "var(--oxidisk-exfat)";
    case "fat32":
      return "var(--oxidisk-fat32)";
    case "ntfs":
      return "var(--oxidisk-ntfs)";
    case "ext4":
      return "var(--oxidisk-ext4)";
    case "btrfs":
      return "var(--oxidisk-btrfs)";
    case "xfs":
      return "var(--oxidisk-xfs)";
    case "f2fs":
      return "var(--oxidisk-f2fs)";
    case "swap":
      return "var(--oxidisk-swap)";
    default:
      return "var(--oxidisk-unknown)";
  }
}

function fsLabelForType(fsType: string, kind: "partition" | "unallocated") {
  if (kind === "unallocated") return "Unallocated";
  switch (fsType) {
    case "apfs":
      return "APFS";
    case "exfat":
      return "exFAT";
    case "fat32":
      return "FAT32";
    case "ntfs":
      return "NTFS";
    case "ext4":
      return "ext4";
    case "btrfs":
      return "Btrfs";
    case "xfs":
      return "XFS";
    case "f2fs":
      return "F2FS";
    case "swap":
      return "Swap";
    default:
      return "Unknown";
  }
}

function buildPartitionSegments(device: PartitionDevice | null): PartitionSegment[] {
  if (!device) return [];
  const partitions = device.partitions.map((part) => ({
    partition: part,
    offset: part.offset ?? null,
    size: part.size,
  }));
  const hasOffsets = partitions.every((part) => part.offset != null);
  const sorted = hasOffsets
    ? [...partitions].sort((a, b) => (a.offset ?? 0) - (b.offset ?? 0))
    : [...partitions];

  const segments: PartitionSegment[] = [];
  let cursor = 0;

  for (const entry of sorted) {
    const offset = entry.offset ?? cursor;
    if (offset > cursor) {
      segments.push({
        key: `unallocated-${cursor}`,
        label: "Unallocated",
        size: offset - cursor,
        offset: cursor,
        fsType: "unallocated",
        kind: "unallocated",
      });
    }

    const part = entry.partition;
    const fsType = fsTypeFromPartition(part);
    segments.push({
      key: part.identifier,
      label: part.name || part.identifier,
      size: part.size,
      offset,
      fsType,
      kind: "partition",
      partition: part,
    });

    cursor = offset + entry.size;
  }

  if (cursor < device.size) {
    segments.push({
      key: `unallocated-${cursor}`,
      label: "Unallocated",
      size: device.size - cursor,
      offset: cursor,
      fsType: "unallocated",
      kind: "unallocated",
    });
  }

  return segments;
}

function renderImageBrandIcon(brand: string | null) {
  switch (brand) {
    case "ubuntu":
      return <SiUbuntu size={64} color="#E95420" />;
    case "mint":
      return <SiLinuxmint size={64} color="#87CF3E" />;
    case "arch":
      return <SiArchlinux size={64} color="#1793D1" />;
    case "windows":
      return <IconBrandWindows size={64} color="#3b82f6" />;
    case "macos":
      return <SiApple size={64} color="#6b7280" />;
    case "fedora":
      return <SiFedora size={64} color="#3c6df0" />;
    case "debian":
      return <SiDebian size={64} color="#D70A53" />;
    case "opensuse":
      return <SiOpensuse size={64} color="#73BA25" />;
    case "manjaro":
      return <SiManjaro size={64} color="#35BF5C" />;
    case "kali":
      return <SiKalilinux size={64} color="#268BEE" />;
    default:
      return null;
  }
}

export default function App() {
  const [opened, { toggle }] = useDisclosure();
  const [activeView, setActiveView] = useState<"analyzer" | "partition" | "images">("analyzer");
  const [disks, setDisks] = useState<SystemDisk[]>([]);
  const [scanData, setScanData] = useState<FileNode | null>(null);
  const [loading, setLoading] = useState(false);
  const [showPowerDataInspector, setShowPowerDataInspector] = useState(true);
  const [showSystemVolumes, setShowSystemVolumes] = useState(false);
  const [partitionDevices, setPartitionDevices] = useState<PartitionDevice[]>([]);
  const [partitionLoading, setPartitionLoading] = useState(false);
  const [selectedPartitionDeviceId, setSelectedPartitionDeviceId] = useState<string | null>(null);
  const [selectedPartitionId, setSelectedPartitionId] = useState<string | null>(null);
  const [selectedUnallocated, setSelectedUnallocated] = useState<{ offset: number; size: number } | null>(null);
  const [partitionBarWidth, setPartitionBarWidth] = useState(0);
  const [preflightResult, setPreflightResult] = useState<PreflightResult | null>(null);
  const [preflightKey, setPreflightKey] = useState<string | null>(null);
  const [preflightRunning, setPreflightRunning] = useState(false);
  const [preflightError, setPreflightError] = useState<string | null>(null);
  const [journalInfo, setJournalInfo] = useState<OperationJournal | null>(null);
  const [journalOpen, setJournalOpen] = useState(false);
  const [wipeWizardOpen, setWipeWizardOpen] = useState(false);
  const [selectedWipeDevice, setSelectedWipeDevice] = useState<PartitionDevice | null>(null);
  const [wipeConfirmText, setWipeConfirmText] = useState("");
  const [wipeLabel, setWipeLabel] = useState("OXIDISK");
  const [wipeTableType, setWipeTableType] = useState("gpt");
  const [wipeFormatType, setWipeFormatType] = useState("exfat");
  const [wipeError, setWipeError] = useState<string | null>(null);
  const [wipeSubmitting, setWipeSubmitting] = useState(false);
  const [wipeSuccess, setWipeSuccess] = useState<string | null>(null);
  const [formatWizardOpen, setFormatWizardOpen] = useState(false);
  const [selectedPartition, setSelectedPartition] = useState<PartitionEntry | null>(null);
  const [formatType, setFormatType] = useState("exfat");
  const [formatLabel, setFormatLabel] = useState("");
  const [formatError, setFormatError] = useState<string | null>(null);
  const [formatSubmitting, setFormatSubmitting] = useState(false);
  const [formatSuccess, setFormatSuccess] = useState<string | null>(null);
  const [labelWizardOpen, setLabelWizardOpen] = useState(false);
  const [labelValue, setLabelValue] = useState("");
  const [uuidValue, setUuidValue] = useState("");
  const [labelError, setLabelError] = useState<string | null>(null);
  const [labelSubmitting, setLabelSubmitting] = useState(false);
  const [labelSuccess, setLabelSuccess] = useState<string | null>(null);
  const [apfsManagerOpen, setApfsManagerOpen] = useState(false);
  const [apfsTarget, setApfsTarget] = useState<PartitionEntry | null>(null);
  const [apfsContainer, setApfsContainer] = useState<ApfsContainerInfo | null>(null);
  const [apfsLoading, setApfsLoading] = useState(false);
  const [apfsError, setApfsError] = useState<string | null>(null);
  const [apfsAddName, setApfsAddName] = useState("");
  const [apfsAddRole, setApfsAddRole] = useState("None");
  const [apfsAddSubmitting, setApfsAddSubmitting] = useState(false);
  const [apfsAddError, setApfsAddError] = useState<string | null>(null);
  const [apfsDeleteBusy, setApfsDeleteBusy] = useState<string | null>(null);
  const [imagePath, setImagePath] = useState<string | null>(null);
  const [imageMode, setImageMode] = useState<"write" | "backup" | "windows">("write");
  const [imageTarget, setImageTarget] = useState<string>("");
  const [imageVerify, setImageVerify] = useState(true);
  const [imageAutoEject, setImageAutoEject] = useState(true);
  const [imageBackupPath, setImageBackupPath] = useState("");
  const [imageBackupCompress, setImageBackupCompress] = useState(true);
  const [imageConfirmText, setImageConfirmText] = useState("");
  const [imageError, setImageError] = useState<string | null>(null);
  const [imageSuccess, setImageSuccess] = useState<string | null>(null);
  const [imageRunning, setImageRunning] = useState(false);
  const [showAllImageTargets, setShowAllImageTargets] = useState(false);
  const [imageShowLog, setImageShowLog] = useState(false);
  const [imageResultMount, setImageResultMount] = useState<string | null>(null);
  const [imageHash, setImageHash] = useState<string | null>(null);
  const [imageHashError, setImageHashError] = useState<string | null>(null);
  const [imageHashRunning, setImageHashRunning] = useState(false);
  const [imageDropActive, setImageDropActive] = useState(false);
  const [imageWindowsDetected, setImageWindowsDetected] = useState(false);
  const [imageWindowsReason, setImageWindowsReason] = useState<string | null>(null);
  const [imageBrand, setImageBrand] = useState<string | null>(null);
  const [imageLabel, setImageLabel] = useState<string | null>(null);
  const [imageWindowsOverride, setImageWindowsOverride] = useState(false);
  const [imageWindowsLabel, setImageWindowsLabel] = useState("WINSTALL");
  const [imageWindowsSheetOpen, setImageWindowsSheetOpen] = useState(false);
  const [imageWindowsSheetConfirmed, setImageWindowsSheetConfirmed] = useState(false);
  const [imageWinTpmBypass, setImageWinTpmBypass] = useState(false);
  const [imageWinLocalAccount, setImageWinLocalAccount] = useState(false);
  const [imageWinPrivacyDefaults, setImageWinPrivacyDefaults] = useState(false);
  const [createWizardOpen, setCreateWizardOpen] = useState(false);
  const [createDevice, setCreateDevice] = useState<PartitionDevice | null>(null);
  const [createFormatType, setCreateFormatType] = useState("exfat");
  const [createLabel, setCreateLabel] = useState("");
  const [createSizeValue, setCreateSizeValue] = useState<number | undefined>(undefined);
  const [createSizeUnit, setCreateSizeUnit] = useState("gb");
  const [createError, setCreateError] = useState<string | null>(null);
  const [createSubmitting, setCreateSubmitting] = useState(false);
  const [createSuccess, setCreateSuccess] = useState<string | null>(null);
  const [deleteWizardOpen, setDeleteWizardOpen] = useState(false);
  const [deletePartition, setDeletePartition] = useState<PartitionEntry | null>(null);
  const [deleteConfirmText, setDeleteConfirmText] = useState("");
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [deleteSubmitting, setDeleteSubmitting] = useState(false);
  const [deleteSuccess, setDeleteSuccess] = useState<string | null>(null);
  const [checkWizardOpen, setCheckWizardOpen] = useState(false);
  const [checkPartition, setCheckPartition] = useState<PartitionEntry | null>(null);
  const [checkRepair, setCheckRepair] = useState(false);
  const [checkError, setCheckError] = useState<string | null>(null);
  const [checkSubmitting, setCheckSubmitting] = useState(false);
  const [checkOutput, setCheckOutput] = useState<string | null>(null);
  const [resizeWizardOpen, setResizeWizardOpen] = useState(false);
  const [resizePartition, setResizePartition] = useState<PartitionEntry | null>(null);
  const [resizeValue, setResizeValue] = useState<number | undefined>(undefined);
  const [resizeUnit, setResizeUnit] = useState("gb");
  const [resizeError, setResizeError] = useState<string | null>(null);
  const [resizeSubmitting, setResizeSubmitting] = useState(false);
  const [resizeSuccess, setResizeSuccess] = useState<string | null>(null);
  const [moveWizardOpen, setMoveWizardOpen] = useState(false);
  const [movePartition, setMovePartition] = useState<PartitionEntry | null>(null);
  const [moveStartValue, setMoveStartValue] = useState<number | undefined>(undefined);
  const [moveUnit, setMoveUnit] = useState("mb");
  const [moveError, setMoveError] = useState<string | null>(null);
  const [moveSubmitting, setMoveSubmitting] = useState(false);
  const [moveSuccess, setMoveSuccess] = useState<string | null>(null);
  const [moveBounds, setMoveBounds] = useState<{ minStart: number; maxStart: number; offset: number } | null>(null);
  const [resizeOutput, setResizeOutput] = useState<string | null>(null);
  const [moveOutput, setMoveOutput] = useState<string | null>(null);
  const [sidecarOpen, setSidecarOpen] = useState(false);
  const [sidecarLoading, setSidecarLoading] = useState(false);
  const [sidecarStatus, setSidecarStatus] = useState<
    { name: string; found: boolean; path?: string | null; version?: string | null }[]
  >([]);
  const [progressOpen, setProgressOpen] = useState(false);
  const [progressPercent, setProgressPercent] = useState(0);
  const [progressMessage, setProgressMessage] = useState<string | null>(null);
  const [progressBytes, setProgressBytes] = useState<{ current: number; total: number } | null>(null);
  const [progressLog, setProgressLog] = useState<string[]>([]);
  const [progressSpeed, setProgressSpeed] = useState<number | null>(null);
  const [progressEta, setProgressEta] = useState<number | null>(null);
  const lastProgressRef = useRef<{ time: number; bytes: number } | null>(null);
  const imageRunningRef = useRef(false);
  const partitionBarRef = useRef<HTMLDivElement | null>(null);
  const [clipboardPartition, setClipboardPartition] = useState<PartitionEntry | null>(null);
  const [clipboardFs, setClipboardFs] = useState<string | null>(null);
  const [pasteWizardOpen, setPasteWizardOpen] = useState(false);
  const [pasteTargetDevice, setPasteTargetDevice] = useState<PartitionDevice | null>(null);
  const [pasteConfirmText, setPasteConfirmText] = useState("");
  const [pasteError, setPasteError] = useState<string | null>(null);
  const [pasteSubmitting, setPasteSubmitting] = useState(false);
  const [pasteOutput, setPasteOutput] = useState<string | null>(null);
  const [pasteSuccess, setPasteSuccess] = useState<string | null>(null);
  const [tableWizardOpen, setTableWizardOpen] = useState(false);
  const [tableDevice, setTableDevice] = useState<PartitionDevice | null>(null);
  const [tableType, setTableType] = useState("gpt");
  const [tableConfirmText, setTableConfirmText] = useState("");
  const [tableError, setTableError] = useState<string | null>(null);
  const [tableSubmitting, setTableSubmitting] = useState(false);
  const [tableSuccess, setTableSuccess] = useState<string | null>(null);
  const [sudoSetupOpen, setSudoSetupOpen] = useState(false);
  const [sudoSetupLoading, setSudoSetupLoading] = useState(false);
  const [sudoSetupMessage, setSudoSetupMessage] = useState<string | null>(null);
  const [sudoSetupError, setSudoSetupError] = useState<string | null>(null);
  const { colorScheme, setColorScheme } = useMantineColorScheme();
  const [selectedNode, setSelectedNode] = useState<FileNode | null>(null);
  const [trashTarget, setTrashTarget] = useState<FileNode | null>(null);
  const [confirmOpen, { open: openConfirm, close: closeConfirm }] = useDisclosure(false);

  // State für Navigation
  const [currentDisk, setCurrentDisk] = useState<SystemDisk | null>(null);
  const [currentRootName, setCurrentRootName] = useState<string | null>(null);
  const [currentRootPath, setCurrentRootPath] = useState<string | null>(null);
  const [pathParts, setPathParts] = useState<string[]>([]);

  async function loadDisks() {
    try {
      const result = await invoke<SystemDisk[]>("get_disks", { includeSystem: showSystemVolumes });
      setDisks(result);
    } catch (error) {
      console.error(error);
    }
  }

  async function loadPartitionDevices() {
    if (partitionLoading) return;
    setPartitionLoading(true);
    try {
      const result = await invoke<PartitionDevice[]>("get_partition_devices");
      setPartitionDevices(result);
    } catch (error) {
      console.error(error);
    } finally {
      setPartitionLoading(false);
    }
  }

  function openWipeWizard(device: PartitionDevice) {
    setSelectedWipeDevice(device);
    setWipeConfirmText("");
    setWipeLabel("OXIDISK");
    setWipeTableType("gpt");
    setWipeFormatType("exfat");
    setWipeError(null);
    setWipeSuccess(null);
    resetPreflight();
    setWipeWizardOpen(true);
    runPreflight({
      operation: "wipe",
      deviceIdentifier: device.identifier,
      formatType: "exfat",
    });
  }

  function supportsAutoMount(formatType: string) {
    return ["exfat", "fat32", "apfs", "hfs+"].includes(formatType.toLowerCase());
  }

  function isMacMountable(formatType: string) {
    return ["exfat", "fat32", "apfs", "hfs+"].includes(formatType.toLowerCase());
  }

  function isExoticFs(formatType: string) {
    return ["btrfs", "xfs", "f2fs", "nilfs2", "swap"].includes(formatType.toLowerCase());
  }

  function preflightKeyFor(params: {
    operation: string;
    device?: string | null;
    formatType?: string | null;
    newSize?: string | null;
  }) {
    return [params.operation, params.device ?? "", params.formatType ?? "", params.newSize ?? ""].join("|");
  }

  function resetPreflight() {
    setPreflightResult(null);
    setPreflightKey(null);
    setPreflightError(null);
  }

  function preflightReady(key: string) {
    return !!preflightResult?.ok && preflightKey === key;
  }

  async function runPreflight(params: {
    operation: string;
    deviceIdentifier?: string;
    partitionIdentifier?: string;
    formatType?: string;
    newSize?: string;
  }) {
    const key = preflightKeyFor({
      operation: params.operation,
      device: params.partitionIdentifier ?? params.deviceIdentifier ?? "",
      formatType: params.formatType ?? "",
      newSize: params.newSize ?? "",
    });
    setPreflightRunning(true);
    setPreflightError(null);
    try {
      const result = await invoke<{ details?: PreflightResult }>("preflight_partition", params);
      setPreflightResult(result.details ?? null);
      setPreflightKey(key);
    } catch (error) {
      setPreflightError(String(error));
      setPreflightResult(null);
      setPreflightKey(null);
    } finally {
      setPreflightRunning(false);
    }
  }

  async function forceUnmount(params: { deviceIdentifier?: string; partitionIdentifier?: string }) {
    try {
      await invoke("force_unmount_partition", params);
    } catch (error) {
      setPreflightError(String(error));
    }
  }

  async function loadOperationJournal() {
    try {
      const result = await invoke<{ details?: OperationJournal }>("get_operation_journal");
      if (result.details) {
        setJournalInfo(result.details);
        setJournalOpen(true);
      }
    } catch (error) {
      console.error(error);
    }
  }

  async function clearOperationJournal() {
    try {
      await invoke("clear_operation_journal");
    } catch (error) {
      console.error(error);
    }
  }

  async function handleJournalRepair() {
    if (!journalInfo?.device) return;
    try {
      const result = await invoke<{ details?: { output?: string } }>("check_partition", {
        partitionIdentifier: journalInfo.device,
        repair: true,
      });
      const output = result?.details?.output ?? "Repair abgeschlossen.";
      setCheckOutput(output);
    } catch (error) {
      setCheckOutput(String(error));
    } finally {
      await clearOperationJournal();
      setJournalOpen(false);
      setJournalInfo(null);
    }
  }

  function baseDiskIdentifier(identifier: string) {
    const match = identifier.match(/^(disk\d+)/);
    return match ? match[1] : identifier;
  }

  function deviceFreeBytes(device: PartitionDevice) {
    const used = device.partitions.reduce((sum, part) => sum + (part.size || 0), 0);
    return Math.max(0, device.size - used);
  }

  function deviceHasApfs(device: PartitionDevice) {
    return device.partitions.some((part) => {
      const normalized = (part.fs_type ?? part.content ?? "").toLowerCase();
      return normalized.includes("apfs") || normalized.includes("apple_apfs");
    });
  }

  const apfsRoleOptions = ["None", "System", "Data", "Preboot", "Recovery", "VM"];
  const apfsProtectedRoles = new Set(["System", "Data", "Preboot", "Recovery", "VM"]);

  function apfsVolumeProtected(volume: ApfsVolumeInfo) {
    return volume.roles.some((role) => apfsProtectedRoles.has(role));
  }

  async function loadApfsContainer(containerIdentifier: string) {
    setApfsLoading(true);
    setApfsError(null);
    try {
      const result = await invoke<ApfsContainerInfo>("apfs_list_volumes", { containerIdentifier });
      setApfsContainer(result);
    } catch (error) {
      setApfsError(String(error));
    } finally {
      setApfsLoading(false);
    }
  }

  function openApfsManager(partition: PartitionEntry) {
    setApfsTarget(partition);
    setApfsAddName("");
    setApfsAddRole("None");
    setApfsAddError(null);
    setApfsContainer(null);
    setApfsManagerOpen(true);
    loadApfsContainer(partition.identifier);
  }

  async function submitApfsAddVolume() {
    if (!apfsTarget) return;
    const name = apfsAddName.trim();
    if (!name) {
      setApfsAddError("Bitte einen Volume-Namen angeben.");
      return;
    }

    setApfsAddSubmitting(true);
    setApfsAddError(null);
    try {
      await invoke("apfs_add_volume", {
        containerIdentifier: apfsTarget.identifier,
        name,
        role: showPowerDataInspector ? apfsAddRole : "None",
      });
      setApfsAddName("");
      await loadApfsContainer(apfsTarget.identifier);
    } catch (error) {
      setApfsAddError(String(error));
    } finally {
      setApfsAddSubmitting(false);
    }
  }

  async function submitApfsDeleteVolume(volume: ApfsVolumeInfo) {
    if (!apfsTarget) return;
    if (!window.confirm(`Volume ${volume.identifier} wirklich loeschen?`)) {
      return;
    }

    setApfsDeleteBusy(volume.identifier);
    try {
      await invoke("apfs_delete_volume", { volumeIdentifier: volume.identifier });
      await loadApfsContainer(apfsTarget.identifier);
    } catch (error) {
      setApfsError(String(error));
    } finally {
      setApfsDeleteBusy(null);
    }
  }

  async function chooseImageFile() {
    try {
      const selected = await openDialog({
        multiple: false,
        title: "Image auswaehlen",
        filters: [{ name: "Images", extensions: ["iso", "img", "dmg", "dd"] }],
      });
      if (typeof selected === "string") {
        await setImageFile(selected);
      }
    } catch (error) {
      setImageError(String(error));
    }
  }

  async function handleImageDrop(event: DragEvent<HTMLDivElement>) {
    event.preventDefault();
    event.stopPropagation();
    setImageDropActive(false);
    const file = event.dataTransfer.files?.[0];
    const filePath = (file as File & { path?: string }).path;
    if (filePath) {
      await setImageFile(filePath);
    }
  }

  async function setImageFile(selected: string) {
    setImagePath(selected);
    setImageError(null);
    setImageSuccess(null);
    setImageHash(null);
    setImageHashError(null);
    setImageWindowsDetected(false);
    setImageWindowsReason(null);
    setImageBrand(null);
    setImageLabel(null);
    setImageWindowsOverride(false);
    setImageWindowsSheetConfirmed(false);
    try {
      const result = await invoke<{
        details?: {
          isWindows?: boolean;
          reason?: string | null;
          brand?: string | null;
          label?: string | null;
        };
      }>(
        "inspect_image",
        { sourcePath: selected }
      );
      const isWindows = !!result?.details?.isWindows;
      const reason = result?.details?.reason ?? null;
      const brand = result?.details?.brand ?? null;
      const label = result?.details?.label ?? null;
      setImageWindowsDetected(isWindows);
      setImageWindowsReason(reason);
      setImageBrand(brand);
      setImageLabel(label);
    } catch (error) {
      setImageWindowsDetected(false);
      setImageWindowsReason(null);
      setImageBrand(null);
      setImageLabel(null);
    }
  }

  function buildBackupName(deviceIdentifier: string, compress: boolean) {
    const safe = deviceIdentifier.replace(/[^a-zA-Z0-9_-]/g, "-");
    const suffix = compress ? ".img.gz" : ".img";
    return `oxidisk-${safe}${suffix}`;
  }

  async function chooseBackupTarget() {
    try {
      const selected = await openDialog({ directory: true, multiple: false, title: "Zielordner waehlen" });
      if (typeof selected === "string") {
        const fileName = buildBackupName(imageTarget || "disk", imageBackupCompress);
        const normalized = selected.endsWith("/") ? selected.slice(0, -1) : selected;
        setImageBackupPath(`${normalized}/${fileName}`);
        setImageError(null);
      }
    } catch (error) {
      setImageError(String(error));
    }
  }

  async function computeImageHash() {
    if (!imagePath) return;
    setImageHashRunning(true);
    setImageHashError(null);
    setProgressLog([]);
    setProgressOpen(true);
    setProgressMessage("Hashing image");
    try {
      const result = await invoke<{ details?: { sha256?: string } }>("hash_image", { sourcePath: imagePath });
      setImageHash(result?.details?.sha256 ?? null);
    } catch (error) {
      setImageHashError(String(error));
      setImageHash(null);
    } finally {
      setImageHashRunning(false);
      setProgressOpen(false);
      setProgressMessage(null);
      setProgressBytes(null);
    }
  }

  function imageTargetOptions() {
    const isApfsContainer = (device: PartitionDevice) =>
      device.content.toLowerCase().includes("apple_apfs_container");
    const physicalDevices = partitionDevices.filter((device) => !isApfsContainer(device));
    const targets = showAllImageTargets
      ? physicalDevices
      : physicalDevices.filter((device) => !device.internal);

    const containerMap = new Map<string, PartitionDevice[]>();
    const orphanContainers: PartitionDevice[] = [];
    for (const device of partitionDevices) {
      if (!isApfsContainer(device)) continue;
      if (device.parent_device) {
        const list = containerMap.get(device.parent_device) ?? [];
        list.push(device);
        containerMap.set(device.parent_device, list);
      } else {
        orphanContainers.push(device);
      }
    }

    const options: { value: string; label: string; disabled?: boolean }[] = [];
    for (const device of targets) {
      options.push({
        value: device.identifier,
        label: `${device.identifier} · ${formatBytes(device.size)} · ${device.internal ? "Intern" : "Extern"}`,
      });
      if (showAllImageTargets) {
        const containers = containerMap.get(device.identifier) ?? [];
        for (const container of containers) {
          options.push({
            value: `container:${container.identifier}`,
            label: `└── APFS Container (${container.identifier})`,
            disabled: true,
          });
        }
      }
    }

    if (showAllImageTargets && orphanContainers.length > 0) {
      for (const container of orphanContainers) {
        options.push({
          value: `container:${container.identifier}`,
          label: `APFS Container (${container.identifier})`,
          disabled: true,
        });
      }
    }

    return options;
  }

  async function submitFlashImage() {
    if (imageMode === "backup") {
      await submitBackupImage();
      return;
    }
    if (imageMode === "windows") {
      await submitWindowsInstall();
      return;
    }
    if (!imagePath) {
      setImageError("Bitte ein Image auswaehlen.");
      return;
    }
    if (!imageTarget) {
      setImageError("Bitte ein Zielgeraet auswaehlen.");
      return;
    }
    if (imageWindowsDetected && !imageWindowsOverride) {
      setImageError("Windows-ISO erkannt. Nutze spaeter den Windows-Installer-Modus.");
      return;
    }
    if (imageConfirmText.trim() !== imageTarget) {
      setImageError("Bitte die Device-ID exakt eingeben.");
      return;
    }

    setImageRunning(true);
    setImageError(null);
    setImageSuccess(null);
    setProgressLog([]);
    setProgressOpen(true);
    setProgressMessage("Starte Flash...");
    setProgressSpeed(null);
    setProgressEta(null);
    lastProgressRef.current = null;

    try {
      const result = await invoke<{ details?: { sourceHash?: string; verifiedHash?: string } }>("flash_image", {
        sourcePath: imagePath,
        targetDevice: imageTarget,
        verify: imageVerify,
      });
      const sourceHash = result?.details?.sourceHash;
      const verifiedHash = result?.details?.verifiedHash;
      const message = sourceHash
        ? `Flash abgeschlossen. SHA-256: ${sourceHash}${verifiedHash ? " (verifiziert)" : ""}`
        : "Flash abgeschlossen.";
      setImageSuccess(message);
      setImageResultMount(imageBackupPath.trim() || null);
      sendNotification({ title: "Oxidisk", body: message });
      if (imageAutoEject) {
        try {
          await invoke("eject_disk", { deviceIdentifier: imageTarget });
        } catch (error) {
          setImageError(String(error));
        }
      }
      setImageConfirmText("");
      await loadPartitionDevices();
    } catch (error) {
      setImageError(String(error));
    } finally {
      setImageRunning(false);
      setProgressOpen(false);
      setProgressMessage(null);
      setProgressBytes(null);
      setProgressSpeed(null);
      setProgressEta(null);
    }
  }

  async function submitBackupImage() {
    if (!imageTarget) {
      setImageError("Bitte ein Quellgeraet auswaehlen.");
      return;
    }
    if (!imageBackupPath.trim()) {
      setImageError("Bitte einen Zielpfad fuer das Backup angeben.");
      return;
    }
    if (imageConfirmText.trim() !== imageTarget) {
      setImageError("Bitte die Device-ID exakt eingeben.");
      return;
    }

    setImageRunning(true);
    setImageError(null);
    setImageSuccess(null);
    setProgressLog([]);
    setProgressOpen(true);
    setProgressMessage("Backup wird erstellt...");
    setProgressSpeed(null);
    setProgressEta(null);
    lastProgressRef.current = null;

    try {
      await invoke("backup_image", {
        sourceDevice: imageTarget,
        targetPath: imageBackupPath.trim(),
        compress: imageBackupCompress,
      });
      const message = "Backup abgeschlossen und verifiziert.";
      setImageSuccess(message);
      setImageResultMount(null);
      sendNotification({ title: "Oxidisk", body: message });
      if (imageAutoEject) {
        try {
          await invoke("eject_disk", { deviceIdentifier: imageTarget });
        } catch (error) {
          setImageError(String(error));
        }
      }
      setImageConfirmText("");
    } catch (error) {
      setImageError(String(error));
    } finally {
      setImageRunning(false);
      setProgressOpen(false);
      setProgressMessage(null);
      setProgressBytes(null);
      setProgressSpeed(null);
      setProgressEta(null);
    }
  }

  async function submitWindowsInstall() {
    if (!imagePath) {
      setImageError("Bitte ein Windows-ISO auswaehlen.");
      return;
    }
    if (!imageTarget) {
      setImageError("Bitte ein Zielgeraet auswaehlen.");
      return;
    }
    if (!imageWindowsDetected) {
      setImageError("Keine Windows-ISO erkannt. Bitte eine Windows-ISO waehlen.");
      return;
    }
    if (!imageWindowsSheetConfirmed) {
      setImageWindowsSheetOpen(true);
      return;
    }
    if (imageConfirmText.trim() !== imageTarget) {
      setImageError("Bitte die Device-ID exakt eingeben.");
      return;
    }

    setImageRunning(true);
    setImageError(null);
    setImageSuccess(null);
    setProgressLog([]);
    setProgressOpen(true);
    setProgressMessage("Windows Installer wird erstellt...");
    setProgressSpeed(null);
    setProgressEta(null);
    lastProgressRef.current = null;

    try {
      const result = await invoke<{ details?: { mountPoint?: string | null } }>("windows_install", {
        sourcePath: imagePath,
        targetDevice: imageTarget,
        label: imageWindowsLabel.trim() || "WINSTALL",
        tpmBypass: imageWinTpmBypass,
        localAccount: imageWinLocalAccount,
        privacyDefaults: imageWinPrivacyDefaults,
      });
      const message = "Windows-Installer abgeschlossen.";
      setImageSuccess(message);
      setImageResultMount(result?.details?.mountPoint ?? null);
      sendNotification({ title: "Oxidisk", body: message });
      if (imageAutoEject) {
        try {
          await invoke("eject_disk", { deviceIdentifier: imageTarget });
        } catch (error) {
          setImageError(String(error));
        }
      }
      setImageConfirmText("");
      await loadPartitionDevices();
    } catch (error) {
      setImageError(String(error));
    } finally {
      setImageRunning(false);
      setProgressOpen(false);
      setProgressMessage(null);
      setProgressBytes(null);
      setProgressSpeed(null);
      setProgressEta(null);
    }
  }

  function openCreateWizard(device: PartitionDevice) {
    const freeBytes = deviceFreeBytes(device);
    const freeGb = freeBytes / (1024 * 1024 * 1024);
    const initial = freeGb > 0.1 ? Math.min(1, Math.floor(freeGb * 10) / 10) : 0;
    setCreateDevice(device);
    setCreateFormatType("exfat");
    setCreateLabel("OXIDISK");
    setCreateSizeUnit("gb");
    setCreateSizeValue(initial);
    setCreateError(null);
    setCreateSuccess(null);
    resetPreflight();
    setCreateWizardOpen(true);
    runPreflight({
      operation: "create",
      deviceIdentifier: device.identifier,
      formatType: "exfat",
    });
  }

  function labelMaxLength(formatType: string) {
    const normalized = formatType.toLowerCase();
    if (normalized === "fat32") return 11;
    if (normalized === "exfat") return 15;
    return 32;
  }

  function normalizeLabelInput(value: string, formatType: string) {
    if (formatType === "fat32") {
      return value.toUpperCase();
    }
    return value;
  }

  function createSizeBytes() {
    if (!createSizeValue) return 0;
    const multiplier = createSizeUnit === "mb" ? 1024 * 1024 : 1024 * 1024 * 1024;
    return Math.floor(createSizeValue * multiplier);
  }

  function createSizeString() {
    if (!createSizeValue) return "0g";
    const unit = createSizeUnit === "mb" ? "m" : "g";
    return `${createSizeValue}${unit}`;
  }

  function validateCreateInputs() {
    if (!createDevice) return "Kein Geraet ausgewaehlt.";
    const label = createLabel.trim();
    if (!label) return "Bitte ein Label angeben.";
    if (createFormatType === "fat32" && label.length > 11) {
      return "FAT32-Labels duerfen maximal 11 Zeichen haben.";
    }
    if (createFormatType === "exfat" && label.length > 15) {
      return "exFAT-Labels sollten maximal 15 Zeichen haben.";
    }
    const sizeBytes = createSizeBytes();
    if (sizeBytes <= 0) return "Bitte eine Groesse angeben.";
    if (sizeBytes > deviceFreeBytes(createDevice)) {
      return "Groesse uebersteigt den freien Speicher.";
    }
    return null;
  }

  async function submitCreateWizard() {
    if (!createDevice) return;
    const preflightKey = preflightKeyFor({
      operation: "create",
      device: createDevice.identifier,
      formatType: createFormatType,
    });
    if (!preflightReady(preflightKey)) {
      setCreateError("Bitte Safety-Preflight ausfuehren.");
      return;
    }
    const validation = validateCreateInputs();
    if (validation) {
      setCreateError(validation);
      return;
    }

    setCreateSubmitting(true);
    setCreateError(null);
    setCreateSuccess(null);
    try {
      await invoke("create_partition", {
        deviceIdentifier: createDevice.identifier,
        formatType: createFormatType,
        label: createLabel.trim(),
        size: createSizeString(),
      });
      setCreateSuccess("Partition erstellt.");
      setCreateWizardOpen(false);
      await loadPartitionDevices();
    } catch (error) {
      setCreateError(String(error));
    } finally {
      setCreateSubmitting(false);
    }
  }

  function openDeleteWizard(partition: PartitionEntry) {
    setDeletePartition(partition);
    setDeleteConfirmText("");
    setDeleteError(null);
    setDeleteSuccess(null);
    setDeleteWizardOpen(true);
  }

  function openTableWizard(device: PartitionDevice) {
    setTableDevice(device);
    setTableType("gpt");
    setTableConfirmText("");
    setTableError(null);
    setTableSuccess(null);
    setTableWizardOpen(true);
  }

  function deleteDependencyWarning() {
    if (!deletePartition || !partitionDevices.length) return null;
    const base = baseDiskIdentifier(deletePartition.identifier);
    const device = partitionDevices.find((dev) => baseDiskIdentifier(dev.identifier) === base);
    if (!device) return null;
    const mountedOthers = device.partitions.filter(
      (part) => part.identifier !== deletePartition.identifier && part.mount_point
    );
    if (mountedOthers.length === 0) return null;
    return "Andere Partitionen auf dem Geraet sind gemountet. Unmount empfohlen.";
  }

  function openCheckWizard(partition: PartitionEntry) {
    setCheckPartition(partition);
    setCheckRepair(false);
    setCheckError(null);
    setCheckOutput(null);
    setCheckWizardOpen(true);
  }

  async function submitCheckWizard() {
    if (!checkPartition) return;
    setCheckSubmitting(true);
    setCheckError(null);
    setCheckOutput(null);
    try {
      const result = await invoke<{ details?: { output?: string } }>("check_partition", {
        partitionIdentifier: checkPartition.identifier,
        repair: checkRepair,
      });
      const output = result?.details?.output ?? "Check abgeschlossen.";
      setCheckOutput(output);
      setCheckWizardOpen(false);
    } catch (error) {
      setCheckError(String(error));
    } finally {
      setCheckSubmitting(false);
    }
  }

  function openResizeWizard(partition: PartitionEntry) {
    setResizePartition(partition);
    setResizeUnit("gb");
    const initial = Math.max(1, Math.round((partition.size / (1024 * 1024 * 1024)) * 10) / 10);
    setResizeValue(initial);
    setResizeError(null);
    setResizeSuccess(null);
    resetPreflight();
    setResizeWizardOpen(true);
    runPreflight({
      operation: "resize",
      partitionIdentifier: partition.identifier,
      newSize: `${initial}g`,
    });
  }

  function resizeSizeString() {
    if (!resizeValue) return "0g";
    const unit = resizeUnit === "mb" ? "m" : "g";
    return `${resizeValue}${unit}`;
  }

  function resizeMaxValue(partition: PartitionEntry) {
    const base = baseDiskIdentifier(partition.identifier);
    const device = partitionDevices.find((dev) => baseDiskIdentifier(dev.identifier) === base);
    if (!device) return 0;
    const free = deviceFreeBytes(device);
    const total = partition.size + free;
    return resizeUnit === "mb" ? total / (1024 * 1024) : total / (1024 * 1024 * 1024);
  }

  async function submitResizeWizard() {
    if (!resizePartition) return;
    const preflightKey = preflightKeyFor({
      operation: "resize",
      device: resizePartition.identifier,
      newSize: resizeSizeString(),
    });
    if (!preflightReady(preflightKey)) {
      setResizeError("Bitte Safety-Preflight ausfuehren.");
      return;
    }
    if (!resizeValue || resizeValue <= 0) {
      setResizeError("Bitte eine Groesse angeben.");
      return;
    }
    const maxValue = resizeMaxValue(resizePartition);
    if (resizeValue > maxValue) {
      setResizeError("Groesse uebersteigt den maximal verfuegbaren Bereich.");
      return;
    }

    setResizeSubmitting(true);
    setResizeError(null);
    setResizeSuccess(null);
    setProgressPercent(0);
    setProgressMessage("Resize in Arbeit...");
    setProgressBytes(null);
    setProgressLog([]);
    setProgressOpen(true);
    try {
      const result = await invoke<{ details?: { output?: string } }>("resize_partition", {
        partitionIdentifier: resizePartition.identifier,
        newSize: resizeSizeString(),
      });
      const output = result?.details?.output ?? "Resize abgeschlossen.";
      setResizeOutput(output);
      setResizeSuccess("Groesse aktualisiert.");
      setResizeWizardOpen(false);
      await loadPartitionDevices();
    } catch (error) {
      setResizeError(String(error));
    } finally {
      setResizeSubmitting(false);
      setProgressOpen(false);
    }
  }

  function openMoveWizard(partition: PartitionEntry) {
    setMovePartition(partition);
    setMoveUnit("mb");
    setMoveStartValue(undefined);
    setMoveError(null);
    setMoveSuccess(null);
    setMoveBounds(null);
    resetPreflight();
    setMoveWizardOpen(true);
    loadMoveBounds(partition.identifier).catch(() => {
      setMoveBounds(null);
    });
    runPreflight({
      operation: "move",
      partitionIdentifier: partition.identifier,
    });
  }

  async function loadMoveBounds(identifier: string) {
    try {
      const result = await invoke<
        { min_start: number; max_start: number; offset: number; size: number; block_size: number }
      >("get_partition_bounds", { deviceIdentifier: identifier });
      setMoveBounds({
        minStart: result.min_start,
        maxStart: result.max_start,
        offset: result.offset,
      });
      const defaultMb = Math.round(result.offset / (1024 * 1024));
      setMoveStartValue(defaultMb);
      runPreflight({
        operation: "move",
        partitionIdentifier: identifier,
        newSize: `${defaultMb}m`,
      });
    } catch (error) {
      setMoveError(String(error));
    }
  }

  function moveStartString() {
    if (!moveStartValue) return "0m";
    const unit = moveUnit === "gb" ? "g" : "m";
    return `${moveStartValue}${unit}`;
  }

  async function submitMoveWizard() {
    if (!movePartition) return;
    const preflightKey = preflightKeyFor({
      operation: "move",
      device: movePartition.identifier,
      newSize: moveStartString(),
    });
    if (!preflightReady(preflightKey)) {
      setMoveError("Bitte Safety-Preflight ausfuehren.");
      return;
    }
    if (!moveStartValue || moveStartValue < 0) {
      setMoveError("Bitte einen Startwert angeben.");
      return;
    }

    if (moveBounds) {
      const unitFactor = moveUnit === "gb" ? 1024 * 1024 * 1024 : 1024 * 1024;
      const newStartBytes = moveStartValue * unitFactor;
      if (newStartBytes < moveBounds.minStart || newStartBytes > moveBounds.maxStart) {
        setMoveError("Ziel-Start liegt ausserhalb des zulaessigen Bereichs.");
        return;
      }
    }

    setMoveSubmitting(true);
    setMoveError(null);
    setMoveSuccess(null);
    setProgressPercent(0);
    setProgressMessage("Move in Arbeit...");
    setProgressBytes(null);
    setProgressLog([]);
    setProgressOpen(true);
    try {
      const result = await invoke<{ details?: { output?: string } }>("move_partition", {
        partitionIdentifier: movePartition.identifier,
        newStart: moveStartString(),
      });
      const output = result?.details?.output ?? "Move abgeschlossen.";
      setMoveOutput(output);
      setMoveSuccess("Partition verschoben.");
      setMoveWizardOpen(false);
      await loadPartitionDevices();
    } catch (error) {
      setMoveError(String(error));
    } finally {
      setMoveSubmitting(false);
      setProgressOpen(false);
    }
  }

  function copyToClipboard(partition: PartitionEntry) {
    setClipboardPartition(partition);
    setClipboardFs(fsTypeFromPartition(partition));
  }

  function openPasteWizard(device: PartitionDevice) {
    setPasteTargetDevice(device);
    setPasteConfirmText("");
    setPasteError(null);
    setPasteOutput(null);
    setPasteSuccess(null);
    setPasteWizardOpen(true);
  }

  async function submitPasteWizard() {
    if (!pasteTargetDevice || !clipboardPartition) return;
    if (pasteConfirmText.trim() !== pasteTargetDevice.identifier) {
      setPasteError("Bitte die Device-ID exakt eingeben.");
      return;
    }

    if (clipboardPartition.size > deviceFreeBytes(pasteTargetDevice)) {
      setPasteError("Nicht genug freier Speicher auf dem Zielgeraet.");
      return;
    }

    setPasteSubmitting(true);
    setPasteError(null);
    setProgressPercent(0);
    setProgressMessage("Copy in Arbeit...");
    setProgressBytes(null);
    setProgressLog([]);
    setProgressOpen(true);
    try {
      const result = await invoke<{ details?: { output?: string; warnings?: string[] } }>("copy_partition", {
        sourcePartition: clipboardPartition.identifier,
        targetDevice: pasteTargetDevice.identifier,
      });
      const output = result?.details?.output ?? "Copy abgeschlossen.";
      const warnings = result?.details?.warnings ?? [];
      setPasteOutput([output, ...warnings].filter(Boolean).join("\n"));
      setPasteSuccess("Partition kopiert.");
      setPasteWizardOpen(false);
      await loadPartitionDevices();
    } catch (error) {
      setPasteError(String(error));
    } finally {
      setPasteSubmitting(false);
      setProgressOpen(false);
    }
  }

  async function submitDeleteWizard() {
    if (!deletePartition) return;
    if (deleteConfirmText.trim() !== deletePartition.identifier) {
      setDeleteError("Bitte die Partition-ID exakt eingeben.");
      return;
    }

    setDeleteSubmitting(true);
    setDeleteError(null);
    setDeleteSuccess(null);
    try {
      await invoke("delete_partition", { partitionIdentifier: deletePartition.identifier });
      setDeleteSuccess("Partition geloescht.");
      setDeleteWizardOpen(false);
      await loadPartitionDevices();
    } catch (error) {
      setDeleteError(String(error));
    } finally {
      setDeleteSubmitting(false);
    }
  }

  async function submitTableWizard() {
    if (!tableDevice) return;
    if (tableConfirmText.trim() !== tableDevice.identifier) {
      setTableError("Bitte die Device-ID exakt eingeben.");
      return;
    }

    setTableSubmitting(true);
    setTableError(null);
    setTableSuccess(null);
    try {
      await invoke("create_partition_table", {
        deviceIdentifier: tableDevice.identifier,
        tableType,
      });
      setTableSuccess("Partitionstabelle erstellt.");
      setTableWizardOpen(false);
      await loadPartitionDevices();
    } catch (error) {
      setTableError(String(error));
    } finally {
      setTableSubmitting(false);
    }
  }

  function openFormatWizard(partition: PartitionEntry) {
    setSelectedPartition(partition);
    setFormatType("exfat");
    setFormatLabel(partition.name || "OXIDISK");
    setFormatError(null);
    setFormatSuccess(null);
    resetPreflight();
    setFormatWizardOpen(true);
    runPreflight({
      operation: "format",
      partitionIdentifier: partition.identifier,
      formatType: "exfat",
    });
  }

  function validateFormatInputs() {
    const label = formatLabel.trim();
    if (!label) return "Bitte ein Label angeben.";
    if (formatType === "fat32" && label.length > 11) {
      return "FAT32-Labels duerfen maximal 11 Zeichen haben.";
    }
    if (formatType === "exfat" && label.length > 15) {
      return "exFAT-Labels sollten maximal 15 Zeichen haben.";
    }
    return null;
  }

  async function submitFormatWizard() {
    if (!selectedPartition) return;
    const preflightKey = preflightKeyFor({
      operation: "format",
      device: selectedPartition.identifier,
      formatType: formatType,
    });
    if (!preflightReady(preflightKey)) {
      setFormatError("Bitte Safety-Preflight ausfuehren.");
      return;
    }
    const validation = validateFormatInputs();
    if (validation) {
      setFormatError(validation);
      return;
    }

    setFormatSubmitting(true);
    setFormatError(null);
    setFormatSuccess(null);
    try {
      await invoke("format_partition", {
        partitionIdentifier: selectedPartition.identifier,
        formatType,
        label: formatLabel.trim(),
      });

      if (supportsAutoMount(formatType)) {
        try {
          await invoke("mount_volume", { deviceIdentifier: selectedPartition.identifier });
          setFormatSuccess("Formatierung abgeschlossen. Volume wurde gemountet.");
        } catch (error) {
          setFormatSuccess("Formatierung abgeschlossen. Volume konnte nicht automatisch gemountet werden.");
        }
      } else {
        setFormatSuccess(
          "Formatierung abgeschlossen. Hinweis: Das gewaehlte Dateisystem wird von macOS nicht nativ gemountet."
        );
      }

      setFormatWizardOpen(false);
      await loadPartitionDevices();
    } catch (error) {
      setFormatError(String(error));
    } finally {
      setFormatSubmitting(false);
    }
  }

  function openLabelWizard(partition: PartitionEntry) {
    setSelectedPartition(partition);
    setLabelValue(partition.name || "");
    setUuidValue("");
    setLabelError(null);
    setLabelSuccess(null);
    setLabelWizardOpen(true);
  }

  function validateLabelInputs() {
    const fsType = fsTypeFromPartition(selectedPartition);
    const label = labelValue.trim();
    if (!label && !uuidValue.trim()) {
      return "Bitte Label oder UUID angeben.";
    }
    if (fsType === "fat32") {
      if (label.length > 11) return "FAT32-Labels duerfen maximal 11 Zeichen haben.";
      if (label && !/^[A-Z0-9 _-]+$/.test(label)) return "FAT32-Labels muessen Grossbuchstaben sein.";
    }
    if (fsType === "exfat" && label.length > 15) {
      return "exFAT-Labels sollten maximal 15 Zeichen haben.";
    }
    if ((fsType === "ext4" || fsType === "apfs") && uuidValue.trim()) {
      if (!/^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|random)$/.test(uuidValue.trim())) {
        return "UUID Format ist ungueltig.";
      }
    } else if (uuidValue.trim()) {
      return "UUID-Aenderung fuer dieses Dateisystem nicht unterstuetzt.";
    }
    return null;
  }

  async function submitLabelWizard() {
    if (!selectedPartition) return;
    const validation = validateLabelInputs();
    if (validation) {
      setLabelError(validation);
      return;
    }

    setLabelSubmitting(true);
    setLabelError(null);
    setLabelSuccess(null);
    try {
      await invoke("set_label_uuid", {
        partitionIdentifier: selectedPartition.identifier,
        label: labelValue.trim() || null,
        uuid: uuidValue.trim() || null,
      });
      setLabelSuccess("Label/UUID aktualisiert.");
      setLabelWizardOpen(false);
      await loadPartitionDevices();
    } catch (error) {
      setLabelError(String(error));
    } finally {
      setLabelSubmitting(false);
    }
  }

  function validateWipeInputs() {
    const label = wipeLabel.trim();
    if (!label) {
      return "Bitte ein Label angeben.";
    }
    if (wipeFormatType === "fat32" && label.length > 11) {
      return "FAT32-Labels duerfen maximal 11 Zeichen haben.";
    }
    if (wipeFormatType === "exfat" && label.length > 15) {
      return "exFAT-Labels sollten maximal 15 Zeichen haben.";
    }
    return null;
  }

  async function submitWipeWizard() {
    if (!selectedWipeDevice) return;
    const preflightKey = preflightKeyFor({
      operation: "wipe",
      device: selectedWipeDevice.identifier,
      formatType: wipeFormatType,
    });
    if (!preflightReady(preflightKey)) {
      setWipeError("Bitte Safety-Preflight ausfuehren.");
      return;
    }
    if (wipeConfirmText.trim() !== selectedWipeDevice.identifier) {
      setWipeError("Bitte die Device-ID exakt eingeben.");
      return;
    }

    const validation = validateWipeInputs();
    if (validation) {
      setWipeError(validation);
      return;
    }

    setWipeSubmitting(true);
    setWipeError(null);
    setWipeSuccess(null);

    try {
      await invoke("wipe_device", {
        deviceIdentifier: selectedWipeDevice.identifier,
        tableType: wipeTableType,
        formatType: wipeFormatType,
        label: wipeLabel.trim() || "OXIDISK",
      });
      if (supportsAutoMount(wipeFormatType)) {
        try {
          await invoke("mount_disk", { deviceIdentifier: selectedWipeDevice.identifier });
          setWipeSuccess("Formatierung abgeschlossen. Volume wurde gemountet.");
        } catch (error) {
          setWipeSuccess("Formatierung abgeschlossen. Volume konnte nicht automatisch gemountet werden.");
        }
      } else {
        setWipeSuccess(
          "Formatierung abgeschlossen. Hinweis: Das gewaehlte Dateisystem wird von macOS nicht nativ gemountet."
        );
      }
      setWipeWizardOpen(false);
      await loadPartitionDevices();
    } catch (error) {
      setWipeError(String(error));
    } finally {
      setWipeSubmitting(false);
    }
  }

  async function installSudoers() {
    setSudoSetupLoading(true);
    setSudoSetupError(null);
    setSudoSetupMessage(null);
    try {
      const result = await invoke<{ message?: string }>("install_sudoers_helper");
      setSudoSetupMessage(result?.message ?? "Sudoers installiert.");
    } catch (error) {
      setSudoSetupError(String(error));
    } finally {
      setSudoSetupLoading(false);
    }
  }

  async function loadSidecarStatus() {
    setSidecarLoading(true);
    try {
      const result = await invoke<
        { name: string; found: boolean; path?: string | null; version?: string | null }[]
      >("get_sidecar_status");
      setSidecarStatus(result);
    } catch (error) {
      setSidecarStatus([]);
    } finally {
      setSidecarLoading(false);
    }
  }

  async function startScan(disk: SystemDisk) {
    if (loading) return;
    setLoading(true);
    setCurrentDisk(disk);
    setCurrentRootName(disk.name);
    setCurrentRootPath(disk.mount_point);

    // Breadcrumbs initialisieren
    setPathParts([disk.mount_point]);
    setScanData(null);
    setSelectedNode(null);

    try {
      const data = await invoke<FileNode>("scan_directory", { path: disk.mount_point });
      setScanData(data);
    } catch (error) {
      console.error("Scan Fehler:", error);
    } finally {
      setLoading(false);
    }
  }

  async function startScanFolder(path: string) {
    if (loading) return;
    setLoading(true);
    setCurrentDisk(null);
    setCurrentRootPath(path);
    const name = path.split("/").filter(Boolean).pop() || path;
    setCurrentRootName(name);
    setPathParts([path]);
    setScanData(null);
    setSelectedNode(null);

    try {
      const data = await invoke<FileNode>("scan_directory", { path });
      setScanData(data);
    } catch (error) {
      console.error("Scan Fehler:", error);
    } finally {
      setLoading(false);
    }
  }

  async function chooseFolder() {
    try {
      const selected = await openDialog({ directory: true, multiple: false, title: "Ordner wählen" });
      if (typeof selected === "string") {
        await startScanFolder(selected);
      }
    } catch (error) {
      console.error(error);
    }
  }

  useEffect(() => {
    if (activeView === "analyzer") {
      loadDisks();
    }
  }, [showSystemVolumes, activeView]);

  useEffect(() => {
    if (activeView === "partition" || activeView === "images") {
      loadPartitionDevices();
      loadOperationJournal();
    }
  }, [activeView]);

  useEffect(() => {
    if (partitionDevices.length === 0) {
      setSelectedPartitionDeviceId(null);
      setSelectedPartitionId(null);
      setSelectedPartition(null);
      setSelectedUnallocated(null);
      return;
    }

    if (!selectedPartitionDeviceId || !partitionDevices.some((dev) => dev.identifier === selectedPartitionDeviceId)) {
      const initial = partitionDevices[0];
      setSelectedPartitionDeviceId(initial.identifier);
      setSelectedPartitionId(initial.partitions[0]?.identifier ?? null);
      setSelectedPartition(initial.partitions[0] ?? null);
      setSelectedUnallocated(null);
    }
  }, [partitionDevices, selectedPartitionDeviceId]);

  useEffect(() => {
    if (!selectedPartitionDeviceId) return;
    const device = partitionDevices.find((dev) => dev.identifier === selectedPartitionDeviceId);
    if (!device) return;
    if (selectedPartitionId && device.partitions.some((part) => part.identifier === selectedPartitionId)) {
      setSelectedPartition(device.partitions.find((part) => part.identifier === selectedPartitionId) ?? null);
      return;
    }
    setSelectedPartitionId(device.partitions[0]?.identifier ?? null);
    setSelectedPartition(device.partitions[0] ?? null);
    setSelectedUnallocated(null);
  }, [partitionDevices, selectedPartitionDeviceId, selectedPartitionId]);

  useEffect(() => {
    if (imageMode !== "windows") {
      setImageWindowsSheetConfirmed(false);
    }
  }, [imageMode]);

  useEffect(() => {
    const node = partitionBarRef.current;
    if (!node) return;
    const update = () => setPartitionBarWidth(node.clientWidth);
    update();

    const observer = new ResizeObserver(() => update());
    observer.observe(node);
    return () => observer.disconnect();
  }, [selectedPartitionDeviceId]);

  useEffect(() => {
    let unlisten: (() => void) | null = null;
    listen<any>("partition-operation-progress", (event) => {
      const payload = event.payload as {
        percent?: number;
        message?: string;
        phase?: string;
        bytes?: number;
        totalBytes?: number;
      };
      setProgressPercent(payload.percent ?? 0);
      setProgressMessage(payload.message ?? payload.phase ?? null);
      if (payload.bytes !== undefined && payload.totalBytes !== undefined) {
        setProgressBytes({ current: payload.bytes, total: payload.totalBytes });
        const now = Date.now();
        const last = lastProgressRef.current;
        if (last && payload.bytes >= last.bytes) {
          const deltaBytes = payload.bytes - last.bytes;
          const deltaTime = Math.max(0.001, (now - last.time) / 1000);
          const speed = deltaBytes / deltaTime;
          setProgressSpeed(speed);
          if (payload.totalBytes > 0 && speed > 0) {
            const remaining = payload.totalBytes - payload.bytes;
            setProgressEta(remaining / speed);
          } else {
            setProgressEta(null);
          }
        }
        lastProgressRef.current = { time: now, bytes: payload.bytes };
      }
      if (imageRunningRef.current) {
        setProgressOpen(false);
      } else {
        setProgressOpen(true);
      }
    }).then((fn) => {
      unlisten = fn;
    });
    return () => {
      if (unlisten) {
        unlisten();
      }
    };
  }, []);

  useEffect(() => {
    let unlisten: (() => void) | null = null;
    listen<any>("partition-operation-log", (event) => {
      const payload = event.payload as { line?: string; source?: string };
      if (!payload.line) return;
      setProgressLog((prev) => {
        const next = [...prev, payload.source ? `[${payload.source}] ${payload.line}` : payload.line];
        return next.slice(-200);
      });
    }).then((fn) => {
      unlisten = fn;
    });
    return () => {
      if (unlisten) {
        unlisten();
      }
    };
  }, []);

  // Berechne Nutzung für das Dashboard
  const usagePercent = currentDisk
    ? ((currentDisk.total_space - currentDisk.available_space) / currentDisk.total_space) * 100
    : 0;

  async function showInFinder(node: FileNode) {
    try {
      await invoke("open_in_finder", { path: node.path });
    } catch (error) {
      console.error(error);
    }
  }

  async function moveToTrash(node: FileNode) {
    try {
      await invoke("move_to_trash", { path: node.path });
      closeConfirm();
      setTrashTarget(null);
    } catch (error) {
      console.error(error);
    }
  }

  const usagePercentLabel = `${Math.round(usagePercent)}%`;

  function renderPreflightBlock(params: {
    operation: string;
    deviceIdentifier?: string;
    partitionIdentifier?: string;
    formatType?: string;
    newSize?: string;
  }) {
    const key = preflightKeyFor({
      operation: params.operation,
      device: params.partitionIdentifier ?? params.deviceIdentifier ?? "",
      formatType: params.formatType ?? "",
      newSize: params.newSize ?? "",
    });
    const result = preflightKey === key ? preflightResult : null;
    const blockers = result?.blockers ?? [];
    const warnings = result?.warnings ?? [];

    return (
      <Stack gap="xs">
        <Group justify="space-between">
          <Text size="xs" c="dimmed">
            Safety-Preflight
          </Text>
          <Button
            size="xs"
            variant="light"
            onClick={() => runPreflight(params)}
            loading={preflightRunning}
          >
            Preflight starten
          </Button>
        </Group>
        {preflightError && (
          <Text size="xs" c="red">
            {preflightError}
          </Text>
        )}
        {preflightKey && preflightKey !== key && (
          <Text size="xs" c="dimmed">
            Preflight veraltet. Bitte neu pruefen.
          </Text>
        )}
        {result?.battery && result.battery.isLaptop && !result.battery.onAc && (
          <Text size="xs" c="red">
            Akku: {result.battery.percent ?? "?"}% - Netzteil empfohlen.
          </Text>
        )}
        {blockers.map((item, index) => (
          <Text key={`blocker-${index}`} size="xs" c="red">
            {item}
          </Text>
        ))}
        {result?.busyProcesses && result.busyProcesses.length > 0 && (
          <Stack gap={4}>
            {result.busyProcesses.slice(0, 6).map((proc) => (
              <Text key={`${proc.pid}-${proc.command}`} size="xs" c="red">
                {proc.command} (PID {proc.pid})
              </Text>
            ))}
            <Button
              size="xs"
              color="red"
              variant="light"
              onClick={async () => {
                await forceUnmount({
                  deviceIdentifier: params.deviceIdentifier,
                  partitionIdentifier: params.partitionIdentifier,
                });
                await runPreflight(params);
              }}
            >
              Prozesse beenden & Unmount erzwingen
            </Button>
          </Stack>
        )}
        {warnings.map((item, index) => (
          <Text key={`warn-${index}`} size="xs" c="yellow">
            {item}
          </Text>
        ))}
        {result?.sidecars && result.sidecars.length > 0 && (
          <Stack gap={4}>
            {result.sidecars.map((sidecar) => (
              <Text key={sidecar.name} size="xs" c={sidecar.found ? "dimmed" : "red"}>
                {sidecar.found ? "OK" : "Fehlt"}: {sidecar.name}
              </Text>
            ))}
          </Stack>
        )}
      </Stack>
    );
  }

  const selectedPartitionDevice = selectedPartitionDeviceId
    ? partitionDevices.find((device) => device.identifier === selectedPartitionDeviceId) ?? null
    : null;
  const partitionSegments = buildPartitionSegments(selectedPartitionDevice);

  return (
    <AppShell
      header={{ height: 60 }}
      navbar={{ width: 260, breakpoint: "sm", collapsed: { mobile: !opened } }}
      padding="md"
    >
      <Modal opened={confirmOpen} onClose={closeConfirm} title="In Papierkorb verschieben?" centered>
        <Text size="sm">Diese Aktion verschiebt die Datei bzw. den Ordner in den Papierkorb.</Text>
        <Group justify="flex-end" mt="md">
          <Button variant="default" onClick={closeConfirm}>
            Abbrechen
          </Button>
          <Button color="red" onClick={() => trashTarget && moveToTrash(trashTarget)}>
            In Papierkorb
          </Button>
        </Group>
      </Modal>
      <Modal
        opened={wipeWizardOpen}
        onClose={() => setWipeWizardOpen(false)}
        title="Geraet loeschen und neu formatieren"
        centered
      >
        <Stack gap="sm">
          <Text size="sm" c="dimmed">
            Diese Aktion loescht alle Daten und erstellt eine neue Partitionstabelle.
          </Text>
          <Text size="sm">
            Geraet: <b>{selectedWipeDevice?.identifier ?? "-"}</b>
          </Text>
          <NativeSelect
            label="Partitionstabelle"
            value={wipeTableType}
            onChange={(event) => setWipeTableType(event.currentTarget.value)}
            data={[
              { value: "gpt", label: "GPT (Standard)" },
              { value: "mbr", label: "MBR" },
            ]}
          />
          <NativeSelect
            label="Dateisystem"
            value={wipeFormatType}
            onChange={(event) => {
              resetPreflight();
              setWipeFormatType(event.currentTarget.value);
            }}
            data={[
              { value: "exfat", label: "exFAT" },
              { value: "fat32", label: "FAT32" },
              { value: "apfs", label: "APFS" },
              { value: "ext4", label: "EXT4 (Linux)" },
              { value: "ntfs", label: "NTFS" },
              { value: "btrfs", label: "Btrfs (Linux)" },
              { value: "xfs", label: "XFS (Linux)" },
              { value: "f2fs", label: "F2FS (Linux)" },
              { value: "swap", label: "Linux Swap" },
            ]}
          />
          {isExoticFs(wipeFormatType) && (
            <Badge color="yellow" variant="light">
              Dieses Dateisystem wird unter macOS als "unformatiert" oder "unbekannt" angezeigt werden.
            </Badge>
          )}
          {!isMacMountable(wipeFormatType) && (
            <Text size="xs" c="dimmed">
              Hinweis: Dieses Dateisystem wird von macOS nicht nativ gemountet.
            </Text>
          )}
          <TextInput
            label="Label"
            value={wipeLabel}
            onChange={(event) => setWipeLabel(event.currentTarget.value)}
            placeholder="OXIDISK"
          />
          {selectedWipeDevice &&
            renderPreflightBlock({
              operation: "wipe",
              deviceIdentifier: selectedWipeDevice.identifier,
              formatType: wipeFormatType,
            })}
          <TextInput
            label="Device-ID bestaetigen"
            value={wipeConfirmText}
            onChange={(event) => setWipeConfirmText(event.currentTarget.value)}
            placeholder={selectedWipeDevice?.identifier ?? "diskX"}
            description="Gib die Device-ID exakt ein, um fortzufahren."
          />
          {wipeError && (
            <Text size="sm" c="red">
              {wipeError}
            </Text>
          )}
          <Group justify="flex-end" mt="md">
            <Button variant="default" onClick={() => setWipeWizardOpen(false)} disabled={wipeSubmitting}>
              Abbrechen
            </Button>
            <Button
              color="red"
              onClick={submitWipeWizard}
              loading={wipeSubmitting}
              disabled={!preflightReady(
                preflightKeyFor({
                  operation: "wipe",
                  device: selectedWipeDevice?.identifier ?? "",
                  formatType: wipeFormatType,
                })
              )}
            >
              Geraet loeschen
            </Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={!!wipeSuccess} onClose={() => setWipeSuccess(null)} title="Aktion abgeschlossen" centered>
        <Stack gap="sm">
          <Text size="sm">{wipeSuccess}</Text>
          <Group justify="flex-end" mt="md">
            <Button onClick={() => setWipeSuccess(null)}>OK</Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={!!imageSuccess} onClose={() => setImageSuccess(null)} title="Flash abgeschlossen" centered>
        <Stack gap="sm">
          <Text size="sm">{imageSuccess}</Text>
          {imageResultMount && (
            <Button
              variant="light"
              onClick={() => invoke("open_in_finder", { path: imageResultMount }).catch(() => {})}
            >
              Im Finder anzeigen
            </Button>
          )}
          <Group justify="flex-end" mt="md">
            <Button onClick={() => setImageSuccess(null)}>OK</Button>
          </Group>
        </Stack>
      </Modal>
      <Modal
        opened={imageWindowsSheetOpen}
        onClose={() => setImageWindowsSheetOpen(false)}
        title="Windows Setup anpassen"
        centered
      >
        <Stack gap="sm">
          <Text size="sm" c="dimmed">
            Diese Einstellungen erzeugen eine autounattend.xml auf dem Stick.
          </Text>
          <Switch
            label="TPM 2.0 / Secure Boot umgehen"
            checked={imageWinTpmBypass}
            onChange={(event) => setImageWinTpmBypass(event.currentTarget.checked)}
          />
          <Switch
            label="Lokalen Account erzwingen"
            checked={imageWinLocalAccount}
            onChange={(event) => setImageWinLocalAccount(event.currentTarget.checked)}
          />
          <Switch
            label="Datenschutz-Einstellungen deaktivieren"
            checked={imageWinPrivacyDefaults}
            onChange={(event) => setImageWinPrivacyDefaults(event.currentTarget.checked)}
          />
          <Group justify="flex-end" mt="md">
            <Button variant="default" onClick={() => setImageWindowsSheetOpen(false)}>
              Abbrechen
            </Button>
            <Button
              onClick={async () => {
                setImageWindowsSheetOpen(false);
                setImageWindowsSheetConfirmed(true);
                await submitWindowsInstall();
              }}
            >
              Fortfahren
            </Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={sudoSetupOpen} onClose={() => setSudoSetupOpen(false)} title="Helper einrichten" centered>
        <Stack gap="sm">
          <Text size="sm" c="dimmed">
            Oxidisk richtet einen sudoers-Eintrag ein, damit der Helper ohne Passwortprompt laeuft.
            Du wirst einmalig nach dem Admin-Passwort gefragt.
          </Text>
          {sudoSetupMessage && <Text size="sm">{sudoSetupMessage}</Text>}
          {sudoSetupError && (
            <Text size="sm" c="red">
              {sudoSetupError}
            </Text>
          )}
          <Group justify="flex-end" mt="md">
            <Button variant="default" onClick={() => setSudoSetupOpen(false)} disabled={sudoSetupLoading}>
              Schliessen
            </Button>
            <Button onClick={installSudoers} loading={sudoSetupLoading}>
              Setup starten
            </Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={formatWizardOpen} onClose={() => setFormatWizardOpen(false)} title="Partition formatieren" centered>
        <Stack gap="sm">
          <Text size="sm" c="dimmed">
            Partition: <b>{selectedPartition?.identifier ?? "-"}</b>
          </Text>
          <NativeSelect
            label="Dateisystem"
            value={formatType}
            onChange={(event) => {
              resetPreflight();
              setFormatType(event.currentTarget.value);
            }}
            data={[
              { value: "exfat", label: "exFAT" },
              { value: "fat32", label: "FAT32" },
              { value: "apfs", label: "APFS" },
              { value: "ext4", label: "EXT4 (Linux)" },
              { value: "ntfs", label: "NTFS" },
              { value: "btrfs", label: "Btrfs (Linux)" },
              { value: "xfs", label: "XFS (Linux)" },
              { value: "f2fs", label: "F2FS (Linux)" },
              { value: "swap", label: "Linux Swap" },
            ]}
          />
          {isExoticFs(formatType) && (
            <Badge color="yellow" variant="light">
              Dieses Dateisystem wird unter macOS als "unformatiert" oder "unbekannt" angezeigt werden.
            </Badge>
          )}
          {!isMacMountable(formatType) && (
            <Text size="xs" c="dimmed">
              Hinweis: Dieses Dateisystem wird von macOS nicht nativ gemountet.
            </Text>
          )}
          <TextInput
            label="Label"
            value={formatLabel}
            onChange={(event) => setFormatLabel(event.currentTarget.value)}
            placeholder="OXIDISK"
          />
          {formatError && (
            <Text size="sm" c="red">
              {formatError}
            </Text>
          )}
          <Group justify="flex-end" mt="md">
            <Button variant="default" onClick={() => setFormatWizardOpen(false)} disabled={formatSubmitting}>
              Abbrechen
            </Button>
            <Button color="red" onClick={submitFormatWizard} loading={formatSubmitting}>
              Formatieren
            </Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={apfsManagerOpen} onClose={() => setApfsManagerOpen(false)} title="APFS Volumes" centered size="lg">
        <Stack gap="sm">
          <Text size="sm" c="dimmed">
            Container: <b>{apfsTarget?.identifier ?? "-"}</b>
          </Text>
          {apfsLoading && <Text size="sm" c="dimmed">Lade APFS-Container…</Text>}
          {apfsError && (
            <Text size="sm" c="red">
              {apfsError}
            </Text>
          )}
          {apfsContainer && (() => {
            const capacity = apfsContainer.capacity ?? 0;
            const used =
              apfsContainer.capacityUsed ??
              (apfsContainer.capacity != null && apfsContainer.capacityFree != null
                ? apfsContainer.capacity - apfsContainer.capacityFree
                : 0);
            if (!capacity) return null;
            const percent = Math.min(100, Math.round((used / capacity) * 100));
            return (
              <Stack gap={4}>
                <Text size="xs" c="dimmed">
                  Belegt: {formatBytes(used)} von {formatBytes(capacity)}
                </Text>
                <Slider value={percent} min={0} max={100} step={1} disabled />
              </Stack>
            );
          })()}
          <Divider />
          {apfsContainer && apfsContainer.volumes.length === 0 && (
            <Text size="sm" c="dimmed">
              Keine Volumes gefunden.
            </Text>
          )}
          {apfsContainer && apfsContainer.volumes.length > 0 && (
            <Stack gap="xs">
              {apfsContainer.volumes.map((volume) => (
                <Group key={volume.identifier} justify="space-between" align="center">
                  <div>
                    <Text size="sm" fw={600}>
                      {volume.name || volume.identifier}
                    </Text>
                    <Text size="xs" c="dimmed">
                      {volume.identifier}
                      {volume.mountPoint ? ` · ${volume.mountPoint}` : ""}
                    </Text>
                    {volume.roles.length > 0 && (
                      <Group gap="xs" mt={4}>
                        {volume.roles.map((role) => (
                          <Badge key={`${volume.identifier}-${role}`} variant="light">
                            {role}
                          </Badge>
                        ))}
                      </Group>
                    )}
                  </div>
                  <Group gap="xs">
                    <Text size="xs" c="dimmed">
                      {formatBytes(volume.used || volume.size)}
                    </Text>
                    <Button
                      size="xs"
                      color="red"
                      variant="light"
                      loading={apfsDeleteBusy === volume.identifier}
                      disabled={apfsVolumeProtected(volume)}
                      onClick={() => submitApfsDeleteVolume(volume)}
                    >
                      Loeschen
                    </Button>
                  </Group>
                </Group>
              ))}
            </Stack>
          )}
          <Divider />
          <Text fw={600} size="sm">
            Neues Volume
          </Text>
          <TextInput
            label="Name"
            value={apfsAddName}
            onChange={(event) => setApfsAddName(event.currentTarget.value)}
            placeholder="OXIDISK"
          />
          {showPowerDataInspector && (
            <NativeSelect
              label="Role (optional)"
              value={apfsAddRole}
              onChange={(event) => setApfsAddRole(event.currentTarget.value)}
              data={apfsRoleOptions.map((role) => ({ value: role, label: role }))}
            />
          )}
          {apfsAddError && (
            <Text size="sm" c="red">
              {apfsAddError}
            </Text>
          )}
          <Group justify="flex-end" mt="sm">
            <Button variant="default" onClick={() => setApfsManagerOpen(false)} disabled={apfsAddSubmitting}>
              Schliessen
            </Button>
            <Button onClick={submitApfsAddVolume} loading={apfsAddSubmitting}>
              Volume erstellen
            </Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={labelWizardOpen} onClose={() => setLabelWizardOpen(false)} title="Label/UUID" centered>
        <Stack gap="sm">
          <Text size="sm" c="dimmed">
            Partition: <b>{selectedPartition?.identifier ?? "-"}</b>
          </Text>
          {(() => {
            const fsType = fsTypeFromPartition(selectedPartition);
            if (fsType === "fat32") {
              return (
                <Text size="xs" c="dimmed">
                  FAT32: Nur Labels, Grossbuchstaben und max. 11 Zeichen.
                </Text>
              );
            }
            if (fsType === "ntfs") {
              return <Text size="xs" c="dimmed">NTFS: UUID-Aenderung nicht unterstuetzt.</Text>;
            }
            if (fsType === "swap") {
              return <Text size="xs" c="dimmed">Linux Swap: UUID-Aenderung nicht unterstuetzt.</Text>;
            }
            if (fsType === "ext4") {
              return <Text size="xs" c="dimmed">EXT4: UUID kann gesetzt oder als "random" generiert werden.</Text>;
            }
            if (fsType === "apfs") {
              return <Text size="xs" c="dimmed">APFS: UUID kann gesetzt werden.</Text>;
            }
            return <Text size="xs" c="dimmed">Dateisystem wird automatisch erkannt.</Text>;
          })()}
          <TextInput
            label="Label"
            value={labelValue}
            onChange={(event) => setLabelValue(event.currentTarget.value)}
            placeholder="OXIDISK"
          />
          {(() => {
            const fsType = fsTypeFromPartition(selectedPartition);
            if (fsType === "fat32" || fsType === "ntfs" || fsType === "exfat" || fsType === "swap") {
              return null;
            }
            return (
              <Group gap="xs" align="flex-end">
                <TextInput
                  label="UUID"
                  value={uuidValue}
                  onChange={(event) => setUuidValue(event.currentTarget.value)}
                  placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
                  style={{ flex: 1 }}
                />
                <Button
                  variant="light"
                  onClick={() => setUuidValue("random")}
                >
                  Random
                </Button>
              </Group>
            );
          })()}
          {labelError && (
            <Text size="sm" c="red">
              {labelError}
            </Text>
          )}
          <Group justify="flex-end" mt="md">
            <Button variant="default" onClick={() => setLabelWizardOpen(false)} disabled={labelSubmitting}>
              Abbrechen
            </Button>
            <Button onClick={submitLabelWizard} loading={labelSubmitting}>
              Speichern
            </Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={createWizardOpen} onClose={() => setCreateWizardOpen(false)} title="Partition erstellen" centered>
        <Stack gap="sm">
          <Text size="sm" c="dimmed">
            Geraet: <b>{createDevice?.identifier ?? "-"}</b>
          </Text>
          {createDevice && (
            <Text size="sm" c="dimmed">
              Verfuegbar: {formatBytes(deviceFreeBytes(createDevice))}
            </Text>
          )}
          {createDevice && deviceHasApfs(createDevice) && (
            <Text size="xs" c="dimmed">
              Hinweis: APFS-Container erkannt. Freier Platz kann dynamisch zwischen Volumes geteilt werden.
            </Text>
          )}
          <Text size="xs" c="dimmed">
            Partition wird an 1-MiB-Grenzen ausgerichtet (Optimiert fuer SSD-Performance).
          </Text>
          <NativeSelect
            label="Dateisystem"
            value={createFormatType}
            onChange={(event) => {
              resetPreflight();
              setCreateFormatType(event.currentTarget.value);
            }}
            data={[
              { value: "exfat", label: "exFAT" },
              { value: "fat32", label: "FAT32" },
              { value: "apfs", label: "APFS" },
              { value: "ext4", label: "EXT4 (Linux)" },
              { value: "ntfs", label: "NTFS" },
              { value: "btrfs", label: "Btrfs (Linux)" },
              { value: "xfs", label: "XFS (Linux)" },
              { value: "f2fs", label: "F2FS (Linux)" },
              { value: "swap", label: "Linux Swap" },
            ]}
          />
          {isExoticFs(createFormatType) && (
            <Badge color="yellow" variant="light">
              Dieses Dateisystem wird unter macOS als "unformatiert" oder "unbekannt" angezeigt werden.
            </Badge>
          )}
          {!isMacMountable(createFormatType) && (
            <Text size="xs" c="dimmed">
              Hinweis: Dieses Dateisystem wird von macOS nicht nativ gemountet.
            </Text>
          )}
          <TextInput
            label="Label"
            value={createLabel}
            onChange={(event) =>
              setCreateLabel(normalizeLabelInput(event.currentTarget.value, createFormatType))
            }
            maxLength={labelMaxLength(createFormatType)}
            placeholder="OXIDISK"
          />
          {createDevice &&
            renderPreflightBlock({
              operation: "create",
              deviceIdentifier: createDevice.identifier,
              formatType: createFormatType,
            })}
          <Group gap="xs" align="flex-end">
            <NumberInput
              label="Groesse"
              value={createSizeValue}
              onChange={(value) => setCreateSizeValue(typeof value === "number" ? value : undefined)}
              min={0}
              step={0.1}
              style={{ flex: 1 }}
            />
            <NativeSelect
              label="Einheit"
              value={createSizeUnit}
              onChange={(event) => setCreateSizeUnit(event.currentTarget.value)}
              data={[
                { value: "gb", label: "GB" },
                { value: "mb", label: "MB" },
              ]}
            />
          </Group>
          {(() => {
            if (!createDevice) return null;
            const free = deviceFreeBytes(createDevice);
            const freeGb = free / (1024 * 1024 * 1024);
            const maxValue = createSizeUnit === "mb" ? Math.max(0.1, free / (1024 * 1024)) : Math.max(0.1, freeGb);
            const sliderValue = createSizeValue ?? 0;
            return (
              <Slider
                value={sliderValue}
                min={0}
                max={Number.isFinite(maxValue) ? maxValue : 0}
                step={0.1}
                onChange={(value) => setCreateSizeValue(value)}
              />
            );
          })()}
          {createError && (
            <Text size="sm" c="red">
              {createError}
            </Text>
          )}
          <Group justify="flex-end" mt="md">
            <Button variant="default" onClick={() => setCreateWizardOpen(false)} disabled={createSubmitting}>
              Abbrechen
            </Button>
            <Button
              onClick={submitCreateWizard}
              loading={createSubmitting}
              disabled={!preflightReady(
                preflightKeyFor({
                  operation: "create",
                  device: createDevice?.identifier ?? "",
                  formatType: createFormatType,
                })
              )}
            >
              Partition erstellen
            </Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={deleteWizardOpen} onClose={() => setDeleteWizardOpen(false)} title="Partition loeschen" centered>
        <Stack gap="sm">
          <Text size="sm" c="dimmed">
            Partition: <b>{deletePartition?.identifier ?? "-"}</b>
          </Text>
          {deleteDependencyWarning() && (
            <Text size="sm" c="yellow">
              {deleteDependencyWarning()}
            </Text>
          )}
          <TextInput
            label="Partition-ID bestaetigen"
            value={deleteConfirmText}
            onChange={(event) => setDeleteConfirmText(event.currentTarget.value)}
            placeholder={deletePartition?.identifier ?? "diskXsY"}
            description="Gib die Partition-ID exakt ein, um fortzufahren."
          />
          {deleteError && (
            <Text size="sm" c="red">
              {deleteError}
            </Text>
          )}
          <Group justify="flex-end" mt="md">
            <Button variant="default" onClick={() => setDeleteWizardOpen(false)} disabled={deleteSubmitting}>
              Abbrechen
            </Button>
            <Button color="red" onClick={submitDeleteWizard} loading={deleteSubmitting}>
              Loeschen
            </Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={checkWizardOpen} onClose={() => setCheckWizardOpen(false)} title="Partition ueberpruefen" centered>
        <Stack gap="sm">
          <Text size="sm" c="dimmed">
            Partition: <b>{checkPartition?.identifier ?? "-"}</b>
          </Text>
          <Switch
            label="Reparaturversuch (falls moeglich)"
            checked={checkRepair}
            onChange={(event) => setCheckRepair(event.currentTarget.checked)}
          />
          {checkError && (
            <Text size="sm" c="red">
              {checkError}
            </Text>
          )}
          <Group justify="flex-end" mt="md">
            <Button variant="default" onClick={() => setCheckWizardOpen(false)} disabled={checkSubmitting}>
              Abbrechen
            </Button>
            <Button onClick={submitCheckWizard} loading={checkSubmitting}>
              Ueberpruefen
            </Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={!!checkOutput} onClose={() => setCheckOutput(null)} title="Check Output" centered>
        <Stack gap="sm">
          <Text size="xs" style={{ whiteSpace: "pre-wrap", fontFamily: "ui-monospace" }}>
            {checkOutput}
          </Text>
          <Group justify="flex-end" mt="md">
            <Button onClick={() => setCheckOutput(null)}>OK</Button>
          </Group>
        </Stack>
      </Modal>
      <Modal
        opened={journalOpen}
        onClose={() => {
          setJournalOpen(false);
          setJournalInfo(null);
        }}
        title="Unvollstaendige Operation gefunden"
        centered
      >
        <Stack gap="sm">
          <Text size="sm">
            Oxidisk hat ein Journal gefunden. Eine vorherige Move-Operation wurde moeglicherweise
            unterbrochen.
          </Text>
          {journalInfo && (
            <Text size="xs" c="dimmed">
              Device: {journalInfo.device} • Letzter Block: {journalInfo.lastCopied ?? 0} Bytes
            </Text>
          )}
          <Group justify="flex-end" mt="md">
            <Button
              variant="default"
              onClick={async () => {
                await clearOperationJournal();
                setJournalOpen(false);
                setJournalInfo(null);
              }}
            >
              Ignorieren
            </Button>
            <Button onClick={handleJournalRepair}>Reparieren</Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={resizeWizardOpen} onClose={() => setResizeWizardOpen(false)} title="Groesse aendern" centered>
        <Stack gap="sm">
          <Text size="sm" c="dimmed">
            Partition: <b>{resizePartition?.identifier ?? "-"}</b>
          </Text>
          {resizePartition && ["ext4", "ntfs"].includes(fsTypeFromPartition(resizePartition)) && (
            <Text size="xs" c="red">
              Experimental: Resize fuer EXT4/NTFS nutzt GPT-Rewrite und kann riskant sein.
            </Text>
          )}
          <Text size="xs" c="dimmed">
            Partition wird an 1-MiB-Grenzen ausgerichtet (Optimiert fuer SSD-Performance).
          </Text>
          {resizePartition && (
            <Text size="sm" c="dimmed">
              Aktuell: {formatBytes(resizePartition.size)}
            </Text>
          )}
          <Group gap="xs" align="flex-end">
            <NumberInput
              label="Neue Groesse"
              value={resizeValue}
              onChange={(value) => {
                resetPreflight();
                setResizeValue(typeof value === "number" ? value : undefined);
              }}
              min={0}
              step={0.1}
              style={{ flex: 1 }}
            />
            <NativeSelect
              label="Einheit"
              value={resizeUnit}
              onChange={(event) => {
                resetPreflight();
                setResizeUnit(event.currentTarget.value);
              }}
              data={[
                { value: "gb", label: "GB" },
                { value: "mb", label: "MB" },
              ]}
            />
          </Group>
          {resizePartition && (
            <Slider
              value={resizeValue ?? 0}
              min={0}
              max={Math.max(0, resizeMaxValue(resizePartition))}
              step={0.1}
              onChange={(value) => {
                resetPreflight();
                setResizeValue(value);
              }}
            />
          )}
          {resizePartition &&
            renderPreflightBlock({
              operation: "resize",
              partitionIdentifier: resizePartition.identifier,
              newSize: resizeSizeString(),
            })}
          {resizeError && (
            <Text size="sm" c="red">
              {resizeError}
            </Text>
          )}
          <Group justify="flex-end" mt="md">
            <Button variant="default" onClick={() => setResizeWizardOpen(false)} disabled={resizeSubmitting}>
              Abbrechen
            </Button>
            <Button
              onClick={submitResizeWizard}
              loading={resizeSubmitting}
              disabled={!preflightReady(
                preflightKeyFor({
                  operation: "resize",
                  device: resizePartition?.identifier ?? "",
                  newSize: resizeSizeString(),
                })
              )}
            >
              Groesse aendern
            </Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={moveWizardOpen} onClose={() => setMoveWizardOpen(false)} title="Partition verschieben (Experimental)" centered>
        <Stack gap="sm">
          <Text size="sm" c="red">
            Warnung: Ein Move ist riskant. Ein Stromausfall kann zu Datenverlust fuehren.
          </Text>
          <Text size="sm" c="dimmed">
            Partition: <b>{movePartition?.identifier ?? "-"}</b>
          </Text>
          <Text size="xs" c="dimmed">
            Ziel-Startoffset (ab Anfang der Disk). Werte werden an 1-MiB-Grenzen ausgerichtet.
          </Text>
          {moveBounds && (
            <Text size="xs" c="dimmed">
              Erlaubter Bereich: {Math.round(moveBounds.minStart / (1024 * 1024))} MB bis {Math.round(moveBounds.maxStart / (1024 * 1024))} MB
            </Text>
          )}
          <Group gap="xs" align="flex-end">
            <NumberInput
              label="Start"
              value={moveStartValue}
              onChange={(value) => {
                resetPreflight();
                setMoveStartValue(typeof value === "number" ? value : undefined);
              }}
              min={0}
              step={1}
              style={{ flex: 1 }}
            />
            <NativeSelect
              label="Einheit"
              value={moveUnit}
              onChange={(event) => {
                resetPreflight();
                setMoveUnit(event.currentTarget.value);
              }}
              data={[
                { value: "mb", label: "MB" },
                { value: "gb", label: "GB" },
              ]}
            />
          </Group>
          {moveError && (
            <Text size="sm" c="red">
              {moveError}
            </Text>
          )}
          {movePartition &&
            renderPreflightBlock({
              operation: "move",
              partitionIdentifier: movePartition.identifier,
              newSize: moveStartString(),
            })}
          <Group justify="flex-end" mt="md">
            <Button variant="default" onClick={() => setMoveWizardOpen(false)} disabled={moveSubmitting}>
              Abbrechen
            </Button>
            <Button
              color="red"
              onClick={submitMoveWizard}
              loading={moveSubmitting}
              disabled={!preflightReady(
                preflightKeyFor({
                  operation: "move",
                  device: movePartition?.identifier ?? "",
                  newSize: moveStartString(),
                })
              )}
            >
              Verschieben
            </Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={tableWizardOpen} onClose={() => setTableWizardOpen(false)} title="Partitionstabelle erstellen" centered>
        <Stack gap="sm">
          <Text size="sm" c="dimmed">
            Geraet: <b>{tableDevice?.identifier ?? "-"}</b>
          </Text>
          <Text size="sm" c="red">
            Warnung: Alle Daten auf dem Geraet werden geloescht.
          </Text>
          <NativeSelect
            label="Partitionstabelle"
            value={tableType}
            onChange={(event) => setTableType(event.currentTarget.value)}
            data={[
              { value: "gpt", label: "GPT (Standard)" },
              { value: "mbr", label: "MBR (Legacy)" },
            ]}
          />
          <TextInput
            label="Device-ID bestaetigen"
            value={tableConfirmText}
            onChange={(event) => setTableConfirmText(event.currentTarget.value)}
            placeholder={tableDevice?.identifier ?? "diskX"}
            description="Gib die Device-ID exakt ein, um fortzufahren."
          />
          {tableError && (
            <Text size="sm" c="red">
              {tableError}
            </Text>
          )}
          <Group justify="flex-end" mt="md">
            <Button variant="default" onClick={() => setTableWizardOpen(false)} disabled={tableSubmitting}>
              Abbrechen
            </Button>
            <Button color="red" onClick={submitTableWizard} loading={tableSubmitting}>
              Tabelle erstellen
            </Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={!!formatSuccess} onClose={() => setFormatSuccess(null)} title="Aktion abgeschlossen" centered>
        <Stack gap="sm">
          <Text size="sm">{formatSuccess}</Text>
          <Group justify="flex-end" mt="md">
            <Button onClick={() => setFormatSuccess(null)}>OK</Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={!!labelSuccess} onClose={() => setLabelSuccess(null)} title="Aktion abgeschlossen" centered>
        <Stack gap="sm">
          <Text size="sm">{labelSuccess}</Text>
          <Group justify="flex-end" mt="md">
            <Button onClick={() => setLabelSuccess(null)}>OK</Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={!!createSuccess} onClose={() => setCreateSuccess(null)} title="Aktion abgeschlossen" centered>
        <Stack gap="sm">
          <Text size="sm">{createSuccess}</Text>
          <Group justify="flex-end" mt="md">
            <Button onClick={() => setCreateSuccess(null)}>OK</Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={!!deleteSuccess} onClose={() => setDeleteSuccess(null)} title="Aktion abgeschlossen" centered>
        <Stack gap="sm">
          <Text size="sm">{deleteSuccess}</Text>
          <Group justify="flex-end" mt="md">
            <Button onClick={() => setDeleteSuccess(null)}>OK</Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={!!tableSuccess} onClose={() => setTableSuccess(null)} title="Aktion abgeschlossen" centered>
        <Stack gap="sm">
          <Text size="sm">{tableSuccess}</Text>
          <Group justify="flex-end" mt="md">
            <Button onClick={() => setTableSuccess(null)}>OK</Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={!!resizeSuccess} onClose={() => setResizeSuccess(null)} title="Aktion abgeschlossen" centered>
        <Stack gap="sm">
          <Text size="sm">{resizeSuccess}</Text>
          <Group justify="flex-end" mt="md">
            <Button onClick={() => setResizeSuccess(null)}>OK</Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={!!moveSuccess} onClose={() => setMoveSuccess(null)} title="Aktion abgeschlossen" centered>
        <Stack gap="sm">
          <Text size="sm">{moveSuccess}</Text>
          <Group justify="flex-end" mt="md">
            <Button onClick={() => setMoveSuccess(null)}>OK</Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={pasteWizardOpen} onClose={() => setPasteWizardOpen(false)} title="Partition einfuegen" centered>
        <Stack gap="sm">
          <Text size="sm" c="dimmed">
            Quelle: <b>{clipboardPartition?.identifier ?? "-"}</b>
          </Text>
          <Text size="sm" c="dimmed">
            Ziel: <b>{pasteTargetDevice?.identifier ?? "-"}</b>
          </Text>
          {clipboardPartition && (
            <Text size="sm" c="dimmed">
              Groesse: {formatBytes(clipboardPartition.size)}
            </Text>
          )}
          {clipboardFs && !isMacMountable(clipboardFs) && (
            <Text size="xs" c="dimmed">
              Hinweis: Dieses Dateisystem wird von macOS nicht nativ gemountet.
            </Text>
          )}
          <TextInput
            label="Device-ID bestaetigen"
            value={pasteConfirmText}
            onChange={(event) => setPasteConfirmText(event.currentTarget.value)}
            placeholder={pasteTargetDevice?.identifier ?? "diskX"}
            description="Gib die Device-ID exakt ein, um fortzufahren."
          />
          {pasteError && (
            <Text size="sm" c="red">
              {pasteError}
            </Text>
          )}
          <Group justify="flex-end" mt="md">
            <Button variant="default" onClick={() => setPasteWizardOpen(false)} disabled={pasteSubmitting}>
              Abbrechen
            </Button>
            <Button onClick={submitPasteWizard} loading={pasteSubmitting}>
              Einfuegen
            </Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={!!pasteSuccess} onClose={() => setPasteSuccess(null)} title="Aktion abgeschlossen" centered>
        <Stack gap="sm">
          <Text size="sm">{pasteSuccess}</Text>
          <Group justify="flex-end" mt="md">
            <Button onClick={() => setPasteSuccess(null)}>OK</Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={!!resizeOutput} onClose={() => setResizeOutput(null)} title="Resize Output" centered>
        <Stack gap="sm">
          <Text size="xs" style={{ whiteSpace: "pre-wrap", fontFamily: "ui-monospace" }}>
            {resizeOutput}
          </Text>
          <Group justify="flex-end" mt="md">
            <Button onClick={() => setResizeOutput(null)}>OK</Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={!!moveOutput} onClose={() => setMoveOutput(null)} title="Move Output" centered>
        <Stack gap="sm">
          <Text size="xs" style={{ whiteSpace: "pre-wrap", fontFamily: "ui-monospace" }}>
            {moveOutput}
          </Text>
          <Group justify="flex-end" mt="md">
            <Button onClick={() => setMoveOutput(null)}>OK</Button>
          </Group>
        </Stack>
      </Modal>
      <Modal opened={!!pasteOutput} onClose={() => setPasteOutput(null)} title="Copy Output" centered>
        <Stack gap="sm">
          <Text size="xs" style={{ whiteSpace: "pre-wrap", fontFamily: "ui-monospace" }}>
            {pasteOutput}
          </Text>
          <Group justify="flex-end" mt="md">
            <Button onClick={() => setPasteOutput(null)}>OK</Button>
          </Group>
        </Stack>
      </Modal>
      <Modal
        opened={progressOpen}
        onClose={() => {}}
        withCloseButton={false}
        closeOnClickOutside={false}
        closeOnEscape={false}
        title="Vorgang laeuft"
        centered
      >
        <Stack gap="sm">
          <Text size="sm">
            Bitte Computer nicht ausschalten. {progressMessage ? progressMessage : "In Bearbeitung"}
          </Text>
          <Slider value={progressPercent} min={0} max={100} step={1} disabled />
          <Text size="xs" c="dimmed">
            {progressPercent}%
          </Text>
          {progressBytes && progressBytes.total > 0 && (
            <Text size="xs" c="dimmed">
              {formatBytes(progressBytes.current)} / {formatBytes(progressBytes.total)}
            </Text>
          )}
          {progressLog.length > 0 && (
            <Text size="xs" style={{ whiteSpace: "pre-wrap", fontFamily: "ui-monospace" }}>
              {progressLog.join("\n")}
            </Text>
          )}
        </Stack>
      </Modal>
      <Modal opened={sidecarOpen} onClose={() => setSidecarOpen(false)} title="Sidecar Status" centered>
        <Stack gap="sm">
          {sidecarLoading && <Text size="sm" c="dimmed">Pruefe Sidecars…</Text>}
          {!sidecarLoading && sidecarStatus.length === 0 && (
            <Text size="sm" c="dimmed">Keine Informationen verfuegbar.</Text>
          )}
          {!sidecarLoading && sidecarStatus.length > 0 && (
            <Stack gap="xs">
              {sidecarStatus.map((item) => (
                <Group key={item.name} justify="space-between">
                  <Text size="sm">
                    {item.found ? "✅" : "❌"} {item.name}
                  </Text>
                  <Text size="xs" c="dimmed">
                    {item.version ?? (item.path ?? "Nicht gefunden")}
                  </Text>
                </Group>
              ))}
            </Stack>
          )}
          <Group justify="flex-end" mt="md">
            <Button onClick={() => setSidecarOpen(false)}>OK</Button>
          </Group>
        </Stack>
      </Modal>
      {/* --- HEADER --- */}
      <AppShell.Header>
        <Group h="100%" px="md" justify="space-between" style={{ WebkitAppRegion: "drag" }}>
          <Group>
            <Burger opened={opened} onClick={toggle} hiddenFrom="sm" size="sm" />
            <ThemeIcon variant="gradient" gradient={{ from: "indigo", to: "cyan" }} radius="md" size="lg">
              <IconChartPie size={22} />
            </ThemeIcon>
            <Text fw={800} size="xl" style={{ letterSpacing: "-0.5px" }}>
              Oxidisk
            </Text>
          </Group>
          <Group gap="xs" style={{ WebkitAppRegion: "no-drag" }}>
            <Button variant="subtle" onClick={loadDisks} leftSection={<IconRefresh size={18} />}>
              Neu laden
            </Button>
            <ActionIcon
              variant="light"
              size="lg"
              aria-label="Dark Mode umschalten"
              onClick={() => setColorScheme(colorScheme === "dark" ? "light" : "dark")}
            >
              {colorScheme === "dark" ? <IconSun size={18} /> : <IconMoon size={18} />}
            </ActionIcon>
            <Menu position="bottom-end" shadow="md" withArrow>
              <Menu.Target>
                <Button variant="light" leftSection={<IconSettings size={18} />}>
                  Einstellungen
                </Button>
              </Menu.Target>
              <Menu.Dropdown>
                <Stack gap="md">
                  <Switch
                    label="System-Volumes anzeigen"
                    checked={showSystemVolumes}
                    onChange={(event) => setShowSystemVolumes(event.currentTarget.checked)}
                  />
                  <Switch
                    label="Power-User-Daten im Inspector"
                    checked={showPowerDataInspector}
                    onChange={(event) => setShowPowerDataInspector(event.currentTarget.checked)}
                  />
                </Stack>
              </Menu.Dropdown>
            </Menu>
          </Group>
        </Group>
      </AppShell.Header>

      {/* --- SIDEBAR --- */}
      <AppShell.Navbar p="md" style={{ backgroundColor: "var(--mantine-color-body)" }}>
        <ScrollArea>
          <Text size="xs" fw={700} c="dimmed" mb="sm" tt="uppercase">
            Ansicht
          </Text>
          <NavLink
            label={<Text fw={600}>Analyzer</Text>}
            leftSection={
              <ThemeIcon color="indigo" variant="light">
                <IconChartPie size={16} />
              </ThemeIcon>
            }
            active={activeView === "analyzer"}
            onClick={() => setActiveView("analyzer")}
            variant="filled"
            color={activeView === "analyzer" ? "indigo" : "gray"}
            style={{ borderRadius: 8, marginBottom: 4 }}
          />
          <NavLink
            label={<Text fw={600}>Partition Manager</Text>}
            leftSection={
              <ThemeIcon color="orange" variant="light">
                <IconDatabase size={16} />
              </ThemeIcon>
            }
            active={activeView === "partition"}
            onClick={() => setActiveView("partition")}
            variant="filled"
            color={activeView === "partition" ? "orange" : "gray"}
            style={{ borderRadius: 8, marginBottom: 12 }}
          />
          <NavLink
            label={<Text fw={600}>Image Writer</Text>}
            leftSection={
              <ThemeIcon color="teal" variant="light">
                <IconDeviceFloppy size={16} />
              </ThemeIcon>
            }
            active={activeView === "images"}
            onClick={() => setActiveView("images")}
            variant="filled"
            color={activeView === "images" ? "teal" : "gray"}
            style={{ borderRadius: 8, marginBottom: 12 }}
          />

          <Text size="xs" fw={700} c="dimmed" mb="sm" tt="uppercase">
            Deine Laufwerke
          </Text>
          {activeView === "analyzer" && (
            <>
              {disks.map((disk, index) => (
                <NavLink
                  key={index}
                  label={<Text fw={600}>{disk.name}</Text>}
                  leftSection={
                    <ThemeIcon color={disk.is_removable ? "orange" : "indigo"} variant="light">
                      {disk.is_removable ? <IconDeviceFloppy size={16} /> : <IconDatabase size={16} />}
                    </ThemeIcon>
                  }
                  description={disk.is_mounted ? `${formatBytes(disk.available_space)} frei` : "Nicht eingehängt"}
                  active={currentDisk?.mount_point === disk.mount_point}
                  onClick={() => {
                    if (!disk.is_mounted) {
                      window.alert("Bitte mounten Sie dieses Laufwerk zuerst im Finder oder Festplattendienstprogramm.");
                      return;
                    }
                    startScan(disk);
                  }}
                  variant="filled"
                  color={disk.is_mounted ? "indigo" : "gray"}
                  style={{ borderRadius: 8, marginBottom: 4 }}
                />
              ))}
              <Button variant="light" fullWidth mt="sm" leftSection={<IconFolder size={16} />} onClick={chooseFolder}>
                Ordner wählen…
              </Button>
            </>
          )}
          {activeView === "partition" && (
            <Button
              variant="light"
              fullWidth
              mt="sm"
              leftSection={<IconRefresh size={16} />}
              onClick={loadPartitionDevices}
            >
              Geräte aktualisieren
            </Button>
          )}
        </ScrollArea>
      </AppShell.Navbar>

      {/* --- MAIN CONTENT --- */}
      <AppShell.Main style={{ display: "flex", flexDirection: "column", height: "100vh", overflow: "hidden" }}>
        {activeView === "partition" && (
          <Group h="100%" gap="md" align="stretch">
            <Paper
              withBorder
              p="md"
              radius="md"
              shadow="sm"
              style={{ flex: 1, minWidth: 260, display: "flex", flexDirection: "column", gap: 12 }}
            >
              <Group justify="space-between" align="center">
                <div>
                  <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                    Laufwerke
                  </Text>
                  <Title order={4}>Geraete</Title>
                </div>
                <ActionIcon variant="light" onClick={loadPartitionDevices} aria-label="Neu laden">
                  <IconRefresh size={18} />
                </ActionIcon>
              </Group>
              <Divider />
              {partitionLoading && (
                <Center style={{ flex: 1 }}>
                  <Stack align="center">
                    <Loader size="lg" type="dots" color="orange" />
                    <Text c="dimmed">Suche nach Geraeten…</Text>
                  </Stack>
                </Center>
              )}
              {!partitionLoading && partitionDevices.length === 0 && (
                <Center style={{ flex: 1 }}>
                  <Stack align="center">
                    <ThemeIcon size={56} radius="xl" color="gray" variant="light">
                      <IconDatabase size={28} />
                    </ThemeIcon>
                    <Text c="dimmed">Keine Geraete gefunden.</Text>
                  </Stack>
                </Center>
              )}
              {!partitionLoading && partitionDevices.length > 0 && (
                <Stack gap="xs">
                  {partitionDevices.map((device) => {
                    const active = device.identifier === selectedPartitionDeviceId;
                    return (
                      <Paper
                        key={device.identifier}
                        withBorder
                        radius="md"
                        p="sm"
                        className={active ? "device-card device-card--active" : "device-card"}
                        onClick={() => {
                          setSelectedPartitionDeviceId(device.identifier);
                          setSelectedPartitionId(device.partitions[0]?.identifier ?? null);
                            setSelectedPartition(device.partitions[0] ?? null);
                          setSelectedUnallocated(null);
                        }}
                      >
                        <Group justify="space-between" align="center" wrap="nowrap">
                          <Group gap="sm" wrap="nowrap">
                            <ThemeIcon
                              size={40}
                              radius="md"
                              color={device.internal ? "gray" : "teal"}
                              variant="light"
                            >
                              {device.internal ? <IconDatabase size={20} /> : <IconDeviceFloppy size={20} />}
                            </ThemeIcon>
                            <div>
                              <Text fw={600}>{device.identifier}</Text>
                              <Text size="xs" c="dimmed">
                                {formatBytes(device.size)} · {device.internal ? "Intern" : "Extern"}
                              </Text>
                            </div>
                          </Group>
                          {device.is_protected && (
                            <Tooltip label={device.protection_reason ?? "SIP geschuetzt"}>
                              <IconLock size={14} color="var(--mantine-color-red-6)" />
                            </Tooltip>
                          )}
                        </Group>
                      </Paper>
                    );
                  })}
                </Stack>
              )}
            </Paper>

            <Paper
              withBorder
              p="md"
              radius="md"
              shadow="sm"
              style={{ flex: 3, display: "flex", flexDirection: "column", gap: 12 }}
            >
              <Group justify="space-between" align="center">
                <div>
                  <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                    Partition Manager
                  </Text>
                  <Title order={3}>Partitionen verwalten</Title>
                  <Text size="sm" c="dimmed">
                    Aktionen laufen ueber den privilegierten Helper. Auswahl links bestimmt den Fokus.
                  </Text>
                  {clipboardPartition && (
                    <Badge color="indigo" variant="light" mt="xs">
                      Copied: {clipboardPartition.identifier}
                    </Badge>
                  )}
                </div>
                <Group gap="xs">
                  <Button variant="light" onClick={() => setSudoSetupOpen(true)}>
                    Helper einrichten
                  </Button>
                  <Button
                    variant="light"
                    onClick={() => {
                      setSidecarOpen(true);
                      loadSidecarStatus();
                    }}
                  >
                    Sidecars
                  </Button>
                </Group>
              </Group>
              <Divider />

              {!selectedPartitionDevice && (
                <Center style={{ flex: 1 }}>
                  <Stack align="center">
                    <ThemeIcon size={64} radius="xl" color="gray" variant="light">
                      <IconDatabase size={32} />
                    </ThemeIcon>
                    <Text c="dimmed">Bitte ein Laufwerk links auswaehlen.</Text>
                  </Stack>
                </Center>
              )}

              {selectedPartitionDevice && (
                <>
                  <Group justify="space-between" align="center">
                    <div>
                      <Text fw={700}>{selectedPartitionDevice.identifier}</Text>
                      <Text size="sm" c="dimmed">
                        {formatBytes(selectedPartitionDevice.size)} · {selectedPartitionDevice.internal ? "Intern" : "Extern"} · {selectedPartitionDevice.content}
                      </Text>
                    </div>
                    <Group gap="xs">
                      <Button
                        size="xs"
                        variant="light"
                        onClick={() => openWipeWizard(selectedPartitionDevice)}
                        disabled={selectedPartitionDevice.is_protected}
                      >
                        Geraet loeschen
                      </Button>
                      <Button
                        size="xs"
                        variant="light"
                        disabled={selectedPartitionDevice.is_protected || deviceFreeBytes(selectedPartitionDevice) === 0}
                        onClick={() => openCreateWizard(selectedPartitionDevice)}
                      >
                        Partition erstellen
                      </Button>
                      <Button
                        size="xs"
                        variant="light"
                        disabled={
                          selectedPartitionDevice.is_protected ||
                          !clipboardPartition ||
                          deviceFreeBytes(selectedPartitionDevice) < (clipboardPartition?.size ?? 0)
                        }
                        onClick={() => openPasteWizard(selectedPartitionDevice)}
                      >
                        Einfuegen
                      </Button>
                      <Button
                        size="xs"
                        variant="light"
                        disabled={selectedPartitionDevice.is_protected}
                        onClick={() => openTableWizard(selectedPartitionDevice)}
                      >
                        Partitionstabelle
                      </Button>
                    </Group>
                  </Group>

                  <Paper withBorder radius="lg" p="md" className="partition-bar">
                    <div className="partition-bar__scroll" ref={partitionBarRef}>
                      <div className="partition-bar__track">
                        {partitionSegments.map((segment) => {
                          const isSelected = segment.kind === "partition"
                            ? segment.partition?.identifier === selectedPartitionId
                            : selectedUnallocated?.offset === segment.offset;
                          const width = (segment.size / selectedPartitionDevice.size) * 100;
                          const label = segment.kind === "unallocated" ? formatBytes(segment.size) : segment.label;
                          const showLabel =
                            partitionBarWidth > 0
                              ? (partitionBarWidth * width) / 100 >= 40
                              : width >= 8;
                          const isLocked = !!segment.partition?.is_protected;
                          return (
                            <button
                              key={segment.key}
                              type="button"
                              className={
                                [
                                  "partition-segment",
                                  isSelected ? "partition-segment--selected" : "",
                                  isLocked ? "partition-segment--locked" : "",
                                ]
                                  .filter(Boolean)
                                  .join(" ")
                              }
                              style={{ width: `${width}%`, background: fsColorForSegment(segment.fsType, segment.kind) }}
                              title={label}
                              onClick={() => {
                                if (segment.kind === "partition" && segment.partition) {
                                  setSelectedPartitionId(segment.partition.identifier);
                                  setSelectedPartition(segment.partition);
                                  setSelectedUnallocated(null);
                                } else {
                                  setSelectedPartitionId(null);
                                  setSelectedPartition(null);
                                  setSelectedUnallocated({ offset: segment.offset, size: segment.size });
                                }
                              }}
                            >
                              {isLocked && <IconLock size={12} className="partition-segment__lock" />}
                              <span className="partition-segment__label">
                                {showLabel ? label : ""}
                              </span>
                            </button>
                          );
                        })}
                      </div>
                    </div>
                  </Paper>

                  <Group justify="space-between" align="center" mt="xs">
                    <Text size="sm" c="dimmed">
                      Auswahl: {selectedPartition ? `${selectedPartition.identifier} · ${selectedPartition.name || ""}`.trim() : selectedUnallocated ? "Unallocated" : "-"}
                    </Text>
                    <Group gap="xs">
                      <Button
                        size="xs"
                        variant="light"
                        disabled={!selectedPartition || selectedPartition.is_protected}
                        onClick={() => selectedPartition && copyToClipboard(selectedPartition)}
                      >
                        Kopieren
                      </Button>
                      <Button
                        size="xs"
                        variant="light"
                        disabled={!selectedPartition || selectedPartition.is_protected}
                        onClick={() => selectedPartition && openFormatWizard(selectedPartition)}
                      >
                        Formatieren
                      </Button>
                      <Button
                        size="xs"
                        variant="light"
                        disabled={!selectedPartition || selectedPartition.is_protected}
                        onClick={() => selectedPartition && openResizeWizard(selectedPartition)}
                      >
                        Groesse
                      </Button>
                      <Button
                        size="xs"
                        variant="light"
                        disabled={!selectedPartition || selectedPartition.is_protected}
                        onClick={() => selectedPartition && openMoveWizard(selectedPartition)}
                      >
                        Verschieben
                      </Button>
                      <Button
                        size="xs"
                        variant="light"
                        disabled={!selectedPartition || selectedPartition.is_protected}
                        onClick={() => selectedPartition && openCheckWizard(selectedPartition)}
                      >
                        Ueberpruefen
                      </Button>
                      <Button
                        size="xs"
                        variant="light"
                        disabled={!selectedPartition || selectedPartition.is_protected}
                        onClick={() => selectedPartition && openLabelWizard(selectedPartition)}
                      >
                        Label/UUID
                      </Button>
                      {selectedPartition && fsTypeFromPartition(selectedPartition) === "apfs" && (
                        <Button
                          size="xs"
                          variant="light"
                          disabled={selectedPartition.is_protected}
                          onClick={() => openApfsManager(selectedPartition)}
                        >
                          Volumen verwalten
                        </Button>
                      )}
                      <Button
                        size="xs"
                        variant="light"
                        color="red"
                        disabled={!selectedPartition || selectedPartition.is_protected}
                        onClick={() => selectedPartition && openDeleteWizard(selectedPartition)}
                      >
                        Loeschen
                      </Button>
                    </Group>
                  </Group>
                  {selectedPartition?.is_protected && (
                    <Text size="xs" c="dimmed">
                      Systempartition (SIP) erkannt. Aenderungen sind gesperrt.
                    </Text>
                  )}

                  <Paper withBorder radius="md" p="sm" className="partition-table">
                    <Table highlightOnHover verticalSpacing="sm">
                      <Table.Thead>
                        <Table.Tr>
                          <Table.Th></Table.Th>
                          <Table.Th>Name</Table.Th>
                          <Table.Th>Dateisystem</Table.Th>
                          <Table.Th>Mountpoint</Table.Th>
                          <Table.Th>Groesse</Table.Th>
                          <Table.Th>Flags</Table.Th>
                        </Table.Tr>
                      </Table.Thead>
                      <Table.Tbody>
                        {partitionSegments.map((segment) => {
                          const isSelected = segment.kind === "partition"
                            ? segment.partition?.identifier === selectedPartitionId
                            : selectedUnallocated?.offset === segment.offset;
                          const fsLabel = fsLabelForType(segment.fsType, segment.kind);
                          return (
                            <Table.Tr
                              key={`row-${segment.key}`}
                              data-selected={isSelected}
                              onClick={() => {
                                if (segment.kind === "partition" && segment.partition) {
                                  setSelectedPartitionId(segment.partition.identifier);
                                  setSelectedPartition(segment.partition);
                                  setSelectedUnallocated(null);
                                } else {
                                  setSelectedPartitionId(null);
                                  setSelectedPartition(null);
                                  setSelectedUnallocated({ offset: segment.offset, size: segment.size });
                                }
                              }}
                            >
                              <Table.Td>
                                <span
                                  className="partition-dot"
                                  style={{ background: fsColorForSegment(segment.fsType, segment.kind) }}
                                />
                              </Table.Td>
                              <Table.Td>
                                <Text fw={600} size="sm">
                                  {segment.kind === "unallocated"
                                    ? "Unallocated"
                                    : segment.partition?.identifier}
                                </Text>
                                {segment.partition?.name && (
                                  <Text size="xs" c="dimmed">
                                    {segment.partition.name}
                                  </Text>
                                )}
                              </Table.Td>
                              <Table.Td>
                                <Text size="sm">
                                  {segment.kind === "unallocated" ? "-" : fsLabel}
                                </Text>
                              </Table.Td>
                              <Table.Td>
                                <Text size="sm" c="dimmed">
                                  {segment.partition?.mount_point ?? "-"}
                                </Text>
                              </Table.Td>
                              <Table.Td>
                                <Text size="sm" className="tabular-nums">
                                  {formatBytes(segment.size)}
                                </Text>
                              </Table.Td>
                              <Table.Td>
                                {segment.partition?.is_protected ? (
                                  <Text size="xs" c="red">
                                    SIP
                                  </Text>
                                ) : (
                                  <Text size="xs" c="dimmed">
                                    -
                                  </Text>
                                )}
                              </Table.Td>
                            </Table.Tr>
                          );
                        })}
                      </Table.Tbody>
                    </Table>
                  </Paper>
                </>
              )}
            </Paper>
          </Group>
        )}

        {activeView === "images" && (
          <Group h="100%" gap="md" align="stretch">
            <Paper withBorder p="md" radius="md" shadow="sm" style={{ flex: 2, display: "flex", flexDirection: "column", gap: 12 }}>
              {imageRunning ? (
                <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center" }}>
                  <Paper
                    withBorder
                    radius="lg"
                    p="xl"
                    style={{
                      width: "min(760px, 92%)",
                      textAlign: "center",
                      position: "relative",
                      background: "var(--mantine-color-body)",
                    }}
                  >
                    <Stack gap="md" align="center">
                      <div className="pulse-icon">
                        {(imageMode === "backup" ? null : renderImageBrandIcon(imageBrand)) ??
                          (imageMode === "windows" || imageWindowsDetected ? (
                            <IconBrandWindows size={64} color="#3b82f6" />
                          ) : (
                            <IconDeviceFloppy size={64} color="#f97316" />
                          ))}
                      </div>
                      <div>
                        <Text size="xs" c="dimmed" tt="uppercase" fw={700}>
                          Image Writer aktiv
                        </Text>
                        <Title order={3} mt={4}>
                          {imageTarget ? `Schreibe auf ${imageTarget}` : "Vorgang laeuft"}
                        </Title>
                        <Text size="sm" c="dimmed">
                          {imagePath ? truncateMiddle(imagePath, 52) : ""}
                        </Text>
                        {imageMode !== "backup" && imageLabel && (
                          <Text size="xs" c="dimmed">
                            {truncateMiddle(imageLabel, 52)}
                          </Text>
                        )}
                      </div>
                      <Progress value={progressPercent} size="lg" radius="xl" />
                      <Group justify="space-between" style={{ width: "100%" }}>
                        <Text size="sm" c="dimmed">
                          {(() => {
                            if (!progressMessage) return "";
                            const parts = progressMessage.split(" · ");
                            const left = parts[0] ?? progressMessage;
                            return truncateMiddle(left, 40);
                          })()}
                        </Text>
                        <Text size="sm" c="dimmed" className="tabular-nums">
                          {(() => {
                            const speed =
                              progressSpeed != null ? `${(progressSpeed / (1024 * 1024)).toFixed(1)} MB/s` : "";
                            const eta = progressEta != null ? `· ${formatEta(progressEta)}` : "";
                            if (speed || eta) return `${speed} ${eta}`.trim();
                            if (!progressMessage) return "";
                            const parts = progressMessage.split(" · ");
                            return parts[1] ?? "";
                          })()}
                        </Text>
                      </Group>
                      <Group justify="space-between" style={{ width: "100%" }} mt="sm">
                        <Button
                          variant="outline"
                          color="red"
                          onClick={async () => {
                            try {
                              await invoke("cancel_helper_operation");
                              setImageRunning(false);
                              setProgressOpen(false);
                              setProgressMessage(null);
                              setProgressBytes(null);
                              setProgressSpeed(null);
                              setProgressEta(null);
                            } catch (error) {
                              setImageError(String(error));
                            }
                          }}
                        >
                          Abbrechen
                        </Button>
                        <Button
                          variant="subtle"
                          onClick={() => setImageShowLog((prev) => !prev)}
                        >
                          {imageShowLog ? "Log ausblenden" : "Log anzeigen"}
                        </Button>
                      </Group>
                    </Stack>
                    {imageShowLog && progressLog.length > 0 && (
                      <Paper
                        withBorder
                        radius="md"
                        p="sm"
                        style={{
                          position: "absolute",
                          bottom: 16,
                          left: "50%",
                          transform: "translateX(-50%)",
                          width: "min(720px, 92%)",
                          maxHeight: 200,
                          overflow: "auto",
                          textAlign: "left",
                        }}
                      >
                        <Text size="xs" style={{ whiteSpace: "pre-wrap", fontFamily: "ui-monospace" }}>
                          {progressLog.join("\n")}
                        </Text>
                      </Paper>
                    )}
                  </Paper>
                </div>
              ) : (
                <>
                  <Group justify="space-between" align="center">
                    <div>
                      <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                        Image Writer
                      </Text>
                      <Title order={3}>Flash Images auf USB/SD</Title>
                      <Text size="sm" c="dimmed">
                        Schreibe ISO/IMG/DMG direkt auf einen Stick. Optional mit SHA-256 Verifikation.
                      </Text>
                    </div>
                    <Group gap="xs">
                      <Button variant="light" onClick={loadPartitionDevices} leftSection={<IconRefresh size={18} />}>
                        Neu laden
                      </Button>
                    </Group>
                  </Group>
                  <Divider />
                </>
              )}

              {!imageRunning && (
                <>
                  <Group justify="space-between" align="center">
                    <Text fw={600}>Modus</Text>
                    <NativeSelect
                      value={imageMode}
                      onChange={(event) => setImageMode(event.currentTarget.value as "write" | "backup" | "windows")}
                      data={[
                        { value: "write", label: "Schreiben (Image -> Stick)" },
                        { value: "backup", label: "Backup erstellen (Stick -> Image)" },
                        { value: "windows", label: "Windows Installer (File Copy)" },
                      ]}
                    />
                  </Group>

                  <Stack gap="md">
                    <Paper withBorder radius="lg" p="md" shadow="sm" className="wizard-card">
                      <Stack gap="xs">
                        <Group justify="space-between" align="center">
                          <Text fw={600}>Quelle</Text>
                          <Badge variant="light" color="blue">
                            {imageMode === "backup" ? "Device" : "ISO/IMG/DMG"}
                          </Badge>
                        </Group>
                        {imageMode === "write" || imageMode === "windows" ? (
                          <Stack gap="xs">
                            <Paper
                              withBorder
                              radius="md"
                              p="md"
                              className={imageDropActive ? "dropzone dropzone--active" : "dropzone"}
                              onDragEnter={() => setImageDropActive(true)}
                              onDragLeave={() => setImageDropActive(false)}
                              onDragOver={(event) => {
                                event.preventDefault();
                                event.stopPropagation();
                              }}
                              onDrop={handleImageDrop}
                              onClick={chooseImageFile}
                            >
                              <Group justify="space-between" align="center" wrap="nowrap">
                                <Group gap="sm" wrap="nowrap">
                                  <ThemeIcon size={44} radius="md" variant="light" color="blue">
                                    <IconDeviceFloppy size={22} />
                                  </ThemeIcon>
                                  <div>
                                    <Text fw={600}>{imagePath ? "Quelle erkannt" : "Drag & Drop"}</Text>
                                    <Text size="xs" c="dimmed">
                                      {imagePath ? truncateMiddle(imagePath, 52) : "ISO/IMG/DMG hier ablegen oder klicken"}
                                    </Text>
                                  </div>
                                </Group>
                                <Button variant="light">Auswaehlen</Button>
                              </Group>
                            </Paper>
                            <Group gap="xs" align="center">
                              <Button
                                variant="subtle"
                                onClick={computeImageHash}
                                disabled={!imagePath}
                                loading={imageHashRunning}
                              >
                                SHA-256 berechnen
                              </Button>
                              {imageHash && (
                                <Text size="xs" c="dimmed">
                                  {imageHash}
                                </Text>
                              )}
                              {imageHashError && (
                                <Text size="xs" c="red">
                                  {imageHashError}
                                </Text>
                              )}
                            </Group>
                            {imageMode === "write" && imageWindowsDetected && (
                              <Stack gap={4}>
                                <Text size="sm" c="red">
                                  Achtung: Windows-ISO erkannt. Der Windows-Installer-Modus ist in Arbeit.
                                </Text>
                                {imageWindowsReason && (
                                  <Text size="xs" c="dimmed">
                                    Hinweis: {imageWindowsReason}
                                  </Text>
                                )}
                                <Switch
                                  label="Trotzdem flashen (nur Daten-Stick)"
                                  checked={imageWindowsOverride}
                                  onChange={(event) => setImageWindowsOverride(event.currentTarget.checked)}
                                />
                              </Stack>
                            )}
                            {imageMode === "windows" && (
                              <Stack gap={4}>
                                {imageWindowsDetected ? (
                                  <Text size="sm" c="dimmed">
                                    Windows-ISO erkannt. Installer wird per Datei-Kopie erstellt.
                                  </Text>
                                ) : (
                                  <Text size="sm" c="red">
                                    Keine Windows-ISO erkannt.
                                  </Text>
                                )}
                                {imageWindowsReason && (
                                  <Text size="xs" c="dimmed">
                                    Hinweis: {imageWindowsReason}
                                  </Text>
                                )}
                              </Stack>
                            )}
                          </Stack>
                        ) : (
                          <Text size="sm" c="dimmed">
                            Quelle ist das ausgewaehlte Geraet im Zielbereich.
                          </Text>
                        )}
                      </Stack>
                    </Paper>

                    <Paper withBorder radius="lg" p="md" shadow="sm" className="wizard-card">
                      <Stack gap="xs">
                        <Group justify="space-between" align="center">
                          <Text fw={600}>{imageMode === "write" ? "Zielgeraet" : "Quellgeraet"}</Text>
                          <Switch
                            label="Alle Laufwerke"
                            checked={showAllImageTargets}
                            onChange={(event) => setShowAllImageTargets(event.currentTarget.checked)}
                          />
                        </Group>
                        <NativeSelect
                          label={imageMode === "write" ? "USB/SD Target" : "USB/SD Quelle"}
                          value={imageTarget}
                          onChange={(event) => setImageTarget(event.currentTarget.value)}
                          data={imageTargetOptions()}
                          placeholder="Geraet waehlen"
                        />
                        {imageTargetOptions().length === 0 && (
                          <Text size="xs" c="dimmed">
                            Keine externen Laufwerke gefunden. Aktiviere "Alle Laufwerke" um interne Disks zu sehen.
                          </Text>
                        )}
                      </Stack>
                    </Paper>

                    <Paper withBorder radius="lg" p="md" shadow="sm" className="wizard-card">
                      <Stack gap="xs">
                        <Text fw={600}>Einstellungen</Text>
                        {imageMode === "write" ? (
                          <Switch
                            label="SHA-256 Verifikation"
                            checked={imageVerify}
                            onChange={(event) => setImageVerify(event.currentTarget.checked)}
                          />
                        ) : imageMode === "backup" ? (
                          <Switch
                            label="Backup komprimieren (.img.gz)"
                            checked={imageBackupCompress}
                            onChange={(event) => setImageBackupCompress(event.currentTarget.checked)}
                          />
                        ) : (
                          <Text size="sm" c="dimmed">
                            Zielstick wird als GPT + ExFAT vorbereitet (UEFI kompatibel).
                          </Text>
                        )}
                        <Switch
                          label="Nach Erfolg sicher auswerfen"
                          checked={imageAutoEject}
                          onChange={(event) => setImageAutoEject(event.currentTarget.checked)}
                        />
                      </Stack>
                    </Paper>

                    {imageMode === "backup" && (
                      <Paper withBorder radius="lg" p="md" shadow="sm" className="wizard-card">
                        <Stack gap="xs">
                          <Text fw={600}>Zielpfad</Text>
                          <Group gap="xs" align="center">
                            <Button variant="light" onClick={chooseBackupTarget} disabled={!imageTarget}>
                              Zielordner waehlen
                            </Button>
                            <Text size="xs" c="dimmed">
                              {imageTarget ? "Pfad wird aus Ordner + Dateiname gebaut" : "Geraet zuerst waehlen"}
                            </Text>
                          </Group>
                          <TextInput
                            label="Dateipfad"
                            value={imageBackupPath}
                            onChange={(event) => setImageBackupPath(event.currentTarget.value)}
                            placeholder="/Users/you/Desktop/oxidisk-diskX.img"
                          />
                        </Stack>
                      </Paper>
                    )}

                    {imageMode === "windows" && (
                      <Paper withBorder radius="lg" p="md" shadow="sm" className="wizard-card">
                        <Stack gap="xs">
                          <Text fw={600}>Label</Text>
                          <TextInput
                            label="Volume-Label"
                            value={imageWindowsLabel}
                            onChange={(event) => setImageWindowsLabel(event.currentTarget.value)}
                            placeholder="WINSTALL"
                          />
                          <Text size="xs" c="dimmed">
                            ExFAT ist Standard fuer Windows-ISOs. FAT32-Fallback (zwei Partitionen) ist fuer Advanced Users geplant.
                          </Text>
                        </Stack>
                      </Paper>
                    )}

                    <Paper withBorder radius="lg" p="md" shadow="sm" className="wizard-card">
                      <Stack gap="xs">
                        <Text fw={600}>
                          {imageMode === "write" ? "Flash" : imageMode === "backup" ? "Backup" : "Windows Installer"}
                        </Text>
                        <TextInput
                          label="Device-ID bestaetigen"
                          value={imageConfirmText}
                          onChange={(event) => setImageConfirmText(event.currentTarget.value)}
                          placeholder={imageTarget || "diskX"}
                          description="Gib die Device-ID exakt ein, um fortzufahren."
                        />
                        {imageError && (
                          <Text size="sm" c="red">
                            {imageError}
                          </Text>
                        )}
                      </Stack>
                    </Paper>

                    <Paper withBorder radius="xl" p="md" shadow="md" className="hero-action">
                      <Group justify="space-between" align="center" wrap="nowrap">
                        <div>
                          <Text fw={700} size="sm">
                            Bereit zum Start
                          </Text>
                          <Text size="xs" c="dimmed">
                            {imageMode === "write"
                              ? "Image wird direkt auf das Ziel geschrieben."
                              : imageMode === "backup"
                                ? "Backup wird mit Verifikation erstellt."
                                : "Windows-Installer erstellt den Stick per Datei-Kopie."}
                          </Text>
                        </div>
                        <Button
                          size="md"
                          color={imageMode === "backup" ? "blue" : "red"}
                          onClick={submitFlashImage}
                          loading={imageRunning}
                          disabled={
                            imageMode === "write"
                              ? !imagePath || !imageTarget
                              : imageMode === "backup"
                                ? !imageTarget
                                : !imagePath || !imageTarget || !imageWindowsDetected
                          }
                        >
                          {imageMode === "write"
                            ? "Flash starten"
                            : imageMode === "backup"
                              ? "Backup starten"
                              : "Windows-Installer erstellen"}
                        </Button>
                      </Group>
                    </Paper>
                  </Stack>
                </>
              )}
            </Paper>
          </Group>
        )}

        {/* FALL 1: Lädt */}
        {activeView === "analyzer" && loading && (
          <Center style={{ flex: 1 }}>
            <Stack align="center">
              <Loader size="xl" type="dots" color="indigo" />
              <Text c="dimmed" size="lg">
                Scanne Dateisystem...
              </Text>
              <Text size="xs" c="dimmed">
                Das kann bei großen Platten einen Moment dauern.
              </Text>
            </Stack>
          </Center>
        )}

        {/* FALL 2: Nichts ausgewählt */}
        {activeView === "analyzer" && !loading && !scanData && (
          <Center style={{ flex: 1 }}>
            <Stack align="center" gap="xs">
              <ThemeIcon size={80} radius="xl" color="gray" variant="light">
                <IconDatabase size={40} />
              </ThemeIcon>
              <Title order={3} c="dimmed">
                Kein Laufwerk ausgewählt
              </Title>
              <Text c="dimmed">Wähle ein Volume aus der Liste links.</Text>
            </Stack>
          </Center>
        )}

        {/* FALL 3: Daten da! Das DASHBOARD */}
        {activeView === "analyzer" && !loading && scanData && currentDisk && (
          <Group h="100%" gap="md" align="stretch">
            {/* 1. Übersichtskarte (Total / Used) */}
            <Paper withBorder p="md" radius="md" shadow="sm" style={{ flex: 2, display: "flex", flexDirection: "column", gap: 12 }}>
              {currentDisk ? (
                <Group justify="space-between">
                  <Group>
                    <RingProgress
                      size={70}
                      thickness={8}
                      roundCaps
                      sections={[{ value: usagePercent, color: usagePercent > 90 ? "red" : "indigo" }]}
                      label={
                        <Center>
                          <Stack gap={2} align="center">
                            <IconDatabase size={18} style={{ opacity: 0.9, color: "white" }} />
                            <Text size="xs" fw={700} c="white">
                              {usagePercentLabel}
                            </Text>
                          </Stack>
                        </Center>
                      }
                    />
                    <div>
                      <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                        Laufwerk
                      </Text>
                      <Text fw={700} size="lg">
                        {currentDisk.name}
                      </Text>
                      <Text size="sm" c="dimmed">
                        {formatBytes(currentDisk.total_space - currentDisk.available_space)} belegt von {formatBytes(currentDisk.total_space)}
                      </Text>
                    </div>
                  </Group>
                  <Stack gap={0} align="flex-end">
                    <Text size="xl" fw={900} c="indigo">
                      {formatBytes(currentDisk.available_space)}
                    </Text>
                    <Text size="sm" c="dimmed">
                      Freier Speicher
                    </Text>
                  </Stack>
                </Group>
              ) : (
                <Group justify="space-between">
                  <Group>
                    <ThemeIcon variant="light" size="lg" radius="md">
                      <IconFolder size={18} />
                    </ThemeIcon>
                    <div>
                      <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                        Ordner
                      </Text>
                      <Text fw={700} size="lg">
                        {currentRootName ?? "Ordner"}
                      </Text>
                      <Text size="sm" c="dimmed">
                        Größe: {scanData ? formatBytes(scanData.value) : "-"}
                      </Text>
                    </div>
                  </Group>
                  <Stack gap={0} align="flex-end">
                    <Text size="xl" fw={900} c="indigo">
                      {scanData ? formatBytes(scanData.value) : "-"}
                    </Text>
                    <Text size="sm" c="dimmed">
                      Gesamt
                    </Text>
                  </Stack>
                </Group>
              )}
              <Divider />

              {/* 2. Breadcrumbs (Pfad Navigation) */}
              <Paper px="md" py="xs" radius="md" style={{ backgroundColor: "var(--mantine-color-body)" }}>
                <Breadcrumbs separator="→" style={{ flexWrap: "wrap" }}>
                  {pathParts.map((part, index) => (
                    <Anchor key={index} size="sm" onClick={() => console.log("Navigiere zu", part)}>
                      {part === "/" || part === currentRootPath ? currentRootName ?? part : part}
                    </Anchor>
                  ))}
                </Breadcrumbs>
              </Paper>

              {/* 3. Das Chart */}
              <div
                style={{
                  flex: 1,
                  minHeight: 0,
                  position: "relative",
                  background: "var(--mantine-color-body)",
                  borderRadius: 8,
                  border: "1px solid var(--mantine-color-default-border)",
                }}
              >
                <ResponsiveSunburst
                  data={scanData}
                  id={(node) => (node as FileNode).path || (node as FileNode).name}
                  value="value"
                  cornerRadius={4}
                  borderWidth={1}
                  borderColor="transparent"
                  colors={CHART_COLORS}
                  childColor={{ from: "color", modifiers: [["brighter", 0.1]] }}
                  enableArcLabels={true}
                  arcLabelsSkipAngle={10}
                  arcLabelsTextColor="white"
                  tooltip={(node) => {
                    const data = node.data as FileNode;
                    const tooltipBg = colorScheme === "dark" ? "var(--mantine-color-dark-7)" : "white";
                    const tooltipBorder = colorScheme === "dark" ? "var(--mantine-color-dark-5)" : "var(--mantine-color-default-border)";
                    return (
                      <Paper
                        p="xs"
                        shadow="md"
                        radius="sm"
                        withBorder
                        style={{ display: "flex", alignItems: "center", gap: 10, backgroundColor: tooltipBg, borderColor: tooltipBorder }}
                      >
                        <div style={{ width: 12, height: 12, backgroundColor: node.color, borderRadius: 2 }} />
                        <div>
                          <Anchor size="xs" fw={700} onClick={() => showInFinder({ ...data, value: node.value as number })}>
                            {node.id}
                          </Anchor>
                          <Text size="xs">{formatBytes(node.value as number)}</Text>
                        </div>
                      </Paper>
                    );
                  }}
                  onClick={(node) => setSelectedNode({ ...(node.data as FileNode), value: node.value as number })}
                />
              </div>
            </Paper>

            {/* Inspector Panel */}
            <Paper withBorder p="md" radius="md" shadow="sm" style={{ flex: 1, minWidth: 260 }}>
              <Stack gap="sm">
                <Text fw={700}>Inspector</Text>
                {!selectedNode && <Text c="dimmed">Wähle ein Segment im Diagramm.</Text>}
                {selectedNode && (
                  <>
                    <Anchor size="sm" fw={600} onClick={() => showInFinder(selectedNode)}>
                      {selectedNode.name}
                    </Anchor>
                    <Text size="sm" c="dimmed">
                      Größe: {formatBytes(selectedNode.value)}
                    </Text>
                    {scanData && scanData.value > 0 && (
                      <Text size="sm" c="dimmed">
                        Anteil: {((selectedNode.value / scanData.value) * 100).toFixed(1)}%
                      </Text>
                    )}
                    {showPowerDataInspector && (
                      <>
                        <Text size="sm" c="dimmed">
                          Dateien: {selectedNode.fileCount.toLocaleString()}
                        </Text>
                        {selectedNode.modifiedAt && (
                          <Text size="sm" c="dimmed">
                            Geändert: {formatDate(selectedNode.modifiedAt)}
                          </Text>
                        )}
                      </>
                    )}
                    <Group gap="xs" mt="sm">
                      <Button variant="light" leftSection={<IconFolder size={16} />} onClick={() => showInFinder(selectedNode)}>
                        Im Finder zeigen
                      </Button>
                      <Button
                        color="red"
                        variant="light"
                        leftSection={<IconTrash size={16} />}
                        onClick={() => {
                          setTrashTarget(selectedNode);
                          openConfirm();
                        }}
                      >
                        In Papierkorb
                      </Button>
                    </Group>
                  </>
                )}
              </Stack>
            </Paper>
          </Group>
        )}
      </AppShell.Main>
    </AppShell>
  );
}
