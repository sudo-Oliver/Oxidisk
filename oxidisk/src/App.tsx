import { useEffect, useState } from "react";
import { listen } from "@tauri-apps/api/event";
import { invoke } from "@tauri-apps/api/core";
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
} from "@tabler/icons-react";
import { ResponsiveSunburst } from "@nivo/sunburst";
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

export default function App() {
  const [opened, { toggle }] = useDisclosure();
  const [activeView, setActiveView] = useState<"analyzer" | "partition">("analyzer");
  const [disks, setDisks] = useState<SystemDisk[]>([]);
  const [scanData, setScanData] = useState<FileNode | null>(null);
  const [loading, setLoading] = useState(false);
  const [showPowerDataInspector, setShowPowerDataInspector] = useState(true);
  const [showSystemVolumes, setShowSystemVolumes] = useState(false);
  const [partitionDevices, setPartitionDevices] = useState<PartitionDevice[]>([]);
  const [partitionLoading, setPartitionLoading] = useState(false);
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
    if (activeView === "partition") {
      loadPartitionDevices();
      loadOperationJournal();
    }
  }, [activeView]);

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
      }
      setProgressOpen(true);
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
            <Paper withBorder p="md" radius="md" shadow="sm" style={{ flex: 2, display: "flex", flexDirection: "column", gap: 12 }}>
              <Group justify="space-between" align="center">
                <div>
                  <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
                    Partition Manager
                  </Text>
                  <Title order={3}>Geraete und Partitionen</Title>
                  <Text size="sm" c="dimmed">
                    macOS: Aktionen laufen ueber privilegierten Helper. Die UI ist der Startpunkt.
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
                  <Button variant="light" onClick={loadPartitionDevices} leftSection={<IconRefresh size={18} />}>
                    Neu laden
                  </Button>
                </Group>
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
                    <ThemeIcon size={64} radius="xl" color="gray" variant="light">
                      <IconDatabase size={32} />
                    </ThemeIcon>
                    <Text c="dimmed">Keine Geraete gefunden.</Text>
                  </Stack>
                </Center>
              )}

              {!partitionLoading && partitionDevices.length > 0 && (
                <Stack gap="sm">
                  {partitionDevices.map((device) => (
                    <Paper key={device.identifier} withBorder p="sm" radius="md">
                      <Group justify="space-between" align="center">
                        <div>
                          <Text fw={700}>{device.identifier}</Text>
                          <Text size="sm" c="dimmed">
                            {formatBytes(device.size)} · {device.internal ? "Intern" : "Extern"} · {device.content}
                          </Text>
                          {device.is_protected && (
                            <Group gap="xs" mt={4}>
                              <Tooltip label={device.protection_reason ?? "SIP geschuetzt"}>
                                <Group gap={4}>
                                  <IconLock size={14} color="var(--mantine-color-red-6)" />
                                  <Text size="xs" c="red">
                                    SIP geschuetzt
                                  </Text>
                                </Group>
                              </Tooltip>
                            </Group>
                          )}
                        </div>
                        <Group gap="xs">
                          <Button
                            size="xs"
                            variant="light"
                            onClick={() => openWipeWizard(device)}
                            disabled={device.is_protected}
                          >
                            Geraet loeschen
                          </Button>
                          <Button
                            size="xs"
                            variant="light"
                            disabled={device.is_protected || deviceFreeBytes(device) === 0}
                            onClick={() => openCreateWizard(device)}
                          >
                            Partition erstellen
                          </Button>
                          <Button
                            size="xs"
                            variant="light"
                            disabled={
                              device.is_protected ||
                              !clipboardPartition ||
                              deviceFreeBytes(device) < (clipboardPartition?.size ?? 0)
                            }
                            onClick={() => openPasteWizard(device)}
                          >
                            Einfuegen
                          </Button>
                          <Button
                            size="xs"
                            variant="light"
                            disabled={device.is_protected}
                            onClick={() => openTableWizard(device)}
                          >
                            Partitionstabelle
                          </Button>
                        </Group>
                      </Group>
                      <Divider my="sm" />
                      <Stack gap="xs">
                        {device.partitions.length === 0 && <Text size="sm" c="dimmed">Keine Partitionen</Text>}
                        {device.partitions.map((part) => (
                          <Group key={part.identifier} justify="space-between" align="center">
                            <div>
                              <Text size="sm" fw={600}>
                                {part.identifier} {part.name ? `· ${part.name}` : ""}
                              </Text>
                              <Text size="xs" c="dimmed">
                                {formatBytes(part.size)} · {part.content}
                                {part.mount_point ? ` · ${part.mount_point}` : ""}
                              </Text>
                              {part.is_protected && (
                                <Group gap="xs" mt={4}>
                                  <Tooltip label={part.protection_reason ?? "SIP geschuetzt"}>
                                    <Group gap={4}>
                                      <IconLock size={12} color="var(--mantine-color-red-6)" />
                                      <Text size="xs" c="red">
                                        SIP geschuetzt
                                      </Text>
                                    </Group>
                                  </Tooltip>
                                </Group>
                              )}
                            </div>
                            <Group gap="xs">
                              <Button
                                size="xs"
                                variant="light"
                                disabled={part.is_protected}
                                onClick={() => copyToClipboard(part)}
                              >
                                Kopieren
                              </Button>
                              <Button
                                size="xs"
                                variant="light"
                                disabled={part.is_protected}
                                onClick={() => openFormatWizard(part)}
                              >
                                Formatieren
                              </Button>
                              <Button
                                size="xs"
                                variant="light"
                                disabled={part.is_protected}
                                onClick={() => openResizeWizard(part)}
                              >
                                Groesse
                              <Button
                                size="xs"
                                variant="light"
                                disabled={
                                  device.is_protected ||
                                  !clipboardPartition ||
                                  deviceFreeBytes(device) < (clipboardPartition?.size ?? 0)
                                }
                                onClick={() => openPasteWizard(device)}
                              >
                                Einfuegen
                              </Button>
                              </Button>
                              <Button
                                size="xs"
                                variant="light"
                                disabled={part.is_protected}
                                onClick={() => openMoveWizard(part)}
                              >
                                Verschieben
                              </Button>
                              <Button
                                size="xs"
                                variant="light"
                                disabled={part.is_protected}
                                onClick={() => openCheckWizard(part)}
                              >
                                Ueberpruefen
                              </Button>
                              <Button
                                size="xs"
                                variant="light"
                                disabled={part.is_protected}
                                onClick={() => openLabelWizard(part)}
                              >
                                Label/UUID
                              </Button>
                              <Button
                                size="xs"
                                variant="light"
                                disabled={part.is_protected}
                                onClick={() => openDeleteWizard(part)}
                              >
                                Loeschen
                              </Button>
                            </Group>
                          </Group>
                        ))}
                      </Stack>
                    </Paper>
                  ))}
                </Stack>
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
                  id="name"
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
