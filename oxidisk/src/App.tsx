import { useEffect, useState } from "react";
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
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import {
  IconDatabase,
  IconChartPie,
  IconSettings,
  IconRefresh,
  IconDeviceFloppy,
  IconSun,
  IconMoon,
  IconFolder,
  IconTrash,
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
  const [disks, setDisks] = useState<SystemDisk[]>([]);
  const [scanData, setScanData] = useState<FileNode | null>(null);
  const [loading, setLoading] = useState(false);
  const [showPowerDataInspector, setShowPowerDataInspector] = useState(true);
  const [showSystemVolumes, setShowSystemVolumes] = useState(false);
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
    loadDisks();
  }, [showSystemVolumes]);

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
            Deine Laufwerke
          </Text>
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
        </ScrollArea>
      </AppShell.Navbar>

      {/* --- MAIN CONTENT --- */}
      <AppShell.Main style={{ display: "flex", flexDirection: "column", height: "100vh", overflow: "hidden" }}>
        {/* FALL 1: Lädt */}
        {loading && (
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
        {!loading && !scanData && (
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
        {!loading && scanData && currentDisk && (
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
