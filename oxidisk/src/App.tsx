import { useEffect, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
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
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import {
  IconDatabase,
  IconChartPie,
  IconSettings,
  IconRefresh,
  IconDeviceFloppy,
  IconFolder,
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
}

interface FileNode {
  name: string;
  value: number;
  children?: FileNode[];
  displaySize: string;
}

// --- HELPER ---
function formatBytes(bytes: number, decimals = 1) {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB", "TB", "PB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(decimals))} ${sizes[i]}`;
}

export default function App() {
  const [opened, { toggle }] = useDisclosure();
  const [disks, setDisks] = useState<SystemDisk[]>([]);
  const [scanData, setScanData] = useState<FileNode | null>(null);
  const [loading, setLoading] = useState(false);

  // State für Navigation
  const [currentDisk, setCurrentDisk] = useState<SystemDisk | null>(null);
  const [pathParts, setPathParts] = useState<string[]>([]);

  async function loadDisks() {
    try {
      const result = await invoke<SystemDisk[]>("get_disks");
      setDisks(result);
    } catch (error) {
      console.error(error);
    }
  }

  async function startScan(disk: SystemDisk) {
    if (loading) return;
    setLoading(true);
    setCurrentDisk(disk);

    // Breadcrumbs initialisieren
    setPathParts([disk.mount_point]);
    setScanData(null);

    try {
      const data = await invoke<FileNode>("scan_directory", { path: disk.mount_point });
      setScanData(data);
    } catch (error) {
      console.error("Scan Fehler:", error);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadDisks();
  }, []);

  // Berechne Nutzung für das Dashboard
  const usagePercent = currentDisk
    ? ((currentDisk.total_space - currentDisk.available_space) / currentDisk.total_space) * 100
    : 0;

  return (
    <AppShell
      header={{ height: 60 }}
      navbar={{ width: 260, breakpoint: "sm", collapsed: { mobile: !opened } }}
      padding="md"
    >
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
          <div style={{ WebkitAppRegion: "no-drag" }}>
            <Button variant="subtle" color="gray" onClick={loadDisks} leftSection={<IconRefresh size={18} />}>
              Neu laden
            </Button>
          </div>
        </Group>
      </AppShell.Header>

      {/* --- SIDEBAR --- */}
      <AppShell.Navbar p="md" style={{ backgroundColor: "#f8f9fa" }}>
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
              description={`${formatBytes(disk.available_space)} frei`}
              active={currentDisk?.mount_point === disk.mount_point}
              onClick={() => startScan(disk)}
              variant="filled"
              color="indigo"
              style={{ borderRadius: 8, marginBottom: 4 }}
            />
          ))}
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
          <Stack h="100%" gap="md">
            {/* 1. Übersichtskarte (Total / Used) */}
            <Paper withBorder p="md" radius="md" shadow="sm">
              <Group justify="space-between">
                <Group>
                  <RingProgress
                    size={70}
                    thickness={8}
                    roundCaps
                    sections={[{ value: usagePercent, color: usagePercent > 90 ? "red" : "indigo" }]}
                    label={
                      <Center>
                        <IconDatabase size={20} style={{ opacity: 0.5 }} />
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
            </Paper>

            {/* 2. Breadcrumbs (Pfad Navigation) */}
            <Paper px="md" py="xs" radius="md" bg="gray.0">
              <Breadcrumbs separator="→" style={{ flexWrap: "wrap" }}>
                {pathParts.map((part, index) => (
                  <Anchor key={index} size="sm" onClick={() => console.log("Navigiere zu", part)}>
                    {part === "/" || part === currentDisk.mount_point ? currentDisk.name : part}
                  </Anchor>
                ))}
              </Breadcrumbs>
            </Paper>

            {/* 3. Das Chart */}
            <div style={{ flex: 1, minHeight: 0, position: "relative", background: "white", borderRadius: 8 }}>
              <ResponsiveSunburst
                data={scanData}
                id="name"
                value="value"
                cornerRadius={4}
                borderWidth={1}
                borderColor="white"
                colors={{ scheme: "nivo" }}
                childColor={{ from: "color", modifiers: [["brighter", 0.1]] }}
                enableArcLabels={true}
                arcLabelsSkipAngle={10}
                arcLabelsTextColor={{ from: "color", modifiers: [["darker", 2]] }}
                tooltip={({ id, value, color }) => (
                  <Paper p="xs" shadow="md" radius="sm" withBorder style={{ display: "flex", alignItems: "center", gap: 10 }}>
                    <div style={{ width: 12, height: 12, backgroundColor: color, borderRadius: 2 }} />
                    <div>
                      <Text size="xs" fw={700}>
                        {id}
                      </Text>
                      <Text size="xs">{formatBytes(value)}</Text>
                    </div>
                  </Paper>
                )}
                onClick={(node) => console.log("Zoom in:", node.id)}
              />
            </div>
          </Stack>
        )}
      </AppShell.Main>
    </AppShell>
  );
}
