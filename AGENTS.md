# Codebase Analysis: muse-jsx-redux

## 1. Full Directory Structure
muse-jsx-redux/
├── .github/workflows/
│   ├── ci.yml
│   └── pages.yml
├── demo/                          # Vite React demo app (the UI)
│   ├── index.html
│   └── src/
│       ├── App.tsx                # Root component + useMuse hook (all connection/stream logic)
│       ├── AthenaLogger.tsx       # Raw BLE packet capture UI
│       ├── db.ts                  # IndexedDB persistence layer (idb)
│       ├── EEGRecorder.tsx        # EEG recording/export UI
│       ├── FrequencyBands.tsx     # Frequency band graph component (FFT)
│       ├── index.css              # Global styles (CSS custom properties, glass-panel, grid)
│       └── main.tsx               # React entry + IndexedDB cleanup on load/unload
├── src/                           # Library source (published as muse-jsx npm package)
│   ├── index.ts                   # Public API exports
│   ├── muse.ts                    # MuseClient (Classic Muse BLE)
│   ├── muse-athena.ts             # MuseAthenaClient (Athena/new protocol)
│   ├── muse.spec.ts               # Tests
│   └── lib/
│       ├── athena-parser.ts       # Athena tag-based packet decoder
│       ├── muse-interfaces.ts     # All TypeScript interfaces
│       ├── muse-parse.ts          # EEG/PPG/IMU byte-level decoders
│       ├── muse-utils.ts          # BLE observable helper + encode/decode
│       ├── zip-samples.ts         # EEG multi-channel synchronizer → EEGSample
│       ├── zip-samplesPpg.ts      # PPG multi-channel synchronizer → PPGSample
│       └── *.spec.ts              # Tests
├── dist/                          # Built CJS output
├── dist-esm/                      # Built ESM output
├── docs/
│   ├── ATHENA_SUPPORT.md
│   ├── ATHENA_IMPLEMENTATION_SUMMARY.md
│   ├── MIGRATION_GUIDE.md
│   ├── MY_NOTE.md                 # Dev notes on Athena protocol quirks
│   └── MY_NOTE_jp.md
├── scripts/
├── package.json
├── tsconfig.json / tsconfig.esm.json
└── vite.config.mjs
---
## 2. No Redux — State Management Pattern
There is no Redux in this project. Despite the repo name, state management is entirely React-local using useState, useRef, and custom hooks. Here is the full pattern:
Key state in useMuse hook (App.tsx):
// Connection lifecycle
const [status, setStatus] = useState<ConnectionStatus>('disconnected'); // 'disconnected' | 'connecting' | 'connected'
const [battery, setBattery] = useState<string>('unknown');
const [accelerometer, setAccelerometer] = useState({ x: 0, y: 0, z: 0 });
const [data, setData] = useState<Reading[]>([]); // Epoched EEG window for the graph
// Filter configuration — drives stream re-subscription via BehaviorSubject
const [filterSettings, setFilterSettings] = useState<FilterSettings>({
    notchEnabled: true,    notchFrequency: 60,
    bandpassEnabled: true, bandpassLow: 5, bandpassHigh: 40,
});
// The shared, filtered EEG stream (RxJS Observable) passed to child components
const [filteredStream$, setFilteredStream$] = useState<Observable<EEGSample> | null>(null);
// Refs (mutable, no re-render)
const clientRef = React.useRef<MuseClient | MuseAthenaClient | null>(null);
const subscriptionsRef = React.useRef<Subscription[]>([]);
const filterSettings$ = React.useRef(new BehaviorSubject<FilterSettings>(filterSettings)); // Stream of filter config
In App component root state:
const [mode, setMode] = useState<'muse' | 'athena'>('athena');
const [enableAux, setEnableAux] = useState(false);
const [currentView, setCurrentView] = useState<'graph' | 'logger' | 'recording' | 'bands'>(...); // URL-synced
const [selectedPreset, setSelectedPreset] = useState<AthenaPreset>('p1045');
const [visibleChannels, setVisibleChannels] = useState<boolean[]>(new Array(8).fill(true));
const [yRange, setYRange] = useState(500); // Y-axis µV range slider
const [recordingsCount, setRecordingsCount] = useState(0); // Badge counter
---
## 3. Sensor Data Flow: Source → Graph
The complete pipeline from BLE to pixels:
BLE Hardware
    ↓ (Web Bluetooth GATT notifications)
MuseClient.eegReadings  /  MuseAthenaClient.eegReadings
    → Observable<EEGReading>
    → { index, electrode: 0-7, timestamp, samples: number[12] }
    ↓
zipSamples(eegReadings)
    → Observable<EEGSample>
    → { index, timestamp, data: number[] }  ← all channels zipped per timestamp
    ↓
notchFilter({ nbChannels, cutoffFrequency })        (@neurosity/pipes)
    ↓
bandpassFilter({ nbChannels, cutoffFrequencies, samplingRate })
    ↓
.pipe(share())  ← shared, multicast Observable stored as filteredStream$
    ↓
    ├── [graph view] epoch({ duration:250, interval:25, samplingRate:256 })
    │       → map to Reading[] array → setData(readings) → Recharts <LineChart>
    │
    └── [bands view] useFrequencyBands hook
            ↓
        epoch({ duration:1024, interval:250, samplingRate:256 })
            ↓
        fft({ bins: 64|128|256|512|1024 })
            ↓
        powerByBand({ delta:[0.1,4], theta:[4,7.5], alpha:[7.5,12.5], beta:[12.5,30], gamma:[30,100] })
            ↓
        bandPowers: Record<BandName, number[]>  ← array indexed by channel
            ↓
        Extract selected channel → BandPoint → append to rolling buffer (trimmed to historySeconds)
            ↓
        setBandData([...buffer]) → Recharts <LineChart>
Sensor stream re-subscription is managed via filterSettings$ (a BehaviorSubject) and switchMap: whenever filter settings change, the entire downstream pipeline is torn down and rebuilt.
---
## 4. Key File Paths
Category
BLE client (Classic Muse)
BLE client (Athena/new)
All TypeScript interfaces
EEG byte decoder
Athena packet decoder
EEG channel zipper
PPG channel zipper
Main app + useMuse hook
Frequency band graph
EEG recording/export
Raw packet logger
IndexedDB persistence
Global CSS/design system
Public API exports
---
##5. Data Shape / Interfaces
Raw EEG reading (per-electrode, per BLE notification):
// src/lib/muse-interfaces.ts
interface EEGReading {
    index: number;       // BLE packet sequence number
    electrode: number;   // 0–3 (Classic) or 0–7 (Athena)
    timestamp: number;   // ms since epoch
    samples: number[];   // 12 samples (Classic) or 2 samples (Athena) in µV
}
Zipped EEG sample (all channels, single timestamp):
// src/lib/zip-samples.ts
interface EEGSample {
    index: number;
    timestamp: number;   // ms since epoch
    data: number[];      // [ch0_µV, ch1_µV, ..., chN_µV]
}
Graph display reading (epoched, for LineChart):
// App.tsx (inline)
type Reading = {
    index: number;
    timestamp: number;
    ch0: number; ch1: number; ch2: number; /* ...etc, dynamic keys */ [key: string]: number;
}
Frequency band power point:
// demo/src/FrequencyBands.tsx
type BandPoint = {
    time: number;    // seconds since session start
    delta: number;   // µV² power in delta band (for selected channel)
    theta: number;
    alpha: number;
    beta: number;
    gamma: number;
};
Band ranges configuration:
type BandRanges = {
    delta: [number, number];   // default: [0.1, 4]   Hz
    theta: [number, number];   // default: [4, 7.5]   Hz
    alpha: [number, number];   // default: [7.5, 12.5] Hz
    beta:  [number, number];   // default: [12.5, 30]  Hz
    gamma: [number, number];   // default: [30, 100]   Hz
};
FrequencyBandSettings (full settings object):
type FrequencyBandSettings = {
    bins: number;              // FFT bins: 64|128|256|512|1024, default 256
    epochDuration: number;     // ms, default 1024
    epochInterval: number;     // ms (update rate), default 250
    selectedChannel: number;   // 0-indexed EEG channel to display
    scale: 'log' | 'linear';   // Y-axis scale mode
    bandRanges: BandRanges;
    visibleBands: Record<BandName, boolean>;  // per-band show/hide toggles
    historySeconds: number;    // rolling buffer length, default 30s
};
Athena-specific sensor types:
interface AthenaAccGyroSample { index, timestamp, acc?: XYZ, gyro?: XYZ }
interface AthenaOpticalReading { index, opticalChannel: 0|1|2, timestamp, samples: number[] }
interface AthenaBatteryData { timestamp, values: number[] }
interface RawAthenaPacket { timestamp, uuid, data: Uint8Array }
---
## 6. Frequency Band Graph: How it Renders
Component tree for the "Frequency Bands" view:
<FrequencyBands filteredStream$={...} status={...} channelNames={...}>
    <FrequencyBandControls settings={...} setSettings={...} channelNames={...} />
    //   └── selects (Channel, FFT Bins, Y-Axis Scale)
    //   └── range sliders (Epoch Duration 128–2048ms, Update Interval 50–1000ms, History 5–120s)
    //   └── per-band checkboxes + Hz range number inputs (delta/theta/alpha/beta/gamma)
    <FrequencyBandGraph data={bandData} settings={settings} />
    //   └── <ResponsiveContainer> <LineChart data={displayData}>
    //         <XAxis dataKey="time" tickFormatter={(v) => `${v}s`} />
    //         <YAxis tickFormatter={(v) => formatYAxis(v, scale)} label="log₁₀(µV²) or µV²" />
    //         <Tooltip />
    //         <Legend />
    //         {BAND_NAMES.map(band => visibleBands[band] && <Line dataKey={band} stroke={BAND_COLORS[band]} />)}
Log transform at display time (FrequencyBandGraph):
const displayData = data.map((point) => {
    const transformed: any = { time: point.time.toFixed(1) };
    BAND_NAMES.forEach((band) => {
        const raw = point[band];
        transformed[band] = scale === 'log' ? (raw > 0 ? Math.log10(raw) : -3) : raw;
    });
    return transformed;
});
Band colors:
const BAND_COLORS: Record<BandName, string> = {
    delta: '#FF6B6B',   // red
    theta: '#4ECDC4',   // teal
    alpha: '#45B7D1',   // blue
    beta:  '#96CEB4',   // green
    gamma: '#FFEAA7',   // yellow
};
---
## 7. Existing UI Controls & Patterns
All controls follow the same patterns. Here is the inventory:
Pattern: checkbox toggle (used for notch/bandpass enables, channel visibility, band visibility, aux enable):
<label className="checkbox-wrapper" title="...">
    <input type="checkbox" checked={setting} onChange={(e) => update('key', e.target.checked)} />
    <span>Label Text</span>
</label>
Pattern: select dropdown (used for device mode, preset, channel, FFT bins, Y-scale):
<div className="input-group">
    <label htmlFor="id">Label</label>
    <select id="id" value={setting} onChange={(e) => update('key', e.target.value as Type)}>
        <option value="val">Display</option>
    </select>
</div>
Pattern: range slider with live value display (used for Y-axis range, epoch duration, update interval, history length):
<div className="input-group">
    <label style={{ display: 'flex', justifyContent: 'space-between' }} htmlFor="id">
        <span>Label</span>
        <span style={{ color: 'var(--accent)' }}>{value} unit</span>  {/* live value */}
    </label>
    <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginTop: '4px' }}>
        <span style={{ color: 'var(--text-muted)' }}>min</span>
        <input id="id" type="range" min={...} max={...} step={...} value={...} onChange={...} style={{ flex: 1 }} />
        <span style={{ color: 'var(--text-muted)' }}>max</span>
    </div>
</div>
Pattern: number input (used for Hz range editing per band):
<input type="number" value={settings.bandRanges[band][0]}
    onChange={(e) => updateBandRange(band, 0, Number(e.target.value))}
    style={{ width: '64px', padding: '4px 8px', textAlign: 'center' }}
    min={0} max={200} step={0.5} />
Pattern: tab navigation (main view switcher, URL-synced):
<nav className="tab-nav">
    <button className={`tab-btn ${currentView === 'graph' ? 'active' : ''}`}
        onClick={() => switchView('graph')}>EEG Graph</button>
    {/* ... more tabs */}
</nav>
Settings state update pattern (used in all control components):
// Generic updater — merges one key into immutable prev state
const update = <K extends keyof FrequencyBandSettings>(key: K, value: FrequencyBandSettings[K]) => {
    setSettings((prev) => ({ ...prev, [key]: value }));
};
// Nested updater — for bandRanges[band][idx]
const updateBandRange = (band: BandName, idx: 0 | 1, value: number) => {
    setSettings((prev) => ({
        ...prev,
        bandRanges: { ...prev.bandRanges, [band]: prev.bandRanges[band].map((v, i) => (i === idx ? value : v)) as [number, number] }
    }));
};
---
## 8. Data Processing & Aggregation
Low-level byte decoding (muse-parse.ts):
- decodeUnsigned12BitData — unpacks 3-byte-per-2-sample 12-bit encoding from Classic Muse
- decodeEEGSamples → scales to µV: 0.48828125 * (n - 0x800)
- decodePPGSamples → 24-bit unsigned values
- parseAccelerometer → scales by 0.0000610352 (±2G)
- parseGyroscope → scales by 0.0074768 (±250 dps)
Athena 14-bit EEG decoding (athena-parser.ts):
- parseUintLEValues(block, 14) — LSB-first bit extraction
- Scales to µV: (v - 8192) * 0.0885 (8192 is zero-center for 14-bit)
- 8 channels × 2 samples per BLE packet at 256 Hz
Multi-channel synchronization (zip-samples.ts):
- Buffers per-electrode EEGReading objects until timestamp changes
- Reconstructs per-timestamp EEGSample with data[electrode] = sample[i]
- Handles variable channel counts (4, 5 Classic; 8 Athena) dynamically
Signal processing pipeline (@neurosity/pipes, in App.tsx):
- notchFilter — remove 50/60 Hz power line interference
- bandpassFilter — pass only 5–40 Hz (configurable)
- epoch — window the continuous stream into overlapping time segments
- fft — compute Fast Fourier Transform per epoch
- powerByBand — integrate spectral power within each named frequency band range
IndexedDB persistence (db.ts):
- DB name: MuseEEGLogDB, schema version 2
- Stores: eeg_recordings (metadata), eeg_data (samples keyed by recordingId), athena_packet_sessions, athena_packet_data
- EEG is buffered in 256-sample (1-second) chunks before writing to reduce IDB transaction overhead
- On page load, previous incomplete recordings are automatically closed with an endTime
- Optional auto-clear on page unload (controlled via localStorage)