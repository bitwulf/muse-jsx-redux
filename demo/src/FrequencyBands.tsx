import React, { useState, useEffect, useRef, useCallback } from 'react';
import { LineChart, Line, XAxis, YAxis, Legend, ResponsiveContainer, Tooltip } from 'recharts';
import { epoch, fft, powerByBand } from '@neurosity/pipes';
import { Observable } from 'rxjs';
import { EEGSample } from 'muse-jsx';

// --- Types ---

export type BandName = 'delta' | 'theta' | 'alpha' | 'beta' | 'gamma';

export type BandPoint = {
    time: number;
    delta: number;
    theta: number;
    alpha: number;
    beta: number;
    gamma: number;
};

export type BandRanges = {
    delta: [number, number];
    theta: [number, number];
    alpha: [number, number];
    beta: [number, number];
    gamma: [number, number];
};

export type FrequencyBandSettings = {
    bins: number;
    epochDuration: number;
    epochInterval: number;
    selectedChannel: number;
    scale: 'log' | 'linear';
    bandRanges: BandRanges;
    visibleBands: Record<BandName, boolean>;
    historySeconds: number;
    averageMode: boolean;
};

// --- Constants ---

export const BAND_COLORS: Record<BandName, string> = {
    delta: '#FF6B6B',
    theta: '#4ECDC4',
    alpha: '#45B7D1',
    beta: '#96CEB4',
    gamma: '#FFEAA7',
};

const BAND_NAMES: BandName[] = ['delta', 'theta', 'alpha', 'beta', 'gamma'];

const DEFAULT_SETTINGS: FrequencyBandSettings = {
    bins: 256,
    epochDuration: 1024,
    epochInterval: 250,
    selectedChannel: 0,
    scale: 'log',
    bandRanges: {
        delta: [0.1, 4],
        theta: [4, 7.5],
        alpha: [7.5, 12.5],
        beta: [12.5, 30],
        gamma: [30, 100],
    },
    visibleBands: {
        delta: true,
        theta: true,
        alpha: true,
        beta: true,
        gamma: true,
    },
    historySeconds: 30,
    averageMode: false,
};

// --- Hook ---

export function useFrequencyBands(filteredStream$: Observable<EEGSample> | null, settings: FrequencyBandSettings) {
    const [bandData, setBandData] = useState<BandPoint[]>([]);
    const bufferRef = useRef<BandPoint[]>([]);
    const startTimeRef = useRef<number>(Date.now());

    // Stable ref so subscription closure always sees latest settings
    const settingsRef = useRef(settings);
    useEffect(() => {
        settingsRef.current = settings;
    }, [settings]);

    useEffect(() => {
        if (!filteredStream$) {
            setBandData([]);
            bufferRef.current = [];
            return;
        }

        bufferRef.current = [];
        startTimeRef.current = Date.now();

        const { bins, epochDuration, epochInterval, bandRanges } = settings;

        const sub = (filteredStream$ as any)
            .pipe(
                epoch({ duration: epochDuration, interval: epochInterval, samplingRate: 256 }) as any,
                fft({ bins }) as any,
                powerByBand(bandRanges) as any,
            )
            .subscribe({
                next: (bandPowers: Record<BandName, number[]>) => {
                    const { selectedChannel, historySeconds: maxHistory, averageMode } = settingsRef.current;

                    // Helper: compute mean of valid (finite, non-null) values across all channels
                    const channelMean = (values: number[]): number => {
                        const valid = (values ?? []).filter((v) => v != null && isFinite(v) && !isNaN(v));
                        if (valid.length === 0) return 0;
                        return valid.reduce((sum, v) => sum + v, 0) / valid.length;
                    };

                    const getValue = (band: BandName): number => {
                        const arr = bandPowers[band];
                        if (!arr || arr.length === 0) return 0;
                        if (averageMode) return channelMean(arr);
                        const v = arr[selectedChannel];
                        return v != null && isFinite(v) ? v : 0;
                    };

                    const point: BandPoint = {
                        time: (Date.now() - startTimeRef.current) / 1000,
                        delta: getValue('delta'),
                        theta: getValue('theta'),
                        alpha: getValue('alpha'),
                        beta: getValue('beta'),
                        gamma: getValue('gamma'),
                    };

                    bufferRef.current = [...bufferRef.current, point];

                    // Trim to historySeconds
                    if (bufferRef.current.length > 0) {
                        const cutoff = point.time - maxHistory;
                        bufferRef.current = bufferRef.current.filter((p) => p.time >= cutoff);
                    }

                    setBandData([...bufferRef.current]);
                },
                error: (err: any) => {
                    console.error('[FrequencyBands] Pipeline error:', err);
                },
            });

        return () => {
            sub.unsubscribe();
            bufferRef.current = [];
        };
        // Re-subscribe when stream or pipeline params change
    }, [
        filteredStream$,
        settings.bins,
        settings.epochDuration,
        settings.epochInterval,
        settings.bandRanges,
        settings.averageMode,
    ]);

    return bandData;
}

// --- FrequencyBandGraph ---

function formatYAxis(value: number, scale: 'log' | 'linear'): string {
    if (scale === 'log') {
        // value is already log10(power)
        return value.toFixed(1);
    }
    if (value >= 1000) return `${(value / 1000).toFixed(1)}k`;
    return value.toFixed(1);
}

export function FrequencyBandGraph({ data, settings }: { data: BandPoint[]; settings: FrequencyBandSettings }) {
    const { scale, visibleBands } = settings;

    // Apply log transform for display if needed
    const displayData = data.map((point) => {
        const transformed: any = { time: point.time.toFixed(1) };
        BAND_NAMES.forEach((band) => {
            const raw = point[band];
            transformed[band] = scale === 'log' ? (raw > 0 ? Math.log10(raw) : -3) : raw;
        });
        return transformed;
    });

    if (data.length === 0) {
        return (
            <div
                style={{
                    height: 400,
                    display: 'flex',
                    justifyContent: 'center',
                    alignItems: 'center',
                    color: '#94a3b8',
                    background: 'rgba(0,0,0,0.2)',
                    borderRadius: '12px',
                    flexDirection: 'column',
                    gap: '8px',
                }}
            >
                <span style={{ fontSize: '1.1rem' }}>No Band Data</span>
                <span style={{ fontSize: '0.85rem' }}>Connect a device to see frequency band power</span>
            </div>
        );
    }

    return (
        <div className="glass-panel" style={{ height: 420, width: '100%', padding: '10px' }}>
            <ResponsiveContainer>
                <LineChart data={displayData} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
                    <XAxis
                        dataKey="time"
                        stroke="#475569"
                        fontSize={11}
                        tickFormatter={(v) => `${v}s`}
                        interval="preserveStartEnd"
                    />
                    <YAxis
                        stroke="#475569"
                        fontSize={11}
                        tickFormatter={(v) => formatYAxis(v, scale)}
                        label={{
                            value: scale === 'log' ? 'log₁₀(µV²)' : 'µV²',
                            angle: -90,
                            position: 'insideLeft',
                            fill: '#475569',
                            fontSize: 11,
                            dx: -2,
                        }}
                    />
                    <Tooltip
                        contentStyle={{
                            background: 'rgba(15,23,42,0.9)',
                            border: '1px solid rgba(255,255,255,0.1)',
                            borderRadius: '8px',
                            fontSize: '0.8rem',
                        }}
                        labelFormatter={(v) => `Time: ${v}s`}
                        formatter={(value: any, name?: string) => [
                            scale === 'log'
                                ? `${Number(value).toFixed(2)} log₁₀(µV²)`
                                : `${Number(value).toFixed(4)} µV²`,
                            name ? name.charAt(0).toUpperCase() + name.slice(1) : '',
                        ]}
                    />
                    <Legend verticalAlign="top" height={36} />
                    {BAND_NAMES.map(
                        (band) =>
                            visibleBands[band] && (
                                <Line
                                    key={band}
                                    type="monotone"
                                    dataKey={band}
                                    name={band.charAt(0).toUpperCase() + band.slice(1)}
                                    stroke={BAND_COLORS[band]}
                                    dot={false}
                                    isAnimationActive={false}
                                    strokeWidth={1.5}
                                />
                            ),
                    )}
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
}

// --- FrequencyBandControls ---

export function FrequencyBandControls({
    settings,
    setSettings,
    channelNames,
}: {
    settings: FrequencyBandSettings;
    setSettings: React.Dispatch<React.SetStateAction<FrequencyBandSettings>>;
    channelNames: string[];
}) {
    const update = useCallback(
        <K extends keyof FrequencyBandSettings>(key: K, value: FrequencyBandSettings[K]) => {
            setSettings((prev) => ({ ...prev, [key]: value }));
        },
        [setSettings],
    );

    const updateBandRange = useCallback(
        (band: BandName, idx: 0 | 1, value: number) => {
            setSettings((prev) => ({
                ...prev,
                bandRanges: {
                    ...prev.bandRanges,
                    [band]: prev.bandRanges[band].map((v, i) => (i === idx ? value : v)) as [number, number],
                },
            }));
        },
        [setSettings],
    );

    const toggleBand = useCallback(
        (band: BandName) => {
            setSettings((prev) => ({
                ...prev,
                visibleBands: { ...prev.visibleBands, [band]: !prev.visibleBands[band] },
            }));
        },
        [setSettings],
    );

    return (
        <div className="glass-panel" style={{ padding: '20px' }}>
            <h3 style={{ marginTop: 0, marginBottom: '16px' }}>Frequency Band Settings</h3>

            <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
                {/* Row 0: Average Mode toggle */}
                <div
                    style={{
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        padding: '12px 16px',
                        borderRadius: '10px',
                        background: settings.averageMode ? 'rgba(99, 102, 241, 0.12)' : 'rgba(255, 255, 255, 0.03)',
                        border: `1px solid ${settings.averageMode ? 'rgba(99, 102, 241, 0.4)' : 'var(--panel-border)'}`,
                        transition: 'all 0.2s ease',
                    }}
                >
                    <div>
                        <div style={{ fontWeight: 600, fontSize: '0.9rem' }}>Average Mode</div>
                        <div style={{ fontSize: '0.78rem', color: 'var(--text-muted)', marginTop: '2px' }}>
                            {settings.averageMode
                                ? 'Showing mean power across all active channels'
                                : 'Showing single-channel band power'}
                        </div>
                    </div>
                    <label className="toggle-switch" aria-label="Toggle Average Mode" title="Toggle Average Mode">
                        <input
                            type="checkbox"
                            checked={settings.averageMode}
                            onChange={(e) => update('averageMode', e.target.checked)}
                        />
                        <span className="toggle-slider" />
                    </label>
                </div>

                {/* Row 1: Channel, Bins, Scale */}
                <div className="grid grid-cols-3" style={{ gap: '16px' }}>
                    <div className="input-group">
                        <label
                            htmlFor="fb-channel"
                            style={{ opacity: settings.averageMode ? 0.45 : 1, transition: 'opacity 0.2s' }}
                        >
                            Channel{settings.averageMode ? ' (disabled in Avg Mode)' : ''}
                        </label>
                        <select
                            id="fb-channel"
                            value={settings.selectedChannel}
                            onChange={(e) => update('selectedChannel', Number(e.target.value))}
                            disabled={settings.averageMode}
                            aria-label="Select EEG channel for band power"
                            style={{ opacity: settings.averageMode ? 0.45 : 1, transition: 'opacity 0.2s' }}
                        >
                            {channelNames.map((name, idx) => (
                                <option key={idx} value={idx}>
                                    {name || `Ch ${idx + 1}`}
                                </option>
                            ))}
                        </select>
                    </div>

                    <div className="input-group">
                        <label htmlFor="fb-bins">FFT Bins</label>
                        <select
                            id="fb-bins"
                            value={settings.bins}
                            onChange={(e) => update('bins', Number(e.target.value))}
                            aria-label="FFT bins count"
                        >
                            {[64, 128, 256, 512, 1024].map((b) => (
                                <option key={b} value={b}>
                                    {b}
                                </option>
                            ))}
                        </select>
                    </div>

                    <div className="input-group">
                        <label htmlFor="fb-scale">Y-Axis Scale</label>
                        <select
                            id="fb-scale"
                            value={settings.scale}
                            onChange={(e) => update('scale', e.target.value as 'log' | 'linear')}
                            aria-label="Y-axis scale type"
                        >
                            <option value="log">Logarithmic</option>
                            <option value="linear">Linear</option>
                        </select>
                    </div>
                </div>

                {/* Row 2: Epoch Duration and Interval sliders */}
                <div className="grid grid-cols-2" style={{ gap: '16px' }}>
                    <div className="input-group">
                        <label style={{ display: 'flex', justifyContent: 'space-between' }} htmlFor="fb-epoch-dur">
                            <span>Epoch Duration</span>
                            <span style={{ color: 'var(--accent)' }}>{settings.epochDuration} ms</span>
                        </label>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginTop: '4px' }}>
                            <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>128</span>
                            <input
                                id="fb-epoch-dur"
                                type="range"
                                min={128}
                                max={2048}
                                step={128}
                                value={settings.epochDuration}
                                onChange={(e) => update('epochDuration', Number(e.target.value))}
                                style={{ flex: 1 }}
                                aria-label="Epoch duration in milliseconds"
                            />
                            <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>2048</span>
                        </div>
                    </div>

                    <div className="input-group">
                        <label style={{ display: 'flex', justifyContent: 'space-between' }} htmlFor="fb-epoch-int">
                            <span>Update Interval</span>
                            <span style={{ color: 'var(--accent)' }}>{settings.epochInterval} ms</span>
                        </label>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginTop: '4px' }}>
                            <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>50</span>
                            <input
                                id="fb-epoch-int"
                                type="range"
                                min={50}
                                max={1000}
                                step={50}
                                value={settings.epochInterval}
                                onChange={(e) => update('epochInterval', Number(e.target.value))}
                                style={{ flex: 1 }}
                                aria-label="Update interval in milliseconds"
                            />
                            <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>1000</span>
                        </div>
                    </div>
                </div>

                {/* Row 3: History length */}
                <div className="input-group" style={{ maxWidth: '400px' }}>
                    <label style={{ display: 'flex', justifyContent: 'space-between' }} htmlFor="fb-history">
                        <span>History Length</span>
                        <span style={{ color: 'var(--accent)' }}>{settings.historySeconds}s</span>
                    </label>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginTop: '4px' }}>
                        <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>5</span>
                        <input
                            id="fb-history"
                            type="range"
                            min={5}
                            max={120}
                            step={5}
                            value={settings.historySeconds}
                            onChange={(e) => update('historySeconds', Number(e.target.value))}
                            style={{ flex: 1 }}
                            aria-label="History length in seconds"
                        />
                        <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>120</span>
                    </div>
                </div>

                {/* Row 4: Band visibility and range editors */}
                <div>
                    <h4
                        style={{
                            margin: '0 0 12px 0',
                            fontSize: '0.9rem',
                            color: 'var(--text-muted)',
                            textTransform: 'uppercase',
                            letterSpacing: '0.05em',
                        }}
                    >
                        Band Ranges (Hz) &amp; Visibility
                    </h4>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                        {BAND_NAMES.map((band) => (
                            <div
                                key={band}
                                className="band-range-row"
                                style={{ display: 'flex', alignItems: 'center', gap: '12px' }}
                            >
                                {/* Visibility toggle */}
                                <label
                                    className="checkbox-wrapper"
                                    style={{
                                        minWidth: '80px',
                                        fontSize: '0.85rem',
                                        color: BAND_COLORS[band],
                                        border: `1px solid ${settings.visibleBands[band] ? BAND_COLORS[band] : 'var(--panel-border)'}`,
                                        padding: '3px 8px',
                                        borderRadius: '6px',
                                        background: settings.visibleBands[band]
                                            ? 'rgba(255,255,255,0.05)'
                                            : 'transparent',
                                        cursor: 'pointer',
                                    }}
                                    title={`Toggle ${band} band visibility`}
                                >
                                    <input
                                        type="checkbox"
                                        checked={settings.visibleBands[band]}
                                        onChange={() => toggleBand(band)}
                                        aria-label={`Toggle ${band} band visibility`}
                                    />
                                    {band.charAt(0).toUpperCase() + band.slice(1)}
                                </label>

                                {/* Range inputs */}
                                <div style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '0.85rem' }}>
                                    <input
                                        type="number"
                                        value={settings.bandRanges[band][0]}
                                        onChange={(e) => updateBandRange(band, 0, Number(e.target.value))}
                                        style={{ width: '64px', padding: '4px 8px', textAlign: 'center' }}
                                        min={0}
                                        max={200}
                                        step={0.5}
                                        aria-label={`${band} band minimum frequency`}
                                        title={`${band} band minimum frequency (Hz)`}
                                    />
                                    <span style={{ color: 'var(--text-muted)' }}>–</span>
                                    <input
                                        type="number"
                                        value={settings.bandRanges[band][1]}
                                        onChange={(e) => updateBandRange(band, 1, Number(e.target.value))}
                                        style={{ width: '64px', padding: '4px 8px', textAlign: 'center' }}
                                        min={0}
                                        max={200}
                                        step={0.5}
                                        aria-label={`${band} band maximum frequency`}
                                        title={`${band} band maximum frequency (Hz)`}
                                    />
                                    <span style={{ color: 'var(--text-muted)', fontSize: '0.8rem' }}>Hz</span>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            </div>
        </div>
    );
}

// --- Main FrequencyBands Component ---

interface FrequencyBandsProps {
    filteredStream$: Observable<EEGSample> | null;
    status: 'disconnected' | 'connecting' | 'connected';
    channelNames: string[];
}

export function FrequencyBands({ filteredStream$, status, channelNames }: FrequencyBandsProps) {
    const [settings, setSettings] = useState<FrequencyBandSettings>({
        ...DEFAULT_SETTINGS,
        selectedChannel: 0,
    });

    const bandData = useFrequencyBands(status === 'connected' ? filteredStream$ : null, settings);

    return (
        <div className="animate-fade-in" style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
            <FrequencyBandControls settings={settings} setSettings={setSettings} channelNames={channelNames} />
            <div className="glass-panel" style={{ padding: '24px' }}>
                <div
                    style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center',
                        marginBottom: '16px',
                    }}
                >
                    <h3 style={{ margin: 0 }}>Frequency Band Power</h3>
                    <div
                        style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: '12px',
                            fontSize: '0.85rem',
                            color: 'var(--text-muted)',
                            flexWrap: 'wrap',
                        }}
                    >
                        {settings.averageMode ? (
                            <span
                                style={{
                                    display: 'inline-flex',
                                    alignItems: 'center',
                                    gap: '5px',
                                    color: '#a5b4fc',
                                    background: 'rgba(99,102,241,0.15)',
                                    border: '1px solid rgba(99,102,241,0.35)',
                                    borderRadius: '6px',
                                    padding: '2px 8px',
                                    fontSize: '0.8rem',
                                    fontWeight: 600,
                                }}
                            >
                                Avg across all channels
                            </span>
                        ) : (
                            <span>
                                Channel:{' '}
                                <span style={{ color: 'var(--accent)' }}>
                                    {channelNames[settings.selectedChannel] || `Ch ${settings.selectedChannel + 1}`}
                                </span>
                            </span>
                        )}
                        <span style={{ color: 'var(--panel-border)' }}>·</span>
                        <span>{bandData.length} points</span>
                    </div>
                </div>
                <FrequencyBandGraph data={bandData} settings={settings} />
            </div>
        </div>
    );
}
