#!/usr/bin/env python3
"""
bandwidth_analyzer.py - threshold sweep experiment for the Edge Anomaly Stream Filter.

For every sample of the unfiltered stream it runs six parallel copies of the exact edge filter
used by the ESP32 firmware (ring buffer N = 50 floats, two-pass mean / sample sigma in double,
zero-division guard sigma < 0.0001, |Z| >= threshold -> publish) for
sigma thresholds {1.0, 1.5, 2.0, 2.5, 3.0, 3.5} and counts the MQTT bytes each copy would send.

Modes
-----
live (default)   subscribe to <prefix>/raw and <prefix>/truth for --duration seconds
                 (start this first, then sensor_emulator.py).
--offline        generate the stream in-process with the emulator's generator
                 (same --seed => exactly the same samples), no broker needed.
                 Several seeds (--seeds 1 2 ... 10) => mean +/- std per threshold.
--live-topic     only measure the bytes really published by a node (firmware / Wokwi) on
                 <prefix>/telemetry; heartbeats on <prefix>/status are not counted.

Bytes per message = size of the MQTT 3.1.1 PUBLISH packet at QoS 0:
1 + len(remaining-length varint) + 2 + len(topic) + len(payload).

Outputs (paths relative to the repository root)
    results/threshold_sweep.csv / .json            (live run, or offline single seed)
    results/samples_<tag>.csv                      (per-sample log: v, mu, sd, z, truth)
    results/threshold_sweep_seeds.csv / .json      (offline multi-seed statistics)
    docs/photos/bandwidth_reduction_chart.png      (bars = reduction %, line = FNR %)
    docs/photos/signal_trace.png                   (--trace: signal with mu +/- k*sigma band)
"""

from __future__ import annotations

import argparse
from collections import deque
import csv
import json
import logging
import math
import queue
import signal
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import numpy as np

if not __package__:
    _REPO_ROOT = str(Path(__file__).resolve().parents[2])
    if _REPO_ROOT not in sys.path:
        sys.path.insert(0, _REPO_ROOT)

from src.analytical_tools.sensor_emulator import (  # noqa: E402
    NODE_ID,
    SAMPLE_PERIOD_MS,
    SAMPLE_RATE_HZ,
    TOPIC_PREFIX,
    SignalModel,
    add_model_arguments,
    format_raw_payload,
    generate_samples,
    model_from_args,
    mqtt_publish_size,
    raw_to_volts,
    topic,
)

LOG = logging.getLogger("bandwidth_analyzer")

REPO_ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = REPO_ROOT / "results"
PHOTOS_DIR = REPO_ROOT / "docs" / "photos"

BUFFER_SIZE = 50
SIGMA_EPSILON = 1e-4
THRESHOLDS = (1.0, 1.5, 2.0, 2.5, 3.0, 3.5)
DEFAULT_THRESHOLD = 2.5
REBASELINE_RUN = 10
POLICY_GATED = "gated"
POLICY_ALL = "all"
POLICIES = (POLICY_GATED, POLICY_ALL)
TARGET_REDUCTION_PCT = 40.0

STATE_WARMUP = "WARMUP"
STATE_ANOMALY = "ANOMALY"
STATE_NOMINAL = "NOMINAL"


def format_telemetry(seq: int, t_ms: int, raw: int, v: float, mu: float, sd: float, z: float,
                     state: str, node_id: str = NODE_ID) -> str:
    """
    Telemetry JSON byte-identical to the firmware: fixed key order, compact separators,
    v/mu/sd with 3 decimals and z with 2 decimals (trailing zeros kept, like "%.3f").
    """
    return (f'{{"node":"{node_id}","seq":{seq},"t_ms":{t_ms},"raw":{raw},'
            f'"v":{v:.3f},"mu":{mu:.3f},"sd":{sd:.3f},"z":{z:.2f},"state":"{state}"}}')


@dataclass
class FilterDecision:
    """What the edge filter did with one sample."""

    state: str
    publish: bool
    mu: float
    sd: float
    z: float
    pushed: bool = True


class EdgeFilter:
    """
    Python twin of the firmware filter (src/edge_node/main.cpp), numerically identical.

    * ring buffer of N 32-bit floats (the ESP32 stores `float`), head index + count
    * warm-up: while count < N, every sample is published with state WARMUP (mu = sd = z = 0)
    * afterwards: two-pass mean and sample sigma (divide by N-1) over the N previous samples,
      walked oldest -> newest and accumulated in double precision exactly like computeStats();
      sigma < 1e-4 -> Z = 0 (zero-division guard)
    * Z = (x - mu) / sigma with x the float32-rounded sample; |Z| >= threshold -> publish
    * buffer update policy:
        "gated" (v2 default, GATED_BUFFER_UPDATE = true): a nominal sample is pushed and resets
        anomaly_run to 0; an anomaly increments anomaly_run and is NOT pushed (a spike must not
        inflate mu / sigma), unless anomaly_run >= REBASELINE_RUN (10 consecutive anomalies =
        1 s) - then it is pushed so a sustained level shift becomes the new baseline. The
        zero-sigma case (Z = 0) counts as nominal.
        "all" (v1 / ablation): every sample is pushed, anomalies included.
    """

    def __init__(self, threshold: float, policy: str = POLICY_GATED,
                 rebaseline_run: int = REBASELINE_RUN, size: int = BUFFER_SIZE) -> None:
        if policy not in POLICIES:
            raise ValueError(f"unknown buffer policy {policy!r}")
        if size < 1:
            raise ValueError(f"buffer size must be >= 1, got {size}")
        if rebaseline_run < 1:
            raise ValueError(f"rebaseline_run must be >= 1, got {rebaseline_run}")
        self.threshold = float(np.float32(threshold))
        self.policy = policy
        self.rebaseline_run = rebaseline_run
        self.anomaly_run = 0
        self.size = size
        self.buf = [0.0] * size
        self.head = 0
        self.count = 0
        self.zero_sigma_events = 0

    @staticmethod
    def _as_float32(value: float) -> float:
        """Round a double to the nearest 32-bit float, like a C `(float)` cast."""
        return float(np.float32(value))

    def _push(self, x: float) -> None:
        self.buf[self.head] = x
        self.head = (self.head + 1) % self.size
        if self.count < self.size:
            self.count += 1

    def _window(self) -> List[float]:
        """Buffer contents from the oldest to the newest value (RingBuffer::at order)."""
        oldest = (self.head + self.size - self.count) % self.size
        return [self.buf[(oldest + i) % self.size] for i in range(self.count)]

    def _stats(self) -> tuple:
        """Two-pass mean / sample sigma in double precision (no catastrophic cancellation)."""
        window = self._window()
        if not window:
            return 0.0, 0.0
        total = 0.0
        for value in window:
            total += value
        mean = total / len(window)
        if len(window) < 2:
            return mean, 0.0
        sum_sq = 0.0
        for value in window:
            d = value - mean
            sum_sq += d * d
        return mean, math.sqrt(sum_sq / (len(window) - 1))

    def process(self, v: float) -> FilterDecision:
        if math.isnan(v) or math.isinf(v):
            return FilterDecision(STATE_NOMINAL, False, 0.0, 0.0, 0.0, False)
        x = self._as_float32(v)
        if self.count < self.size:
            self._push(x)
            return FilterDecision(STATE_WARMUP, True, 0.0, 0.0, 0.0)
        mean, sd = self._stats()
        if sd < SIGMA_EPSILON:
            z = 0.0
            self.zero_sigma_events += 1
        else:
            z = (x - mean) / sd
        if abs(z) >= self.threshold:
            self.anomaly_run += 1
            push = (self.policy == POLICY_ALL or self.anomaly_run >= self.rebaseline_run)
            if push:
                self._push(x)
            return FilterDecision(STATE_ANOMALY, True, mean, sd, z, push)
        self.anomaly_run = 0
        self._push(x)
        return FilterDecision(STATE_NOMINAL, False, mean, sd, z, True)


@dataclass
class ThresholdStats:
    """Counters for one filter copy."""

    threshold: float
    messages: int = 0
    bytes: int = 0
    warmup_messages: int = 0
    anomaly_messages: int = 0
    detected: int = 0
    false_positives: int = 0
    alert_seqs: List[int] = field(default_factory=list)


class Experiment:
    """Feeds samples through the six filter copies and the baseline, collects metrics."""

    def __init__(self, thresholds: Sequence[float] = THRESHOLDS,
                 prefix: str = TOPIC_PREFIX, node_id: str = NODE_ID,
                 policy: str = POLICY_GATED) -> None:
        self.policy = policy
        self.telemetry_topic = topic(prefix, "telemetry")
        self.raw_topic = topic(prefix, "raw")
        self.node_id = node_id
        self.filters = {thr: EdgeFilter(thr, policy) for thr in thresholds}
        self.stats = {thr: ThresholdStats(thr) for thr in thresholds}
        self.reference = EdgeFilter(DEFAULT_THRESHOLD, policy)
        self.samples = 0
        self.baseline_messages = 0
        self.baseline_bytes = 0
        self.raw_stream_bytes = 0
        self.spike_seqs: set = set()
        self.warmup_spike_seqs: set = set()
        self.first_seq: Optional[int] = None
        self.last_seq: Optional[int] = None
        self.missing_seqs = 0
        self.rows: List[dict] = []

    def add(self, seq: int, t_ms: int, raw: int, v: float, is_spike: bool,
            raw_payload_bytes: Optional[int] = None) -> None:
        if self.first_seq is None:
            self.first_seq = seq
        elif self.last_seq is not None and seq != self.last_seq + 1:
            self.missing_seqs += max(0, seq - self.last_seq - 1)
            LOG.warning("Sequence gap: expected %d, got %d", self.last_seq + 1, seq)
        self.last_seq = seq
        self.samples += 1
        warmup = self.samples <= BUFFER_SIZE
        if is_spike:
            (self.warmup_spike_seqs if warmup else self.spike_seqs).add(seq)

        ref = self.reference.process(v)
        base_state = STATE_WARMUP if ref.state == STATE_WARMUP else STATE_NOMINAL
        base_payload = format_telemetry(seq, t_ms, raw, v, ref.mu, ref.sd, ref.z, base_state,
                                        self.node_id)
        self.baseline_messages += 1
        self.baseline_bytes += mqtt_publish_size(self.telemetry_topic, base_payload)
        if raw_payload_bytes is not None:
            self.raw_stream_bytes += raw_payload_bytes

        for thr, filt in self.filters.items():
            dec = filt.process(v)
            if not dec.publish:
                continue
            st = self.stats[thr]
            payload = format_telemetry(seq, t_ms, raw, v, dec.mu, dec.sd, dec.z, dec.state,
                                       self.node_id)
            st.messages += 1
            st.bytes += mqtt_publish_size(self.telemetry_topic, payload)
            if dec.state == STATE_WARMUP:
                st.warmup_messages += 1
            else:
                st.anomaly_messages += 1
                st.alert_seqs.append(seq)
                if is_spike:
                    st.detected += 1
                else:
                    st.false_positives += 1

        self.rows.append({"seq": seq, "t_ms": t_ms, "raw": raw, "v": round(v, 4),
                          "mu": round(ref.mu, 4), "sd": round(ref.sd, 4),
                          "z": round(ref.z, 3), "is_spike": int(is_spike),
                          "alert_2_5": int(ref.state == STATE_ANOMALY),
                          "pushed_2_5": int(ref.pushed)})

    def results(self) -> List[dict]:
        """One row of metrics per threshold."""
        critical = len(self.spike_seqs)
        out = []
        for thr, st in self.stats.items():
            missed = critical - st.detected
            alerts = st.detected + st.false_positives
            out.append({
                "policy": self.policy,
                "threshold_sigma": thr,
                "samples": self.samples,
                "baseline_messages": self.baseline_messages,
                "baseline_bytes": self.baseline_bytes,
                "messages_sent": st.messages,
                "warmup_messages": st.warmup_messages,
                "anomaly_messages": st.anomaly_messages,
                "bytes_sent": st.bytes,
                "reduction_pct": (
                    round(100.0 * (1 - st.bytes / self.baseline_bytes), 2)
                    if self.baseline_bytes else 0.0),
                "reduction_vs_raw_stream_pct": (
                    round(100.0 * (1 - st.bytes / self.raw_stream_bytes), 2)
                    if self.raw_stream_bytes else None),
                "critical_events": critical,
                "warmup_spikes_sent_raw": len(self.warmup_spike_seqs),
                "detected": st.detected,
                "missed": missed,
                "fnr_pct": round(100.0 * missed / critical, 2) if critical else 0.0,
                "false_positives": st.false_positives,
                "precision_pct": round(100.0 * st.detected / alerts, 2) if alerts else None,
            })
        return out


def write_csv(path: Path, rows: List[dict]) -> None:
    if not rows:
        raise ValueError(f"Cannot write empty CSV: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    LOG.info("Wrote %s", path)


def write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2) + "\n", encoding="utf-8")
    LOG.info("Wrote %s", path)


def print_table(rows: List[dict]) -> None:
    header = (f"{'sigma':>5} {'msgs':>6} {'bytes':>8} {'reduct%':>8} {'crit':>5} "
              f"{'det':>4} {'miss':>4} {'FNR%':>6} {'FP':>4} {'prec%':>6}")
    print(header)
    print("-" * len(header))
    for r in rows:
        prec = "-" if r["precision_pct"] is None else f"{r['precision_pct']:.1f}"
        print(f"{r['threshold_sigma']:>5.1f} {r['messages_sent']:>6} {r['bytes_sent']:>8} "
              f"{r['reduction_pct']:>8.2f} {r['critical_events']:>5} {r['detected']:>4} "
              f"{r['missed']:>4} {r['fnr_pct']:>6.2f} {r['false_positives']:>4} {prec:>6}")
    if rows:
        print(f"baseline: {rows[0]['baseline_messages']} messages, "
              f"{rows[0]['baseline_bytes']} bytes")


def plot_sweep(path: Path, thresholds: Sequence[float], reduction: Sequence[float],
               fnr: Sequence[float], title: str,
               reduction_err: Optional[Sequence[float]] = None,
               fnr_err: Optional[Sequence[float]] = None) -> None:
    """Bars = bandwidth reduction %, line on the secondary axis = FNR %."""
    try:
        import matplotlib  # type: ignore[import-not-found]
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt  # type: ignore[import-not-found]
    except ImportError:
        LOG.warning("matplotlib not installed; skipping chart generation for %s", path)
        return

    labels = [f"{t:.1f}" for t in thresholds]
    x = np.arange(len(thresholds))
    colors = ["#1f77b4" if abs(t - DEFAULT_THRESHOLD) > 1e-9 else "#d62728" for t in thresholds]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(x, reduction, width=0.6, color=colors, edgecolor="black", linewidth=0.6,
                  yerr=reduction_err, capsize=4 if reduction_err is not None else 0,
                  label="Bandwidth reduction vs. unfiltered baseline")
    errs = reduction_err if reduction_err is not None else [0.0] * len(reduction)
    for rect, val, err in zip(bars, reduction, errs):
        ax.annotate(f"{val:.1f}%", (rect.get_x() + rect.get_width() / 2, rect.get_height() + err),
                    xytext=(0, 6), textcoords="offset points",
                    ha="center", va="bottom", fontsize=9, fontweight="bold")
    ax.axhline(TARGET_REDUCTION_PCT, color="green", linestyle="--", linewidth=1.5,
               label=f"Target: >{TARGET_REDUCTION_PCT:.0f}% reduction")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_xlabel("Z-score threshold (number of standard deviations, σ)")
    ax.set_ylabel("Bandwidth reduction (%)")
    top_bar = max((v + e for v, e in zip(reduction, errs)), default=0.0)
    ax.set_ylim(0, max(115.0, top_bar + 12.0))
    ax.grid(axis="y", alpha=0.3)

    ax2 = ax.twinx()
    ax2.errorbar(x, fnr, yerr=fnr_err, color="#ff7f0e", marker="o", linewidth=2,
                 capsize=4 if fnr_err is not None else 0,
                 label="False negative rate (missed critical events)")
    ax2.set_ylabel("False negative rate, FNR (%)", color="#ff7f0e")
    ax2.tick_params(axis="y", colors="#ff7f0e")
    ax2.set_ylim(0, max(10.0, max(fnr) * 1.3 + (max(fnr_err) if fnr_err else 0)))

    idx = [i for i, t in enumerate(thresholds) if abs(t - DEFAULT_THRESHOLD) < 1e-9]
    if idx:
        i = idx[0]
        ax.annotate("default σ = 2.5", (x[i], reduction[i] / 2), ha="center", va="center",
                    color="white", fontsize=9, fontweight="bold", rotation=90)

    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="upper center", bbox_to_anchor=(0.5, -0.13), ncol=3,
              fontsize=8.5, frameon=False)
    ax.set_title(title, fontsize=11)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=200)
    plt.close(fig)
    LOG.info("Wrote %s", path)


def plot_trace(path: Path, seed: int, model_args: argparse.Namespace, start_s: float,
               length_s: float, threshold: float, policy: str = POLICY_GATED) -> None:
    """Signal trace with the filter's mu +/- k*sigma band and the detected / missed spikes."""
    try:
        import matplotlib  # type: ignore[import-not-found]
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt  # type: ignore[import-not-found]
    except ImportError:
        LOG.warning("matplotlib not installed; skipping trace chart generation for %s", path)
        return

    if start_s < 0 or length_s <= 0:
        raise ValueError(
            f"trace window must have start >= 0 and length > 0 (got {start_s}, {length_s})")
    n_total = int(round((start_s + length_s) * SAMPLE_RATE_HZ))
    filt = EdgeFilter(threshold, policy)
    t, v, mu, sd, spikes, alerts = [], [], [], [], [], []
    for s in generate_samples(seed, model_from_args(model_args), n_total):
        dec = filt.process(s.v)
        if s.t_ms / 1000.0 < start_s:
            continue
        t.append(s.t_ms / 1000.0)
        v.append(s.v)
        mu.append(dec.mu if dec.state != STATE_WARMUP else np.nan)
        sd.append(dec.sd if dec.state != STATE_WARMUP else np.nan)
        spikes.append(s.is_spike)
        alerts.append(dec.state == STATE_ANOMALY)
    t, v, mu, sd = map(np.asarray, (t, v, mu, sd))
    spikes, alerts = np.asarray(spikes), np.asarray(alerts)

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(t, v, color="#1f77b4", linewidth=0.9, label="ADC voltage v (V)")
    ax.plot(t, mu, color="black", linewidth=1.2, label="Window mean μ (N = 50)")
    ax.fill_between(t, mu - threshold * sd, mu + threshold * sd, color="gray", alpha=0.25,
                    label=f"μ ± {threshold:g}σ (drop zone)")
    tp, fn, fp = alerts & spikes, spikes & ~alerts, alerts & ~spikes
    ax.scatter(t[tp], v[tp], s=60, facecolors="none", edgecolors="red", linewidths=1.6,
               label=f"Spike detected & sent ({tp.sum()})", zorder=3)
    ax.scatter(t[fn], v[fn], s=60, marker="x", color="purple", linewidths=1.8,
               label=f"Spike missed ({fn.sum()})", zorder=3)
    ax.scatter(t[fp], v[fp], s=50, marker="^", color="orange",
               label=f"False alarm ({fp.sum()})", zorder=3)
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Voltage at GPIO36 (V)")
    ax.set_ylim(-0.1, 3.4)
    ax.set_title(f"Emulated sensor signal (seed {seed}), {length_s:g} s after warm-up, "
                 f"threshold |Z| ≥ {threshold:g}, {policy} buffer update", fontsize=11)
    ax.grid(alpha=0.3)
    ax.legend(loc="upper right", fontsize=8, ncol=2, framealpha=0.95)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=200)
    plt.close(fig)
    LOG.info("Wrote %s", path)


def save_run(exp: Experiment, args: argparse.Namespace, mode: str, seed: Optional[int],
             extra: dict) -> List[dict]:
    rows = exp.results()
    print_table(rows)
    stem = args.name or "threshold_sweep"
    write_csv(RESULTS_DIR / f"{stem}.csv", rows)
    write_csv(RESULTS_DIR / f"samples_{stem}.csv", exp.rows)
    meta = {"mode": mode, "policy": exp.policy, "rebaseline_run": REBASELINE_RUN,
            "seed": seed, "duration_s": args.duration,
            "samples": exp.samples, "first_seq": exp.first_seq, "last_seq": exp.last_seq,
            "missing_seqs": exp.missing_seqs, "buffer_size": BUFFER_SIZE,
            "sample_period_ms": SAMPLE_PERIOD_MS,
            "raw_stream_bytes": exp.raw_stream_bytes,
            "zero_sigma_events": {str(t): f.zero_sigma_events for t, f in exp.filters.items()},
            "alert_seqs": {str(t): s.alert_seqs for t, s in exp.stats.items()},
            "spike_seqs": sorted(exp.spike_seqs),
            "warmup_spike_seqs": sorted(exp.warmup_spike_seqs)}
    meta.update(extra)
    write_json(RESULTS_DIR / f"{stem}.json", {"meta": meta, "results": rows})
    if not args.no_chart:
        chart = Path(args.chart) if args.chart else PHOTOS_DIR / "bandwidth_reduction_chart.png"
        plot_sweep(chart, [r["threshold_sigma"] for r in rows],
                   [r["reduction_pct"] for r in rows], [r["fnr_pct"] for r in rows],
                   f"Edge Z-score filter ({exp.policy} buffer): bandwidth vs. missed events "
                   f"({mode}, seed {seed}, {exp.samples} samples)")
    return rows


def run_offline_single(seed: int, args: argparse.Namespace, n_samples: int) -> Experiment:
    exp = Experiment(prefix=args.topic_prefix, node_id=args.node, policy=args.policy)
    raw_topic = topic(args.topic_prefix, "raw")
    for s in generate_samples(seed, model_from_args(args), n_samples):
        exp.add(s.seq, s.t_ms, s.raw, s.v, s.is_spike,
                mqtt_publish_size(raw_topic, format_raw_payload(s, args.node)))
    return exp


def seed_statistics(args: argparse.Namespace, seeds: Sequence[int], n_samples: int,
                    policy: str) -> tuple:
    """Run several seeds offline; return (per-seed rows, mean/std summary per threshold)."""
    saved_policy = args.policy
    args.policy = policy
    per_seed: List[dict] = []
    for seed in seeds:
        exp = run_offline_single(seed, args, n_samples)
        for row in exp.results():
            per_seed.append({"seed": seed, **row})
        LOG.info("policy=%s seed %d: %d samples, %d critical events", policy, seed,
                 exp.samples, len(exp.spike_seqs))
    args.policy = saved_policy

    summary: List[dict] = []
    metrics = ("messages_sent", "bytes_sent", "reduction_pct", "critical_events", "detected",
               "missed", "fnr_pct", "false_positives", "precision_pct")
    for thr in THRESHOLDS:
        rows = [r for r in per_seed if r["threshold_sigma"] == thr]
        out = {"policy": policy, "threshold_sigma": thr, "seeds": len(rows)}
        for m in metrics:
            vals = [r[m] for r in rows if r[m] is not None]
            out[f"{m}_mean"] = round(statistics.mean(vals), 3) if vals else None
            out[f"{m}_std"] = round(statistics.stdev(vals), 3) if len(vals) > 1 else 0.0
        out["reduction_pct_min"] = min(r["reduction_pct"] for r in rows)
        out["fnr_pct_max"] = max(r["fnr_pct"] for r in rows)
        out["seeds_with_zero_missed"] = sum(1 for r in rows if r["missed"] == 0)
        out["total_missed"] = sum(r["missed"] for r in rows)
        out["total_false_positives"] = sum(r["false_positives"] for r in rows)
        out["total_critical_events"] = sum(r["critical_events"] for r in rows)
        summary.append(out)
    return per_seed, summary


def print_seed_summary(summary: List[dict], n_seeds: int) -> None:
    print(f"policy={summary[0]['policy']}")
    print(f"{'sigma':>5} {'reduction % (mean±std)':>24} {'FNR % (mean±std)':>18} "
          f"{'FP (mean)':>9} {'missed/total':>13} {'0-miss seeds':>12}")
    for s in summary:
        print(f"{s['threshold_sigma']:>5.1f} {s['reduction_pct_mean']:>14.2f} ± "
              f"{s['reduction_pct_std']:<6.2f} {s['fnr_pct_mean']:>9.2f} ± {s['fnr_pct_std']:<5.2f}"
              f" {s['false_positives_mean']:>9.1f} {s['total_missed']:>6}/"
              f"{s['total_critical_events']:<6} {s['seeds_with_zero_missed']:>7}/{n_seeds}")


def plot_policy_comparison(path: Path, gated: List[dict], push_all: List[dict],
                           title: str) -> None:
    """Grouped bars (reduction %) + FNR lines for the two buffer policies (seed means)."""
    try:
        import matplotlib  # type: ignore[import-not-found]
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt  # type: ignore[import-not-found]
    except ImportError:
        LOG.warning("matplotlib not installed; skipping policy comparison chart for %s", path)
        return

    thr = [r["threshold_sigma"] for r in gated]
    x = np.arange(len(thr))
    w = 0.38
    fig, ax = plt.subplots(figsize=(10, 6))
    for off, rows, color, label in ((-w / 2, push_all, "#9ecae1", "v1 push-all"),
                                    (w / 2, gated, "#1f77b4", "v2 gated (default)")):
        bars = ax.bar(x + off, [r["reduction_pct_mean"] for r in rows], width=w, color=color,
                      edgecolor="black", linewidth=0.6,
                      yerr=[r["reduction_pct_std"] for r in rows], capsize=3,
                      label=f"Reduction, {label}")
        for rect, r in zip(bars, rows):
            ax.annotate(f"{r['reduction_pct_mean']:.1f}",
                        (rect.get_x() + rect.get_width() / 2,
                         rect.get_height() + r["reduction_pct_std"]),
                        xytext=(0, 4), textcoords="offset points",
                        ha="center", va="bottom", fontsize=7.5)
    ax.axhline(TARGET_REDUCTION_PCT, color="green", linestyle="--", linewidth=1.5,
               label=f"Target: >{TARGET_REDUCTION_PCT:.0f}% reduction")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{t:.1f}" + (" (default)" if t == DEFAULT_THRESHOLD else "")
                        for t in thr])
    ax.set_xlabel("Z-score threshold (number of standard deviations, σ)")
    ax.set_ylabel("Bandwidth reduction (%)")
    ax.set_ylim(0, 115)
    ax.grid(axis="y", alpha=0.3)

    ax2 = ax.twinx()
    ax2.plot(x, [r["fnr_pct_mean"] for r in push_all], color="#ff7f0e", marker="s",
             linestyle="--", linewidth=2, label="FNR, v1 push-all")
    ax2.plot(x, [r["fnr_pct_mean"] for r in gated], color="#d62728", marker="o",
             linewidth=2, label="FNR, v2 gated")
    ax2.set_ylabel("False negative rate, FNR (%)", color="#d62728")
    ax2.tick_params(axis="y", colors="#d62728")
    top = max(max(r["fnr_pct_mean"] for r in push_all), max(r["fnr_pct_mean"] for r in gated))
    ax2.set_ylim(0, max(10.0, top * 1.3))
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="upper center", bbox_to_anchor=(0.5, -0.13), ncol=3,
              fontsize=8.5, frameon=False)
    ax.set_title(title, fontsize=11)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=200)
    plt.close(fig)
    LOG.info("Wrote %s", path)


def run_offline(args: argparse.Namespace) -> int:
    n_samples = args.samples if args.samples else int(round(args.duration * SAMPLE_RATE_HZ))
    if n_samples <= 0:
        LOG.error("Number of samples must be positive (got %d)", n_samples)
        return 2
    seeds = args.seeds or [args.seed]
    if args.compare_policies:
        both = {}
        for policy in (POLICY_GATED, POLICY_ALL):
            _, both[policy] = seed_statistics(args, seeds, n_samples, policy)
            print_seed_summary(both[policy], len(seeds))
        rows = []
        for g, a in zip(both[POLICY_GATED], both[POLICY_ALL]):
            rows.append({"threshold_sigma": g["threshold_sigma"], "seeds": len(seeds),
                         "reduction_pct_gated": g["reduction_pct_mean"],
                         "reduction_pct_push_all": a["reduction_pct_mean"],
                         "fnr_pct_gated": g["fnr_pct_mean"], "fnr_pct_push_all": a["fnr_pct_mean"],
                         "missed_gated": g["total_missed"], "missed_push_all": a["total_missed"],
                         "false_positives_gated": g["total_false_positives"],
                         "false_positives_push_all": a["total_false_positives"],
                         "critical_events": g["total_critical_events"]})
        stem = args.name or "policy_comparison"
        write_csv(RESULTS_DIR / f"{stem}.csv", rows)
        write_json(RESULTS_DIR / f"{stem}.json",
                   {"meta": {"mode": "offline", "seeds": seeds, "samples_per_seed": n_samples},
                    "gated": both[POLICY_GATED], "push_all": both[POLICY_ALL]})
        if not args.no_chart:
            plot_policy_comparison(
                Path(args.chart) if args.chart else PHOTOS_DIR / "policy_comparison.png",
                both[POLICY_GATED], both[POLICY_ALL],
                f"Buffer update policy ablation: seeds {seeds[0]}..{seeds[-1]}, "
                f"{n_samples} samples each (mean ± std)")
        return 0

    if len(seeds) == 1:
        exp = run_offline_single(seeds[0], args, n_samples)
        save_run(exp, args, "offline", seeds[0], {})
        return 0

    per_seed, summary = seed_statistics(args, seeds, n_samples, args.policy)
    stem = args.name or "threshold_sweep_seeds"
    write_csv(RESULTS_DIR / f"{stem}_per_seed.csv", per_seed)
    write_csv(RESULTS_DIR / f"{stem}.csv", summary)
    write_json(RESULTS_DIR / f"{stem}.json",
               {"meta": {"mode": "offline", "policy": args.policy, "seeds": seeds,
                         "samples_per_seed": n_samples},
                "summary": summary})
    print_seed_summary(summary, len(seeds))

    if not args.no_chart:
        chart = (Path(args.chart) if args.chart
                 else PHOTOS_DIR / "bandwidth_reduction_chart_seeds.png")
        plot_sweep(chart, list(THRESHOLDS), [s["reduction_pct_mean"] for s in summary],
                   [s["fnr_pct_mean"] for s in summary],
                   f"Edge Z-score filter ({args.policy} buffer), offline, seeds "
                   f"{seeds[0]}..{seeds[-1]} (mean ± std, {n_samples} samples each)",
                   reduction_err=[s["reduction_pct_std"] for s in summary],
                   fnr_err=[s["fnr_pct_std"] for s in summary])
    return 0


def simulate(seed: int, model, n_samples: int, policy: str,
             threshold: float = DEFAULT_THRESHOLD) -> List[dict]:
    """Run one filter copy over a generated stream and keep every decision."""
    filt = EdgeFilter(threshold, policy)
    out = []
    for smp in generate_samples(seed, model, n_samples):
        dec = filt.process(smp.v)
        out.append({"t": smp.t_ms / 1000.0, "v": smp.v, "raw": smp.raw,
                    "is_spike": smp.is_spike, "state": dec.state,
                    "mu_raw": dec.mu, "sd_raw": dec.sd, "z_raw": dec.z,
                    "mu": dec.mu if dec.state != STATE_WARMUP else float("nan"),
                    "sd": dec.sd if dec.state != STATE_WARMUP else float("nan")})
    return out


def robustness_metrics(trace: List[dict], step_at_s: Optional[float],
                       telemetry_topic: str) -> dict:
    """Detection metrics of one simulated run (+ re-baselining after a level step)."""
    post = trace[BUFFER_SIZE:]
    alerts = [r for r in post if r["state"] == STATE_ANOMALY]
    spikes = [r for r in post if r["is_spike"]]
    missed = [r for r in spikes if r["state"] != STATE_ANOMALY]
    sent_bytes = baseline_bytes = 0
    for i, r in enumerate(trace):
        payload = format_telemetry(i + 1, int(round(r["t"] * 1000)), r["raw"], r["v"],
                                   r["mu_raw"], r["sd_raw"], r["z_raw"], r["state"])
        size = mqtt_publish_size(telemetry_topic, payload)
        baseline_bytes += size
        if r["state"] != STATE_NOMINAL:
            sent_bytes += size
    m = {"samples": len(trace), "critical_events": len(spikes),
         "detected": len(spikes) - len(missed), "missed": len(missed),
         "missed_clipped_at_rail": sum(1 for r in missed if r["raw"] in (0, 4095)),
         "alerts": len(alerts),
         "false_positives": sum(1 for r in alerts if not r["is_spike"]),
         "reduction_pct": (
             round(100.0 * (1 - sent_bytes / baseline_bytes), 2)
             if baseline_bytes else 0.0),
         "flagged_after_step": None, "rebaseline_s": None, "fp_10s_after_step": None}
    if step_at_s is not None:
        after = [r for r in trace if r["t"] >= step_at_s - 1e-9]
        flagged = 0
        rebase_t = None
        for r in after:
            if r["state"] == STATE_NOMINAL:
                rebase_t = r["t"]
                break
            if not r["is_spike"]:
                flagged += 1
        m["flagged_after_step"] = flagged
        m["rebaseline_s"] = round(rebase_t - step_at_s, 2) if rebase_t is not None else None
        m["fp_10s_after_step"] = sum(1 for r in after if r["t"] < step_at_s + 10.0
                                     and r["state"] == STATE_ANOMALY and not r["is_spike"])
    return m


ROBUSTNESS_SCENARIOS = (
    ("reference", 120.0, 0.0, None, 0.0),
    ("drift_0.15V_per_min", 600.0, 0.15, None, 0.0),
    ("step_+0.8V_at_60s", 120.0, 0.0, 60.0, 0.8),
)


def run_robustness(args: argparse.Namespace) -> int:
    seeds = args.seeds or [args.seed]
    tel_topic = topic(args.topic_prefix, "telemetry")
    base = model_from_args(args)
    rows: List[dict] = []
    traces: Dict[tuple, List[dict]] = {}
    for name, duration, drift, step_at, step_v in ROBUSTNESS_SCENARIOS:
        model = SignalModel(base.offset_v, base.amplitude_v, base.freq_hz, base.noise_sigma_v,
                            base.spike_prob, base.spike_min_v, base.spike_max_v,
                            drift, step_at, step_v)
        n = int(round(duration * SAMPLE_RATE_HZ))
        for policy in (POLICY_GATED, POLICY_ALL):
            for seed in seeds:
                trace = simulate(seed, model, n, policy)
                if seed == seeds[0]:
                    traces[(name, policy)] = trace
                rows.append({"scenario": name, "policy": policy, "seed": seed,
                             "threshold_sigma": DEFAULT_THRESHOLD, "duration_s": duration,
                             "drift_v_per_min": drift, "step_at_s": step_at, "step_v": step_v,
                             **robustness_metrics(trace, step_at, tel_topic)})
    if len(seeds) > 1:
        totals = []
        for name, *_ in ROBUSTNESS_SCENARIOS:
            for policy in (POLICY_GATED, POLICY_ALL):
                grp = [r for r in rows if r["scenario"] == name and r["policy"] == policy]
                agg = dict(grp[0], seed=f"{seeds[0]}-{seeds[-1]} (sum/mean)")
                for k in ("samples", "critical_events", "detected", "missed",
                          "missed_clipped_at_rail", "alerts", "false_positives"):
                    agg[k] = sum(r[k] for r in grp)
                agg["reduction_pct"] = round(statistics.mean(r["reduction_pct"] for r in grp), 2)
                for k in ("flagged_after_step", "rebaseline_s", "fp_10s_after_step"):
                    vals = [r[k] for r in grp if r[k] is not None]
                    agg[k] = round(statistics.mean(vals), 2) if vals else None
                totals.append(agg)
        rows = rows + totals
    stem = args.name or "robustness"
    write_csv(RESULTS_DIR / f"{stem}.csv", rows)
    shown = [r for r in rows if len(seeds) == 1 or not isinstance(r["seed"], int)]
    print(f"{'scenario':<22} {'policy':<6} {'crit':>5} {'miss':>5} {'clip':>5} {'FP':>6} "
          f"{'red%':>6} {'flag@step':>9} {'rebase s':>8}")
    for r in shown:
        print(f"{r['scenario']:<22} {r['policy']:<6} {r['critical_events']:>5} {r['missed']:>5} "
              f"{r['missed_clipped_at_rail']:>5} {r['false_positives']:>6} "
              f"{r['reduction_pct']:>6.2f} {str(r['flagged_after_step']):>9} "
              f"{str(r['rebaseline_s']):>8}")
    if not args.no_chart:
        plot_robustness(Path(args.chart) if args.chart else PHOTOS_DIR / "drift_step_test.png",
                        traces, seeds[0])
    return 0


def plot_robustness(path: Path, traces: Dict[tuple, List[dict]], seed: int) -> None:
    """Top: 10 min drift (gated). Bottom: zoom on the +0.8 V step, gated vs push-all."""
    try:
        import matplotlib  # type: ignore[import-not-found]
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt  # type: ignore[import-not-found]
    except ImportError:
        LOG.warning("matplotlib not installed; skipping robustness chart for %s", path)
        return

    def arr(trace, key):
        return np.asarray([r[key] for r in trace], dtype=float)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 8.5))
    name, dur, drift = ROBUSTNESS_SCENARIOS[1][0], ROBUSTNESS_SCENARIOS[1][1], \
        ROBUSTNESS_SCENARIOS[1][2]
    tr = traces[(name, POLICY_GATED)]
    t, v, mu, sd = arr(tr, "t"), arr(tr, "v"), arr(tr, "mu"), arr(tr, "sd")
    spike = np.asarray([r["is_spike"] for r in tr])
    alert = np.asarray([r["state"] == STATE_ANOMALY for r in tr])
    ax1.plot(t, v, color="#1f77b4", linewidth=0.4, label="v (V)")
    ax1.plot(t, mu, color="black", linewidth=1.0, label="μ (gated)")
    ax1.fill_between(t, mu - 2.5 * sd, mu + 2.5 * sd, color="gray", alpha=0.3,
                     label="μ ± 2.5σ")
    ax1.plot(t, 1.65 + drift * t / 60.0, color="green", linestyle="--", linewidth=1,
             label=f"true baseline (+{drift:g} V/min)")
    fn, fp = spike & ~alert & (t > BUFFER_SIZE / SAMPLE_RATE_HZ), alert & ~spike
    ax1.scatter(t[fn], v[fn], marker="x", color="purple", s=40, zorder=3,
                label=f"spike missed ({fn.sum()})")
    ax1.scatter(t[fp], v[fp], marker="^", color="orange", s=30, zorder=3,
                label=f"false alarm ({fp.sum()})")
    ax1.axhline(3.3, color="red", linewidth=0.8, linestyle=":")
    ax1.set_xlim(0, dur)
    ax1.set_ylim(-0.1, 3.45)
    ax1.set_xlabel("Time (s)")
    ax1.set_ylabel("Voltage (V)")
    ax1.set_title(f"(a) Slow drift {drift:g} V/min for {dur / 60:g} min, gated buffer, "
                  f"|Z| ≥ 2.5, seed {seed} (red dotted = 3.3 V ADC rail)", fontsize=10)
    ax1.legend(loc="upper left", fontsize=8, ncol=3, framealpha=0.95)
    ax1.grid(alpha=0.3)

    name, _, _, step_at, step_v = ROBUSTNESS_SCENARIOS[2]
    lo, hi = step_at - 5.0, step_at + 15.0
    for policy, color, style, marker in ((POLICY_GATED, "black", "-", "o"),
                                         (POLICY_ALL, "#d62728", "--", "s")):
        tr = traces[(name, policy)]
        t, v, mu, sd = arr(tr, "t"), arr(tr, "v"), arr(tr, "mu"), arr(tr, "sd")
        sel = (t >= lo) & (t <= hi)
        alert = np.asarray([r["state"] == STATE_ANOMALY for r in tr])
        spike = np.asarray([r["is_spike"] for r in tr])
        if policy == POLICY_GATED:
            ax2.plot(t[sel], v[sel], color="#1f77b4", linewidth=0.9, label="v (V)")
            ax2.fill_between(t[sel], (mu - 2.5 * sd)[sel], (mu + 2.5 * sd)[sel], color="gray",
                             alpha=0.25, label="μ ± 2.5σ (gated)")
        ax2.plot(t[sel], mu[sel], color=color, linestyle=style, linewidth=1.3,
                 label=f"μ ({policy})")
        fp = sel & alert & ~spike
        ax2.scatter(t[fp], v[fp] + (0.05 if policy == POLICY_GATED else -0.05),
                    marker=marker, facecolors="none", edgecolors=color, s=30,
                    label=f"non-spike alert, {policy} ({fp.sum()} in view)")
    ax2.axvline(step_at, color="green", linestyle=":", linewidth=1.2,
                label=f"+{step_v:g} V step at {step_at:g} s")
    ax2.set_xlim(lo, hi)
    ax2.set_ylim(-0.1, 3.45)
    ax2.set_xlabel("Time (s)")
    ax2.set_ylabel("Voltage (V)")
    ax2.set_title(f"(b) Sustained level step, gated (REBASELINE_RUN = {REBASELINE_RUN}) vs "
                  f"push-all, |Z| ≥ 2.5, seed {seed}", fontsize=10)
    ax2.legend(loc="upper left", fontsize=8, ncol=3, framealpha=0.95)
    ax2.grid(alpha=0.3)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=200)
    plt.close(fig)
    LOG.info("Wrote %s", path)


def replot(path: Path, args: argparse.Namespace) -> int:
    """Redraw a sweep or policy-comparison chart from a saved results/*.json file."""
    if args.no_chart:
        LOG.info("replot called with --no-chart; nothing to do")
        return 0
    data = json.loads(path.read_text(encoding="utf-8"))
    meta = data["meta"]
    if "gated" in data and "push_all" in data:
        seeds = meta["seeds"]
        chart = Path(args.chart) if args.chart else PHOTOS_DIR / "policy_comparison.png"
        plot_policy_comparison(
            chart, data["gated"], data["push_all"],
            f"Buffer update policy ablation: seeds {seeds[0]}..{seeds[-1]}, "
            f"{meta['samples_per_seed']} samples each (mean ± std)")
    elif "summary" in data:
        rows = data["summary"]
        seeds = meta["seeds"]
        chart = (Path(args.chart) if args.chart
                 else PHOTOS_DIR / "bandwidth_reduction_chart_seeds.png")
        plot_sweep(chart, [r["threshold_sigma"] for r in rows],
                   [r["reduction_pct_mean"] for r in rows], [r["fnr_pct_mean"] for r in rows],
                   f"Edge Z-score filter ({meta.get('policy', POLICY_ALL)} buffer), offline, "
                   f"seeds {seeds[0]}..{seeds[-1]} "
                   f"(mean ± std, {meta['samples_per_seed']} samples each)",
                   reduction_err=[r["reduction_pct_std"] for r in rows],
                   fnr_err=[r["fnr_pct_std"] for r in rows])
    else:
        rows = data["results"]
        chart = Path(args.chart) if args.chart else PHOTOS_DIR / "bandwidth_reduction_chart.png"
        plot_sweep(chart, [r["threshold_sigma"] for r in rows],
                   [r["reduction_pct"] for r in rows], [r["fnr_pct"] for r in rows],
                   f"Edge Z-score filter ({meta.get('policy', POLICY_ALL)} buffer): bandwidth vs. "
                   f"missed events ({meta['mode']}, seed {meta['seed']}, "
                   f"{meta['samples']} samples)")
    return 0


class StopFlag:
    """SIGINT / SIGTERM -> flag; the main loop finishes the window early and still saves."""

    def __init__(self) -> None:
        self.set = False
        signal.signal(signal.SIGINT, self._handler)
        signal.signal(signal.SIGTERM, self._handler)

    def _handler(self, signum, frame) -> None:  # noqa: ARG002
        LOG.info("Signal %s received - closing the measurement window", signum)
        self.set = True


def make_client(args: argparse.Namespace, client_id: str, topics: Iterable[str],
                inbox: "queue.Queue"):
    """Connect a paho-mqtt 2.x client that pushes (topic, payload, wire_bytes) into inbox."""
    try:
        import paho.mqtt.client as mqtt  # type: ignore[import-not-found]
        from paho.mqtt.enums import CallbackAPIVersion  # type: ignore[import-not-found]
    except ImportError:
        LOG.error("paho-mqtt is not installed. Live broker modes require 'pip install paho-mqtt'. For offline analysis, use '--offline'.")
        sys.exit(1)

    client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id=client_id)
    topic_list = list(topics)

    def on_connect(cl, userdata, flags, reason_code, properties):  # noqa: ARG001
        if reason_code.is_failure:
            LOG.error("Broker refused connection: %s", reason_code)
            return
        LOG.info("Connected to mqtt://%s:%d, subscribing to %s", args.host, args.port,
             ", ".join(topic_list))
        cl.subscribe([(t, 0) for t in topic_list])

    def on_message(cl, userdata, msg):  # noqa: ARG001
        payload = msg.payload.decode("utf-8", errors="replace")
        inbox.put((msg.topic, payload, mqtt_publish_size(msg.topic, payload), time.monotonic()))

    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=5)
    client.connect(args.host, args.port, keepalive=30)
    client.loop_start()
    return client


def _parse_json_dict(payload: str, topic_name: str) -> Optional[dict]:
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        LOG.warning("Ignoring non-JSON message on %s: %r", topic_name, payload[:80])
        return None
    if not isinstance(data, dict):
        LOG.warning("Ignoring non-object JSON on %s: %r", topic_name, payload[:80])
        return None
    return data


def run_live(args: argparse.Namespace) -> int:
    if args.duration <= 0:
        LOG.error("Duration must be positive (got %g)", args.duration)
        return 2
    raw_t, truth_t = topic(args.topic_prefix, "raw"), topic(args.topic_prefix, "truth")
    inbox: "queue.Queue" = queue.Queue()
    stop = StopFlag()
    try:
        client = make_client(args, "bandwidth-analyzer", [raw_t, truth_t], inbox)
    except OSError as exc:
        LOG.error("Cannot reach broker at %s:%d (%s). Is Mosquitto running? "
                  "Use --offline to run without a broker.", args.host, args.port, exc)
        return 1

    exp = Experiment(prefix=args.topic_prefix, node_id=args.node, policy=args.policy)
    truth: set = set()
    pending: deque = deque()
    window_start: Optional[float] = None
    LOG.info("Waiting for the first sample on %s (start sensor_emulator.py now) ...", raw_t)

    def flush(force: bool) -> None:
        while pending and (force or len(pending) > 5):
            r = pending.popleft()
            exp.add(r["seq"], r["t_ms"], r["raw"], r["v"], r["seq"] in truth, r["bytes"])

    def record_truth(tp: str, payload: str) -> None:
        data = _parse_json_dict(payload, tp)
        if data is None:
            return
        try:
            truth.add(int(data["seq"]))
        except (KeyError, TypeError, ValueError):
            LOG.warning("Ignoring malformed truth message on %s: %r", tp, payload[:80])

    try:
        while not stop.set:
            if window_start is not None and time.monotonic() - window_start >= args.duration:
                break
            try:
                tp, payload, nbytes, t_rx = inbox.get(timeout=0.2)
            except queue.Empty:
                continue
            if tp == truth_t:
                record_truth(tp, payload)
                continue
            data = _parse_json_dict(payload, tp)
            if data is None:
                continue
            try:
                seq = int(data["seq"])
                t_ms = int(data["t_ms"])
                raw = int(data["raw"])
            except (KeyError, TypeError, ValueError):
                LOG.warning("Ignoring malformed raw message on %s: %r", tp, payload[:80])
                continue
            if window_start is None:
                window_start = t_rx
                LOG.info("First sample seq=%s received - %g s window started", seq, args.duration)
            pending.append({"seq": seq, "t_ms": t_ms, "raw": raw,
                            "v": raw_to_volts(raw), "bytes": nbytes})
            before = exp.samples
            flush(force=False)
            if exp.samples != before and exp.samples % 100 == 0:
                LOG.info("%d samples processed, %d spikes so far", exp.samples,
                         len(exp.spike_seqs) + len(exp.warmup_spike_seqs))

        time.sleep(0.3)
        while not inbox.empty():
            tp, payload, _, _ = inbox.get_nowait()
            if tp == truth_t:
                record_truth(tp, payload)
        flush(force=True)
    finally:
        client.disconnect()
        client.loop_stop()

    if exp.samples == 0:
        LOG.error("No samples received - nothing to analyse")
        return 1
    elapsed = time.monotonic() - window_start if window_start else 0.0
    save_run(exp, args, "live-mqtt", args.seed if args.seed_known else None,
             {"broker": f"{args.host}:{args.port}", "window_s_measured": round(elapsed, 2),
              "raw_topic": raw_t, "truth_topic": truth_t})
    return 0


def run_live_topic(args: argparse.Namespace) -> int:
    """Measure what a real / Wokwi node publishes on <prefix>/telemetry during the window."""
    if args.duration <= 0:
        LOG.error("Duration must be positive (got %g)", args.duration)
        return 2
    tel_t, status_t = topic(args.topic_prefix, "telemetry"), topic(args.topic_prefix, "status")
    inbox: "queue.Queue" = queue.Queue()
    stop = StopFlag()
    try:
        client = make_client(args, "bandwidth-analyzer-live-topic", [tel_t, status_t], inbox)
    except OSError as exc:
        LOG.error("Cannot reach broker at %s:%d (%s)", args.host, args.port, exc)
        return 1
    start = time.monotonic()
    counts: Dict[str, int] = {}
    tel_bytes = tel_msgs = status_msgs = 0
    seqs: List[int] = []
    LOG.info("Measuring %s for %g s ...", tel_t, args.duration)
    try:
        while not stop.set and time.monotonic() - start < args.duration:
            try:
                tp, payload, nbytes, _ = inbox.get(timeout=0.2)
            except queue.Empty:
                continue
            if tp == status_t:
                status_msgs += 1
                LOG.info("status: %s", payload)
                continue
            tel_msgs += 1
            tel_bytes += nbytes
            try:
                data = json.loads(payload)
                if not isinstance(data, dict):
                    raise ValueError("expected JSON object")
                counts[data.get("state", "?")] = counts.get(data.get("state", "?"), 0) + 1
                seqs.append(int(data["seq"]))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                counts["unparsed"] = counts.get("unparsed", 0) + 1
    finally:
        client.disconnect()
        client.loop_stop()

    result = {"mode": "live-topic", "broker": f"{args.host}:{args.port}", "topic": tel_t,
              "window_s": round(time.monotonic() - start, 2), "telemetry_messages": tel_msgs,
              "telemetry_bytes": tel_bytes, "messages_by_state": counts,
              "status_messages_not_counted": status_msgs}
    if len(seqs) >= 2 and tel_msgs:
        sampled = max(seqs) - min(seqs) + 1
        est_baseline = sampled * tel_bytes / tel_msgs
        if est_baseline > 0:
            result.update({"samples_taken_by_node": sampled,
                           "estimated_baseline_bytes": round(est_baseline),
                           "estimated_reduction_pct": round(100 * (1 - tel_bytes / est_baseline), 2),
                           "note": "baseline = samples taken x mean size of received packets"})
    print(json.dumps(result, indent=2))
    write_json(RESULTS_DIR / f"{args.name or 'live_node_bytes'}.json", result)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Sweep the edge-filter Z threshold and measure MQTT bandwidth.")
    p.add_argument("--host", default="localhost", help="MQTT broker host")
    p.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    p.add_argument("--duration", type=float, default=120.0,
                   help="measurement window in seconds (default %(default)s)")
    p.add_argument("--seed", type=int, default=None,
                   help="seed (offline: generator seed, default 42; live: label only)")
    p.add_argument("--seeds", type=int, nargs="+",
                   help="offline: several seeds -> mean ± std (e.g. --seeds 1 2 3 ... 10)")
    p.add_argument("--samples", type=int, default=0,
                   help="offline: number of samples (default duration x 10 Hz)")
    p.add_argument("--policy", choices=POLICIES, default=POLICY_GATED,
                   help="buffer update policy: gated (v2 default) or all (v1 push-all)")
    p.add_argument("--compare-policies", action="store_true",
                   help="offline: run gated and push-all for --seeds, write the ablation")
    p.add_argument("--robustness", action="store_true",
                   help="offline: drift + level-step tests (results/robustness.csv + figure)")
    p.add_argument("--offline", action="store_true",
                   help="generate the stream in-process (no broker needed)")
    p.add_argument("--live-topic", action="store_true",
                   help="measure bytes a real/Wokwi node publishes on <prefix>/telemetry")
    p.add_argument("--node", default=NODE_ID, help="node id used in JSON payloads")
    p.add_argument("--topic-prefix", default=TOPIC_PREFIX, help="MQTT topic prefix")
    p.add_argument("--name", default=None,
                   help="output file stem in results/ (default threshold_sweep[_seeds])")
    p.add_argument("--chart", default=None, help="chart path (default docs/photos/...)")
    p.add_argument("--no-chart", action="store_true", help="do not draw the sweep chart")
    p.add_argument("--replot", metavar="JSON", default=None,
                   help="only redraw the sweep chart from a saved results/*.json")
    p.add_argument("--trace", nargs="?", const=str(PHOTOS_DIR / "signal_trace.png"),
                   default=None, help="offline: also draw a signal trace (optional path)")
    p.add_argument("--trace-start", type=float, default=10.0, help="trace start time, s")
    p.add_argument("--trace-length", type=float, default=20.0, help="trace length, s")
    p.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    add_model_arguments(p)
    return p


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    args.seed_known = args.seed is not None
    if args.seed is None:
        args.seed = 42
    if args.replot:
        return replot(Path(args.replot), args)
    if args.live_topic:
        return run_live_topic(args)
    if args.robustness:
        return run_robustness(args)
    if args.offline or args.compare_policies or args.seeds:
        rc = run_offline(args)
        if rc == 0 and args.trace:
            plot_trace(Path(args.trace), args.seed, args, args.trace_start, args.trace_length,
                       DEFAULT_THRESHOLD, args.policy)
        return rc
    if args.trace:
        LOG.warning("--trace is only supported with --offline; ignoring in live mode")
    return run_live(args)


if __name__ == "__main__":
    sys.exit(main())
