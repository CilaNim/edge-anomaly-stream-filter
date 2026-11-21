#!/usr/bin/env python3
"""
sensor_emulator.py - synthetic analog sensor stream for the Edge Anomaly Stream Filter.

The emulator reproduces what the ESP32 edge node "sees" on GPIO36 (VP), sample by sample:

    v(t) = 1.65 V + A * sin(2*pi*0.5 Hz*t) + n(t) + spike(t)

    * 0.5 Hz sinusoidal baseline (A = 0.20 V) around mid-scale 1.65 V
    * Gaussian thermal white noise n(t), sigma_n = 0.03 V
    * with probability p = 0.05 per sample a spike of +/-(1.2 ... 1.8) V (random sign)
    * optional robustness tests (off by default): slow linear drift (--drift-v-per-min) and a
      sustained level step (--step-at-s / --step-v)
    * clipping to the ADC input range [0, 3.3] V and 12-bit quantisation exactly like the
      firmware does it: raw = round(V / 3.3 * 4095), v = raw * 3.3 / 4095

Every sample is published unfiltered on  <prefix>/raw  (this is the "no edge filtering"
baseline), and the sequence number of every injected spike is published on  <prefix>/truth
(ground truth for the analyzer, NOT counted as bandwidth).

The generator is a pure function of the seed: the same --seed always gives the same stream,
whether it is published over MQTT or generated in-process by bandwidth_analyzer.py --offline.

Usage:
    python sensor_emulator.py --host localhost --port 1883 --seed 42 --duration 130
    python sensor_emulator.py --duration 0          # run until Ctrl+C
"""

from __future__ import annotations

import argparse
import logging
import math
import signal
import sys
import time
from dataclasses import dataclass
from typing import Iterator, Optional

import numpy as np

NODE_ID = "edge-node-01"
TOPIC_PREFIX = "eeair/cilan1m0/edge-node-01"
SAMPLE_PERIOD_MS = 100
SAMPLE_RATE_HZ = 1000.0 / SAMPLE_PERIOD_MS
ADC_MAX = 4095
V_REF = 3.3

LOG = logging.getLogger("sensor_emulator")


def topic(prefix: str, leaf: str) -> str:
    """Build a full topic name, e.g. topic(TOPIC_PREFIX, 'raw')."""
    return f"{prefix.rstrip('/')}/{leaf}"


@dataclass(frozen=True)
class SignalModel:
    """Physical parameters of the emulated sensor signal (all voltages in volts)."""

    offset_v: float = 1.65
    amplitude_v: float = 0.20
    freq_hz: float = 0.5
    noise_sigma_v: float = 0.03
    spike_prob: float = 0.05
    spike_min_v: float = 1.2
    spike_max_v: float = 1.8
    drift_v_per_min: float = 0.0
    step_at_s: Optional[float] = None
    step_v: float = 0.0


@dataclass(frozen=True)
class Sample:
    """One emulated ADC sample."""

    seq: int
    t_ms: int
    raw: int
    v: float
    is_spike: bool
    spike_v: float


def quantise(volts: float) -> int:
    """Clip to the ADC range and convert to a 12-bit code like the ESP32 ADC."""
    clipped = min(max(volts, 0.0), V_REF)
    code = int(round(clipped / V_REF * ADC_MAX))
    return min(max(code, 0), ADC_MAX)


def raw_to_volts(raw: int) -> float:
    """Same conversion as the firmware: V = raw * 3.3 / 4095.0."""
    return raw * V_REF / ADC_MAX


def validate_model(model: SignalModel) -> None:
    """Validate physical signal parameters before generating samples."""
    if model.noise_sigma_v < 0:
        raise ValueError(f"noise_sigma_v must be >= 0, got {model.noise_sigma_v}")
    if not (0.0 <= model.spike_prob <= 1.0):
        raise ValueError(f"spike_prob must be in [0, 1], got {model.spike_prob}")
    if model.spike_max_v < model.spike_min_v:
        raise ValueError(
            f"spike_max_v ({model.spike_max_v}) must be >= spike_min_v ({model.spike_min_v})")


def generate_samples(seed: int, model: SignalModel = SignalModel(),
                     n_samples: Optional[int] = None) -> Iterator[Sample]:
    """
    Yield emulated samples one by one (endless if n_samples is None).

    Random numbers are drawn in a fixed order per sample (noise, spike flag, spike magnitude,
    spike sign), so the stream is identical for the same seed no matter how many samples are
    requested or whether it is consumed live or offline.
    """
    validate_model(model)
    rng = np.random.default_rng(seed)
    seq = 0
    while n_samples is None or seq < n_samples:
        seq += 1
        t_s = seq * SAMPLE_PERIOD_MS / 1000.0
        noise = rng.normal(0.0, model.noise_sigma_v)
        spike_draw = rng.random()
        magnitude = rng.uniform(model.spike_min_v, model.spike_max_v)
        sign = 1.0 if rng.random() < 0.5 else -1.0

        is_spike = spike_draw < model.spike_prob
        spike_v = sign * magnitude if is_spike else 0.0
        baseline = (model.offset_v
                    + model.amplitude_v * math.sin(2.0 * math.pi * model.freq_hz * t_s)
                    + model.drift_v_per_min * t_s / 60.0)
        if model.step_at_s is not None and t_s >= model.step_at_s:
            baseline += model.step_v
        volts = baseline + noise + spike_v
        raw = quantise(volts)
        yield Sample(seq=seq, t_ms=seq * SAMPLE_PERIOD_MS, raw=raw, v=raw_to_volts(raw),
                     is_spike=is_spike, spike_v=spike_v)


def format_raw_payload(sample: Sample, node_id: str = NODE_ID) -> str:
    """Compact JSON for the unfiltered stream (fixed key order, v with 3 decimals)."""
    return (f'{{"node":"{node_id}","seq":{sample.seq},"t_ms":{sample.t_ms},'
            f'"raw":{sample.raw},"v":{sample.v:.3f}}}')


def format_truth_payload(sample: Sample) -> str:
    """Ground-truth message for an injected spike (seq + signed spike voltage)."""
    return f'{{"seq":{sample.seq},"spike_v":{sample.spike_v:.3f}}}'


def mqtt_publish_size(topic_name: str, payload: str) -> int:
    """
    Size in bytes of an MQTT 3.1.1 PUBLISH packet at QoS 0 (no packet identifier):
    fixed header (1 byte + remaining-length varint) + 2-byte topic length + topic + payload.
    """
    remaining = 2 + len(topic_name.encode("utf-8")) + len(payload.encode("utf-8"))
    varint_len = 1
    value = remaining
    while value >= 128:
        value //= 128
        varint_len += 1
    return 1 + varint_len + remaining


def model_from_args(args: argparse.Namespace) -> SignalModel:
    """Build a SignalModel from the shared CLI options."""
    return SignalModel(amplitude_v=args.amplitude, noise_sigma_v=args.noise,
                       spike_prob=args.spike_prob, spike_min_v=args.spike_min,
                       spike_max_v=args.spike_max, drift_v_per_min=args.drift_v_per_min,
                       step_at_s=args.step_at_s, step_v=args.step_v)


def add_model_arguments(parser: argparse.ArgumentParser) -> None:
    """CLI options for the signal model (also used by bandwidth_analyzer.py --offline)."""
    d = SignalModel()
    g = parser.add_argument_group("signal model")
    g.add_argument("--amplitude", type=float, default=d.amplitude_v,
                   help="sinusoid amplitude in V (default %(default)s)")
    g.add_argument("--noise", type=float, default=d.noise_sigma_v,
                   help="Gaussian noise sigma in V (default %(default)s)")
    g.add_argument("--spike-prob", type=float, default=d.spike_prob,
                   help="spike probability per sample (default %(default)s)")
    g.add_argument("--spike-min", type=float, default=d.spike_min_v,
                   help="minimum spike magnitude in V (default %(default)s)")
    g.add_argument("--spike-max", type=float, default=d.spike_max_v,
                   help="maximum spike magnitude in V (default %(default)s)")
    g.add_argument("--drift-v-per-min", type=float, default=d.drift_v_per_min,
                   help="robustness test: linear baseline drift in V/min (default off)")
    g.add_argument("--step-at-s", type=float, default=d.step_at_s,
                   help="robustness test: time of a sustained level step in s (default off)")
    g.add_argument("--step-v", type=float, default=d.step_v,
                   help="robustness test: size of the level step in V (default %(default)s)")


class GracefulExit:
    """Turns SIGINT / SIGTERM into a flag so the main loop can shut down cleanly."""

    def __init__(self) -> None:
        self.requested = False
        signal.signal(signal.SIGINT, self._handler)
        signal.signal(signal.SIGTERM, self._handler)

    def _handler(self, signum, frame) -> None:  # noqa: ARG002
        LOG.info("Signal %s received - stopping after the current sample", signum)
        self.requested = True


def run_publisher(args: argparse.Namespace) -> int:
    """Publish the emulated stream in real time (10 Hz) until --duration or Ctrl+C."""
    if args.duration < 0:
        LOG.error("duration must be >= 0 (got %g)", args.duration)
        return 2

    try:
        import paho.mqtt.client as mqtt  # type: ignore[import-not-found]
        from paho.mqtt.enums import CallbackAPIVersion  # type: ignore[import-not-found]
    except ImportError:
        LOG.error("paho-mqtt is not installed. To publish real-time sensor streams, install it via 'pip install paho-mqtt'.")
        return 1

    raw_topic = topic(args.topic_prefix, "raw")
    truth_topic = topic(args.topic_prefix, "truth")
    model = model_from_args(args)
    stop = GracefulExit()

    client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2,
                         client_id=f"sensor-emulator-{args.seed}")

    def on_connect(cl, userdata, flags, reason_code, properties):  # noqa: ARG001
        if reason_code.is_failure:
            LOG.error("Broker refused connection: %s", reason_code)
        else:
            LOG.info("Connected to mqtt://%s:%d", args.host, args.port)

    def on_disconnect(cl, userdata, flags, reason_code, properties):  # noqa: ARG001
        if not stop.requested:
            LOG.warning("Disconnected from broker (%s) - paho will reconnect", reason_code)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=1, max_delay=5)
    try:
        client.connect(args.host, args.port, keepalive=30)
    except OSError as exc:
        LOG.error("Cannot reach broker at %s:%d (%s). Is Mosquitto running?",
                  args.host, args.port, exc)
        return 1
    client.loop_start()

    n_samples = None if args.duration <= 0 else int(round(args.duration * SAMPLE_RATE_HZ))
    LOG.info("Publishing %s samples at %.0f Hz on %s (seed=%d, model=%s)",
             "endless" if n_samples is None else n_samples, SAMPLE_RATE_HZ,
             raw_topic, args.seed, model)

    period_s = SAMPLE_PERIOD_MS / 1000.0
    next_deadline = time.monotonic()
    sent = spikes = raw_bytes = 0
    try:
        for sample in generate_samples(args.seed, model, n_samples):
            if stop.requested:
                break
            delay = next_deadline - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            elif delay < -period_s:
                next_deadline = time.monotonic()
            next_deadline += period_s

            payload = format_raw_payload(sample, args.node)
            if sample.is_spike:
                client.publish(truth_topic, format_truth_payload(sample), qos=0)
                spikes += 1
            client.publish(raw_topic, payload, qos=0)
            sent += 1
            raw_bytes += mqtt_publish_size(raw_topic, payload)
            if args.verbose:
                LOG.debug("%s%s", payload, "  <-- SPIKE" if sample.is_spike else "")
            if sent % 100 == 0:
                LOG.info("seq=%d published, %d spikes injected, %d bytes on %s",
                         sample.seq, spikes, raw_bytes, raw_topic)

        LOG.info("Done: %d samples, %d spikes, %d bytes of raw PUBLISH packets", sent, spikes,
                 raw_bytes)
        time.sleep(0.5)
    finally:
        stop.requested = True
        client.disconnect()
        client.loop_stop()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Emulate the analog sensor on GPIO36 and publish it over MQTT.")
    parser.add_argument("--host", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--seed", type=int, default=42, help="random seed (reproducible)")
    parser.add_argument("--duration", type=float, default=130.0,
                        help="seconds to publish; 0 = until Ctrl+C (default %(default)s)")
    parser.add_argument("--node", default=NODE_ID, help="node id in the JSON payload")
    parser.add_argument("--topic-prefix", default=TOPIC_PREFIX, help="MQTT topic prefix")
    parser.add_argument("-v", "--verbose", action="store_true", help="log every payload")
    add_model_arguments(parser)
    return parser


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    return run_publisher(args)


if __name__ == "__main__":
    sys.exit(main())
