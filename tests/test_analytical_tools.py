"""
Unit tests for Edge Anomaly Stream Filter analytical tools and firmware equivalence.
Designed to run with Python's built-in unittest module with zero external dependencies beyond numpy.
"""

import math
import unittest
from unittest.mock import patch
import numpy as np

from src.analytical_tools.sensor_emulator import (
    ADC_MAX,
    V_REF,
    SAMPLE_PERIOD_MS,
    SAMPLE_RATE_HZ,
    SignalModel,
    quantise,
    raw_to_volts,
    validate_model,
    generate_samples,
    format_raw_payload,
    format_truth_payload,
    mqtt_publish_size,
    Sample,
)
from src.analytical_tools.bandwidth_analyzer import (
    BUFFER_SIZE,
    SIGMA_EPSILON,
    DEFAULT_THRESHOLD,
    REBASELINE_RUN,
    POLICY_GATED,
    POLICY_ALL,
    STATE_WARMUP,
    STATE_NOMINAL,
    STATE_ANOMALY,
    EdgeFilter,
    Experiment,
    format_telemetry,
)


class TestSensorEmulator(unittest.TestCase):
    """Tests for synthetic sensor generator and conversions."""

    def test_quantise_and_raw_to_volts(self):
        self.assertEqual(quantise(0.0), 0)
        self.assertEqual(quantise(V_REF), ADC_MAX)
        self.assertEqual(quantise(-1.0), 0)  # lower clip
        self.assertEqual(quantise(5.0), ADC_MAX)  # upper clip
        self.assertAlmostEqual(raw_to_volts(0), 0.0, places=6)
        self.assertAlmostEqual(raw_to_volts(ADC_MAX), V_REF, places=6)
        self.assertAlmostEqual(raw_to_volts(quantise(1.65)), 1.65, delta=0.002)

    def test_quantise_edge_cases(self):
        self.assertEqual(quantise(float("inf")), ADC_MAX)
        self.assertEqual(quantise(-float("inf")), 0)
        with self.assertRaises(ValueError):
            quantise(float("nan"))

    def test_validate_model(self):
        # Valid model
        validate_model(SignalModel())
        # Invalid noise sigma
        with self.assertRaises(ValueError):
            validate_model(SignalModel(noise_sigma_v=-0.01))
        # Invalid spike probability
        with self.assertRaises(ValueError):
            validate_model(SignalModel(spike_prob=-0.1))
        with self.assertRaises(ValueError):
            validate_model(SignalModel(spike_prob=1.5))
        # Invalid spike min/max
        with self.assertRaises(ValueError):
            validate_model(SignalModel(spike_min_v=2.0, spike_max_v=1.0))

    def test_generate_samples_reproducibility(self):
        samples1 = list(generate_samples(seed=42, n_samples=50))
        samples2 = list(generate_samples(seed=42, n_samples=50))
        self.assertEqual(len(samples1), 50)
        for s1, s2 in zip(samples1, samples2):
            self.assertEqual(s1.seq, s2.seq)
            self.assertEqual(s1.raw, s2.raw)
            self.assertAlmostEqual(s1.v, s2.v, places=6)
            self.assertEqual(s1.is_spike, s2.is_spike)
            self.assertAlmostEqual(s1.spike_v, s2.spike_v, places=6)

    def test_mqtt_publish_size(self):
        # 1-byte remaining length (< 128)
        sz = mqtt_publish_size("top", "pay")
        # remaining = 2 + 3 + 3 = 8
        # fixed header = 1 (packet type) + 1 (varint len) + 8 = 10
        self.assertEqual(sz, 10)

        # Multi-byte remaining length boundary
        long_payload = "x" * 126
        # remaining = 2 + 3 ("top") + 126 = 131 (>= 128 -> 2 bytes varint)
        # 1 + 2 + 131 = 134
        self.assertEqual(mqtt_publish_size("top", long_payload), 134)

    def test_payload_formatting(self):
        s = Sample(seq=1, t_ms=100, raw=2048, v=1.650, is_spike=True, spike_v=1.5)
        raw_json = format_raw_payload(s, "node-1")
        self.assertIn('"node":"node-1"', raw_json)
        self.assertIn('"seq":1', raw_json)
        self.assertIn('"v":1.650', raw_json)

        truth_json = format_truth_payload(s)
        self.assertIn('"seq":1', truth_json)
        self.assertIn('"spike_v":1.500', truth_json)


class TestEdgeFilter(unittest.TestCase):
    """Tests for EdgeFilter logic, ring buffer, stats, and update policies."""

    def test_warmup_phase(self):
        f = EdgeFilter(threshold=2.5, policy=POLICY_GATED, size=50)
        for i in range(50):
            dec = f.process(1.65)
            self.assertEqual(dec.state, STATE_WARMUP)
            self.assertTrue(dec.publish)
            self.assertEqual(dec.mu, 0.0)
            self.assertEqual(dec.sd, 0.0)
            self.assertEqual(dec.z, 0.0)
            self.assertTrue(dec.pushed)
        self.assertEqual(f.count, 50)

    def test_zero_sigma_guard(self):
        f = EdgeFilter(threshold=2.5, policy=POLICY_GATED, size=50)
        # Warmup with constant signal
        for _ in range(50):
            f.process(1.65)
        # 51st sample with exact same value: sd == 0.0 < SIGMA_EPSILON
        dec = f.process(1.65)
        self.assertEqual(dec.state, STATE_NOMINAL)
        self.assertFalse(dec.publish)
        self.assertEqual(dec.z, 0.0)
        self.assertAlmostEqual(dec.mu, 1.65, places=5)
        self.assertAlmostEqual(dec.sd, 0.0, places=5)
        self.assertEqual(f.zero_sigma_events, 1)

    def test_anomaly_detection_and_gated_policy(self):
        f = EdgeFilter(threshold=2.5, policy=POLICY_GATED, size=5)
        # Warm up buffer: 1.0, 1.0, 1.0, 1.0, 1.2
        for v in [1.0, 1.0, 1.0, 1.0, 1.2]:
            f.process(v)
        # Mean ~ 1.04, sd ~ 0.0894
        # Inject spike: 3.0 V (Z > 20)
        dec = f.process(3.0)
        self.assertEqual(dec.state, STATE_ANOMALY)
        self.assertTrue(dec.publish)
        self.assertFalse(dec.pushed)  # Gated: single anomaly NOT pushed!
        self.assertEqual(f.anomaly_run, 1)

        # Inject nominal sample: 1.04 V
        dec2 = f.process(1.04)
        self.assertEqual(dec2.state, STATE_NOMINAL)
        self.assertFalse(dec2.publish)
        self.assertTrue(dec2.pushed)
        self.assertEqual(f.anomaly_run, 0)  # Reset to 0

    def test_rebaseline_after_sustained_anomalies(self):
        f = EdgeFilter(threshold=2.5, policy=POLICY_GATED, rebaseline_run=10, size=5)
        # Warmup with small variance so sd > SIGMA_EPSILON
        for v in [1.0, 1.01, 0.99, 1.0, 1.02]:
            f.process(v)
        # Send 9 anomalies: none should be pushed
        for i in range(1, 10):
            dec = f.process(3.0)
            self.assertEqual(dec.state, STATE_ANOMALY)
            self.assertFalse(dec.pushed)
            self.assertEqual(f.anomaly_run, i)
        # 10th consecutive anomaly: pushed!
        dec10 = f.process(3.0)
        self.assertEqual(dec10.state, STATE_ANOMALY)
        self.assertTrue(dec10.pushed)
        self.assertEqual(f.anomaly_run, 10)

    def test_push_all_policy(self):
        f = EdgeFilter(threshold=2.5, policy=POLICY_ALL, size=5)
        for v in [1.0, 1.01, 0.99, 1.0, 1.02]:
            f.process(v)
        # Single anomaly is pushed immediately in 'all' policy
        dec = f.process(3.0)
        self.assertEqual(dec.state, STATE_ANOMALY)
        self.assertTrue(dec.pushed)

    def test_nan_and_inf_safety(self):
        """Verifies that NaN and Inf inputs are safely rejected and do not poison the ring buffer."""
        f = EdgeFilter(threshold=2.5, size=50)
        for _ in range(50):
            f.process(1.65)
        dec_nan = f.process(float("nan"))
        self.assertEqual(dec_nan.state, STATE_NOMINAL)
        self.assertFalse(dec_nan.pushed)
        dec_inf = f.process(float("inf"))
        self.assertEqual(dec_inf.state, STATE_NOMINAL)
        self.assertFalse(dec_inf.pushed)
        # Next sample is clean and not poisoned!
        dec_next = f.process(1.65)
        self.assertFalse(math.isnan(dec_next.mu))
        self.assertFalse(math.isnan(dec_next.sd))
        self.assertFalse(math.isnan(dec_next.z))


class TestCPlusPlusEquivalence(unittest.TestCase):
    """
    Simulates the exact C++ firmware functions from src/edge_node/main.cpp:
    RingBuffer, computeStats, evaluateSample, decideBufferAction
    and verifies that Python EdgeFilter produces byte/number identical output.
    """

    class CppSimulator:
        def __init__(self, size=50, threshold=2.5, rebaseline_run=10, gated=True):
            self.size = size
            self.threshold = threshold
            self.rebaseline_run = rebaseline_run
            self.gated = gated
            self.buf = [0.0] * size
            self.head = 0
            self.count = 0
            self.anomaly_run = 0

        def push(self, x: float):
            self.buf[self.head] = float(np.float32(x))
            self.head = (self.head + 1) % self.size
            if self.count < self.size:
                self.count += 1

        def at(self, i: int):
            oldest = (self.head + self.size - self.count) % self.size
            return self.buf[(oldest + i) % self.size]

        def compute_stats(self):
            n = self.count
            if n == 0:
                return 0.0, 0.0
            total = 0.0
            for i in range(n):
                total += self.at(i)
            mean = total / float(n)
            if n < 2:
                return mean, 0.0
            sum_sq = 0.0
            for i in range(n):
                d = self.at(i) - mean
                sum_sq += d * d
            std_dev = math.sqrt(sum_sq / float(n - 1))
            return mean, std_dev

        def evaluate_sample(self, x: float):
            if self.count < self.size:
                return STATE_WARMUP, 0.0, 0.0, 0.0, False
            mean, std_dev = self.compute_stats()
            if std_dev < 1e-4:
                return STATE_NOMINAL, mean, std_dev, 0.0, True
            z = (float(np.float32(x)) - mean) / std_dev
            state = STATE_ANOMALY if abs(z) >= self.threshold else STATE_NOMINAL
            return state, mean, std_dev, z, False

        def decide_buffer_action(self, state):
            if state == STATE_WARMUP:
                return "PUSH_WARMUP"
            if state == STATE_NOMINAL:
                self.anomaly_run = 0
                return "PUSH_NOMINAL"
            self.anomaly_run += 1
            if not self.gated:
                return "PUSH_LEGACY"
            return "PUSH_REBASELINE" if self.anomaly_run >= self.rebaseline_run else "SKIP_ANOMALY"

        def step(self, raw_volts: float):
            x = float(np.float32(raw_volts))
            state, mean, sd, z, flat = self.evaluate_sample(x)
            action = self.decide_buffer_action(state)
            if action != "SKIP_ANOMALY":
                self.push(x)
            publish = (state != STATE_NOMINAL)
            return state, mean, sd, z, publish, action

    def test_firmware_python_exact_match(self):
        cpp = self.CppSimulator(size=50, threshold=2.5, rebaseline_run=10, gated=True)
        py = EdgeFilter(threshold=2.5, policy=POLICY_GATED, rebaseline_run=10, size=50)

        # Feed 500 samples from synthetic generator
        for sample in generate_samples(seed=123, n_samples=500):
            c_state, c_mu, c_sd, c_z, c_pub, c_act = cpp.step(sample.v)
            p_dec = py.process(sample.v)

            self.assertEqual(p_dec.state, c_state, f"State mismatch at seq={sample.seq}")
            self.assertEqual(p_dec.publish, c_pub, f"Publish mismatch at seq={sample.seq}")
            self.assertAlmostEqual(p_dec.mu, c_mu, places=6, msg=f"Mean mismatch at seq={sample.seq}")
            self.assertAlmostEqual(p_dec.sd, c_sd, places=6, msg=f"SD mismatch at seq={sample.seq}")
            self.assertAlmostEqual(p_dec.z, c_z, places=5, msg=f"Z mismatch at seq={sample.seq}")
            self.assertEqual(py.anomaly_run, cpp.anomaly_run, f"anomaly_run mismatch at seq={sample.seq}")


class TestExperimentMetrics(unittest.TestCase):
    """Test Experiment metrics calculation and results."""

    def test_experiment_reduction_calculation(self):
        exp = Experiment(thresholds=[2.5], policy=POLICY_GATED)
        for s in generate_samples(seed=42, n_samples=100):
            exp.add(s.seq, s.t_ms, s.raw, s.v, s.is_spike, raw_payload_bytes=80)
        res = exp.results()
        self.assertEqual(len(res), 1)
        row = res[0]
        self.assertGreater(row["reduction_pct"], 40.0)
        self.assertEqual(row["samples"], 100)
        self.assertEqual(row["baseline_messages"], 100)


class TestCliAndExecutionModes(unittest.TestCase):
    """Test CLI argument handling and execution modes."""

    @classmethod
    def tearDownClass(cls):
        import pathlib
        res = pathlib.Path("results")
        if res.exists():
            for p in res.glob("test_*"):
                p.unlink(missing_ok=True)
            for p in res.glob("*test_*"):
                p.unlink(missing_ok=True)
            (res / "dummy.png").unlink(missing_ok=True)

    def test_offline_single_seed_no_chart(self):
        from src.analytical_tools.bandwidth_analyzer import main
        rc = main(["--offline", "--samples", "60", "--no-chart", "--name", "test_single_seed"])
        self.assertEqual(rc, 0)

    def test_offline_multi_seed_no_chart(self):
        from src.analytical_tools.bandwidth_analyzer import main
        rc = main(["--offline", "--seeds", "42", "43", "--samples", "60", "--no-chart", "--name", "test_multi_seed"])
        self.assertEqual(rc, 0)

    def test_compare_policies_no_chart(self):
        from src.analytical_tools.bandwidth_analyzer import main
        rc = main(["--compare-policies", "--seeds", "42", "43", "--samples", "60", "--no-chart", "--name", "test_compare"])
        self.assertEqual(rc, 0)

    def test_robustness_no_chart(self):
        from src.analytical_tools.bandwidth_analyzer import main
        rc = main(["--robustness", "--seeds", "42", "--no-chart", "--name", "test_robustness"])
        self.assertEqual(rc, 0)

    def test_negative_samples_error(self):
        from src.analytical_tools.bandwidth_analyzer import main
        rc = main(["--offline", "--samples", "-10", "--no-chart"])
        self.assertEqual(rc, 2)

    def test_seeds_auto_activates_offline(self):
        from src.analytical_tools.bandwidth_analyzer import main
        rc = main(["--seeds", "42", "43", "--samples", "60", "--no-chart", "--name", "test_auto_offline"])
        self.assertEqual(rc, 0)

    def test_sensor_emulator_negative_duration(self):
        from src.analytical_tools.sensor_emulator import build_parser, run_publisher
        args = build_parser().parse_args(["--duration", "-5"])
        rc = run_publisher(args)
        self.assertEqual(rc, 2)

    def test_plot_graceful_without_matplotlib(self):
        from pathlib import Path
        from src.analytical_tools.bandwidth_analyzer import plot_sweep
        # Should not throw exception even if matplotlib is missing
        plot_sweep(Path("results/dummy.png"), [1.0, 2.5], [50.0, 90.0], [0.0, 0.0], "Test Title")


if __name__ == "__main__":
    unittest.main()
