#!/usr/bin/env python3
"""Acceptance-oracle regression tests; no workload/build subprocesses."""
import math
import unittest

from compare import assess_no_regression


def summary(repetitions=10):
    metrics = {}
    for name in ("p50_us", "p95_us", "p99_us", "operations_per_second"):
        before, after = (100.0, 110.0) if name == "operations_per_second" else (10.0, 9.0)
        metrics[name] = {"pairs": [
            {"before": before, "after": after}
            for _ in range(repetitions)
        ]}
    return [{"scenario": ["read", 4096, 1, 1, 0], "metrics": metrics}]


class PerformanceAcceptanceTest(unittest.TestCase):
    def test_accepts_consistent_improvement(self):
        self.assertTrue(assess_no_regression(summary(), 10)["passed"])

    def test_latency_growth_is_not_hidden_by_throughput(self):
        data = summary()
        for pair in data[0]["metrics"]["p99_us"]["pairs"]:
            pair["after"] = 11.0
        result = assess_no_regression(data, 10)
        self.assertFalse(result["passed"])
        self.assertEqual(result["metrics"][2]["status"], "observed_regression")

    def test_throughput_loss_is_not_hidden_by_latency(self):
        data = summary()
        for pair in data[0]["metrics"]["operations_per_second"]["pairs"]:
            pair["after"] = 90.0
        result = assess_no_regression(data, 10)
        self.assertFalse(result["passed"])
        self.assertEqual(result["metrics"][3]["status"], "observed_regression")

    def test_noise_and_rounding_ties_are_not_passes(self):
        for after in (10.0, 11.0):
            data = summary()
            data[0]["metrics"]["p50_us"]["pairs"][-1]["after"] = after
            result = assess_no_regression(data, 10)
            self.assertFalse(result["passed"])
            self.assertEqual(result["metrics"][0]["status"], "inconclusive")

    def test_five_improved_pairs_are_insufficient(self):
        result = assess_no_regression(summary(5), 5)
        self.assertFalse(result["passed"])
        self.assertTrue(all(
            x["status"] == "insufficient_repetitions" for x in result["metrics"]))

    def test_rejects_missing_or_nonfinite_observation(self):
        for value in (math.nan, math.inf, -1.0):
            data = summary()
            data[0]["metrics"]["p50_us"]["pairs"][0]["after"] = value
            with self.assertRaises(ValueError):
                assess_no_regression(data, 10)
        data = summary()
        data[0]["metrics"]["p50_us"]["pairs"].pop()
        with self.assertRaises(ValueError):
            assess_no_regression(data, 10)


if __name__ == "__main__":
    unittest.main()
