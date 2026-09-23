import unittest

import numpy as np

from oanda_multitimeframe_crossover_stack_sweep import (
    CrossSpec,
    build_stack_configurations,
    combine_alignment_masks,
    context_timeframes_for,
    parse_cross_spec,
)


class MultiTimeframeStackTests(unittest.TestCase):
    def test_extended_context_ranges_are_spread_out(self):
        self.assertEqual(
            context_timeframes_for("M1"),
            ("M3", "M12", "H1"),
        )
        self.assertEqual(
            context_timeframes_for("M5"),
            ("M15", "H1", "H4"),
        )

    def test_cross_spec_supports_mixed_ma_kinds(self):
        self.assertEqual(
            parse_cross_spec("ema-sma:8:21"),
            CrossSpec("ema", "sma", 8, 21),
        )

    def test_configuration_builder_includes_zero_one_and_two_contexts(self):
        configs = build_stack_configurations(
            ["M1"],
            [CrossSpec("ema", "ema", 5, 13)],
            ["cross_only"],
        )
        self.assertEqual(
            {len(config.context_timeframes) for config in configs},
            {0, 1, 2},
        )
        keys = {
            (
                config.timeframe,
                config.trigger_spec,
                config.context_timeframes,
                config.context_specs,
            )
            for config in configs
        }
        self.assertEqual(len(keys), len(configs))
        self.assertTrue(
            any(
                config.context_specs
                and config.context_specs[0]
                != config.trigger_spec
                for config in configs
            )
        )

    def test_alignment_masks_require_every_context(self):
        trigger = np.array([True, True, False, True])
        context_one = np.array([True, False, True, True])
        context_two = np.array([True, True, True, False])
        combined = combine_alignment_masks(
            trigger,
            [context_one, context_two],
        )
        np.testing.assert_array_equal(
            combined,
            np.array([True, False, False, False]),
        )


if __name__ == "__main__":
    unittest.main()
