"""Meaningful whitelist, family-removal and unchanged-context contracts."""
import unittest

import numpy as np

import oanda_rolling_family_design_v1 as design


def sample(rows=80):
    rng = np.random.default_rng(6145)
    z = rng.normal(size=(rows, 50)).astype(np.float32)
    z[::7, 9] = np.nan
    left = np.linspace(.05, .8, rows)
    right = np.linspace(.02, .5, rows)
    ids = np.arange(rows, dtype=np.int64) % 68
    return z, left, right, ids


class FamilyDesignTests(unittest.TestCase):
    def test_exact_family_partition_and_fixed_counts(self):
        m = design.group_manifest()
        self.assertEqual(list(m['groups']), list(design.GROUPS))
        self.assertEqual(len(m['feature_names']), 50)
        self.assertEqual(len(set(m['feature_names'])), 50)
        self.assertEqual([x['kept_technical_fields'] for x in m['groups'].values()],
                         [50, 38, 40, 42, 41, 43, 46, 0])
        # The six single-block removals partition all50 technical fields.
        removed = [n for g in design.GROUPS[1:-1]
                   for n in m['groups'][g]['removed_names']]
        self.assertCountEqual(removed, m['feature_names'])
        self.assertEqual(len(removed), 50)
        self.assertEqual(m['aliases_added'], 0)
        self.assertEqual(m['context_columns'],
                         {'known_entry_long_bps': 50, 'known_entry_short_bps': 51, 'pair_id': 52})

    def test_all_groups_preserve_costs_pair_ids_and_input_arrays(self):
        z, left, right, ids = sample()
        original = z.copy()
        for group in design.GROUPS:
            x = design.build_family_matrix(z, left, right, ids, group)
            self.assertEqual(x.shape, (len(z), 53))
            np.testing.assert_array_equal(x[:, 50], left)
            np.testing.assert_array_equal(x[:, 51], right)
            np.testing.assert_array_equal(x[:, 52], ids)
            keep = design.feature_mask(group)
            np.testing.assert_array_equal(x[:, :50][:, keep], z[:, keep])
            self.assertTrue(np.isnan(x[:, :50][:, ~keep]).all())
        np.testing.assert_array_equal(z, original)

    def test_removed_column_mutation_cannot_change_whole_model_input(self):
        z, left, right, ids = sample()
        for group in design.GROUPS[1:]:
            changed = z.copy()
            changed[:, ~design.feature_mask(group)] = 123456.
            before = design.build_family_matrix(z, left, right, ids, group)
            after = design.build_family_matrix(changed, left, right, ids, group)
            np.testing.assert_array_equal(before, after)

    def test_future_rows_cannot_change_earlier_input_rows(self):
        z, left, right, ids = sample()
        for group in design.GROUPS:
            before = design.build_family_matrix(z, left, right, ids, group)
            later = z.copy(); later[40:] = -2000.
            next_left = left.copy(); next_left[40:] = 17.
            next_right = right.copy(); next_right[40:] = 31.
            next_ids = ids.copy(); next_ids[40:] = 0
            after = design.build_family_matrix(later, next_left, next_right, next_ids, group)
            np.testing.assert_array_equal(before[:40], after[:40])

    def test_peer_removal_matches_compact38_values_without_column_shift(self):
        z, left, right, ids = sample()
        x = design.build_family_matrix(z, left, right, ids, 'drop_peers')
        np.testing.assert_array_equal(x[:, :38], z[:, :38])
        self.assertTrue(np.isnan(x[:, 38:50]).all())
        self.assertFalse(np.isnan(x[:, 50:]).any())
        self.assertTrue(all(n.startswith('peer__') for n in design.feature_names()[38:]))

    def test_context_only_has_no_technical_route_and_meta_arms_are_refused(self):
        z, left, right, ids = sample()
        x = design.build_family_matrix(z, left, right, ids, 'context_only')
        self.assertTrue(np.isnan(x[:, :50]).all())
        self.assertEqual(design.group_manifest()['mean_arms'], ['direct', 'mixture_raw'])
        for arm in ('mixture_calibrated', 'direct_context_hgb', 'specialist_context_hgb',
                    'direct_ridge', 'specialist_ridge'):
            with self.assertRaisesRegex(ValueError, 'base_direct_and_raw_mixture'):
                design.validate_mean_arm(arm)
        for arm in design.MEAN_ARMS:
            self.assertEqual(design.validate_mean_arm(arm), arm)

    def test_aliases_reordering_extra_future_columns_and_unknown_groups_refused(self):
        z, left, right, ids = sample()
        names = design.feature_names()
        reordered = names.copy(); reordered[0], reordered[1] = reordered[1], reordered[0]
        alias = names.copy(); alias[0] = 'm1__return_lag_00_pips'
        for bad in (reordered, alias, names[:-1], names+['future__return_60_bps'], 'compact50'):
            with self.assertRaisesRegex(ValueError, 'ordered_50_canonical'):
                design.build_family_matrix(z, left, right, ids, 'full_compact50', names=bad)
        with self.assertRaisesRegex(ValueError, '50_technical'):
            design.build_family_matrix(np.column_stack((z, np.ones(len(z)))), left, right, ids, 'full_compact50')
        with self.assertRaisesRegex(ValueError, 'undeclared_family'):
            design.build_family_matrix(z, left, right, ids, 'auto_best')
        with self.assertRaisesRegex(ValueError, '53_column'):
            design.apply_group_mask(np.ones((3, 54)), 'full_compact50')

    def test_invalid_current_context_is_not_hidden_by_removal(self):
        z, left, right, ids = sample()
        for value in (np.nan, np.inf, -.1):
            bad = left.copy(); bad[0] = value
            with self.assertRaisesRegex(ValueError, 'known_entry_costs'):
                design.build_family_matrix(z, bad, right, ids, 'context_only')
        for value in (-1., 68., 1.5, np.nan):
            bad = ids.astype(float); bad[0] = value
            with self.assertRaisesRegex(ValueError, '68_pair'):
                design.build_family_matrix(z, left, right, bad, 'context_only')
        bad = z.copy(); bad[0, 0] = np.inf
        with self.assertRaisesRegex(ValueError, 'infinity'):
            design.build_family_matrix(bad, left, right, ids, 'context_only')
        with self.assertRaisesRegex(ValueError, 'same_origin_numeric'):
            design.build_family_matrix(z, left[:-1], right, ids, 'full_compact50')

    def test_manifest_and_masks_are_fresh_copies_and_missingness_survives(self):
        one = design.group_manifest()
        one['feature_names'][0] = 'future_bad'
        one['lineage'][0]['aliases_not_added_as_inputs'].append('future_bad')
        again = design.group_manifest()
        self.assertNotEqual(again['feature_names'][0], 'future_bad')
        self.assertNotIn('future_bad', again['lineage'][0]['aliases_not_added_as_inputs'])
        a = design.feature_mask('drop_peers'); a[:] = False
        self.assertEqual(int(design.feature_mask('drop_peers').sum()), 38)
        z, left, right, ids = sample(0)
        self.assertEqual(design.build_family_matrix(z, left, right, ids, 'full_compact50').shape, (0, 53))
        with self.assertRaisesRegex(ValueError, '53_column'):
            design.apply_group_mask(np.zeros((2, 53), dtype=np.int64), 'full_compact50')


if __name__ == '__main__':
    unittest.main()
