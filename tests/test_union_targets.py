import unittest

import wrap_dsqgen


class UnionTargetTests(unittest.TestCase):
    def test_default_targets(self) -> None:
        # Workload set = the paper's five fan-ins (107-query composition).
        self.assertEqual([2, 5, 10, 20, 200], wrap_dsqgen._resolve_union_targets(None))

    def test_capped_targets(self) -> None:
        # --union-max-inputs selects the E3 log2 ladder, capped (cap kept if not pow2).
        self.assertEqual([2, 4, 8, 16, 32, 50], wrap_dsqgen._resolve_union_targets(50))
        self.assertEqual(
            [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048],
            wrap_dsqgen._resolve_union_targets(2048),
        )

    def test_no_targets_when_cap_too_small(self) -> None:
        self.assertEqual([], wrap_dsqgen._resolve_union_targets(1))


if __name__ == "__main__":
    unittest.main()
