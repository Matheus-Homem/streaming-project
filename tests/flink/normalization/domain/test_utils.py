from datetime import datetime, timezone
from unittest import TestCase

from flink.normalization.domain.utils import to_millis


class TestToMillis(TestCase):

    def test_returns_zero_for_none(self):
        self.assertEqual(to_millis(None), 0)

    def test_treats_a_small_int_as_seconds(self):
        self.assertEqual(to_millis(1784290892), 1784290892000)

    def test_treats_a_large_int_as_already_millis(self):
        self.assertEqual(to_millis(1784290892000), 1784290892000)

    def test_can_convert_a_z_suffixed_iso_string(self):
        self.assertEqual(to_millis("2026-07-17T12:21:32Z"), 1784290892000)

    def test_assumes_utc_for_an_offset_naive_iso_string(self):
        self.assertEqual(to_millis("2026-07-17T12:21:32"), 1784290892000)

    def test_can_convert_a_tz_aware_datetime(self):
        dt = datetime(2026, 7, 17, 12, 21, 32, tzinfo=timezone.utc)

        self.assertEqual(to_millis(dt), 1784290892000)

    def test_assumes_utc_for_a_naive_datetime(self):
        dt = datetime(2026, 7, 17, 12, 21, 32)

        self.assertEqual(to_millis(dt), 1784290892000)

    def test_raises_for_an_unsupported_type(self):
        with self.assertRaises(TypeError):
            to_millis(["not", "a", "timestamp"])
