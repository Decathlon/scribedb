import os

from unittest import TestCase

from main import Compare
from scribedb.configuration import Configuration


PATH = f"{os.path.dirname(__file__)}/yaml"


class TestScribedb(TestCase):
    """Provide unit tests for Main."""

    def test_rdbms_is_pg_and_ora(self):
        config_file = Configuration()
        filename = f"{PATH}/pg_and_ora.yaml"
        raw = config_file.json_config(filename)
        self.assertRaises(Exception, Compare.parse_raw, raw)

    def test_rdbms_is_not_pg_and_ora(self):
        config_file = Configuration()
        filename = f"{PATH}/no_pg_and_ora.yaml"
        raw = config_file.json_config(filename)
        self.assertRaises(Exception, Compare.parse_raw, raw)

    def test_is_ok_config_filter(self):
        config_file = Configuration()
        filename = f"{PATH}/config_filter.yaml"
        raw = config_file.json_config(filename)
        compare = Compare.parse_raw(raw)
        self.assertEqual(compare.source.db.type, "postgres")
        self.assertEqual(compare.target.db.type, "oracle")
        self.assertEqual(compare.source.rowcount(), 2)
        self.assertEqual(compare.target.rowcount(), 2)
        self.assertEqual(compare.target.computed_hash, compare.source.computed_hash)

    def test_is_ok_config_filter_one_col(self):
        config_file = Configuration()
        filename = f"{PATH}/config_filter_one_col.yaml"
        raw = config_file.json_config(filename)
        compare = Compare.parse_raw(raw)
        self.assertEqual(compare.source.db.type, "postgres")
        self.assertEqual(compare.target.db.type, "oracle")
        self.assertEqual(compare.source.rowcount(), 2)
        self.assertEqual(compare.target.rowcount(), 2)
        self.assertEqual(compare.target.computed_hash, compare.source.computed_hash)

    def test_is_nok_default_config(self):
        config_file = Configuration()
        filename = f"{PATH}/default_config.yaml"
        raw = config_file.json_config(filename)

        compare = Compare.parse_raw(raw)
        self.assertEqual(compare.source.db.type, "postgres")
        self.assertEqual(compare.target.db.type, "oracle")
        self.assertEqual(compare.source.rowcount(), 107)
        self.assertEqual(compare.target.rowcount(), 108)
        self.assertNotEqual(compare.target.computed_hash, compare.source.computed_hash)
        self.assertRaises(Exception, Compare.parse_raw, raw)

    def test_is_nok_default_estimate(self):
        config_file = Configuration()
        filename = f"{PATH}/default_config_estimate.yaml"
        raw = config_file.json_config(filename)

        compare = Compare.parse_raw(raw)
        self.assertEqual(compare.source.db.type, "postgres")
        self.assertEqual(compare.target.db.type, "oracle")
        self.assertEqual(compare.source.rowcount(), 107)
        self.assertEqual(compare.target.rowcount(), 108)
        self.assertNotEqual(compare.target.computed_hash, compare.source.computed_hash)
        self.assertRaises(Exception, Compare.parse_raw, raw)


    def test_nb_column_is_different(self):
        config_file = Configuration()
        filename = f"{PATH}/column_dont_match.yaml"
        raw = config_file.json_config(filename)
        self.assertRaises(Exception, Compare.parse_raw, raw)
