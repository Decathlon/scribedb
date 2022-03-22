import argparse
import os

from unittest import TestCase

from scribedb import (Compare)


class TestScribedb(TestCase):
    """Provide unit tests for the TestScribedb."""

    def test_pg_connection(self):
        """Test connection to postgres."""
        args = argparse.Namespace(
            filename=f"{os.path.dirname(__file__)}/testsresources/default_config.yaml",
        )
        compare = Compare.parse_file(args.filename)
        self.assertEqual(compare.source.postgres.username, "postgres")
