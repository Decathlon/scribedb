
import os

from unittest import TestCase

from scribedb.configuration import (Configuration)

PATH=f"{os.path.dirname(__file__)}/yaml"


class TestConfiguration(TestCase):
    """Provide unit tests for Configuration."""

    def test_configfile_not_exist(self):
        config_file = Configuration()
        filename=f"{PATH}/not_exists_config.yaml"
        self.assertRaises(Exception, config_file.json_config,config_file_name = filename)

    def test_configfile_empty(self):
        config_file = Configuration()
        filename=f"{PATH}/empty.yaml"
        self.assertRaises(Exception, config_file.json_config,config_file_name = filename)

    def test_configfile_ugly(self):
        config_file = Configuration()
        filename=f"{PATH}/ugly.yaml"
        self.assertRaises(Exception, config_file.json_config,config_file_name = filename)

    def test_configfile_isok(self):
        config_file = Configuration()
        filename=f"{PATH}/default_config.yaml"
        ref='{"loglevel": "INFO", "source": {"postgres": {"host": "localhost", "port": 5432, "username": "postgres", "password": "fddfdfdf", "dbname": "postgres"}, "qry": "select first_name from employee order by first_name;"}, "target": {"oracle": {"host": "localhost", "port": 1521, "username": "system", "password": "Decathlon0147", "instance": "XE"}, "qry": "select first_name from employee order by first_name;"}}'
        sut=config_file.json_config(filename)
        self.assertEquals(sut,ref)
