import logging

import yaml

LOGGER = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
LOGGER.addHandler(ch)


class Configuration:
    def __init__(self, loglevel="INFO"):
        self.loglevel = "INFO"
        self.base_checks = {}
        self.table_checks = {}
        LOGGER.setLevel(getattr(logging, loglevel))

    def read_config_file(self, config_file_name: str) -> int:
      
        try:
            with open(config_file_name, "r") as fd:
                raw_yaml = yaml.safe_load(fd)
            if raw_yaml is None:
                print("Empty configuration file")
                return 2
        except IOError:
            LOGGER.error("[%s] not found", config_file_name)
            raise IOError
        except yaml.YAMLError as exc:
            LOGGER.error("Yaml format error:\n %s", exc)
            raise exc

        # is_all_check_are_known(raw_yaml)

        for k, v in raw_yaml.items():
            if k == "source":
                self.cluster_checks = raw_yaml["source"]
            if k == "target":
                self.base_checks = raw_yaml["target"]
            if k == "loglevel":
                self.table_checks = raw_yaml["loglevel"]
        return 0
