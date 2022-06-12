import logging

import yaml
import json

LOGGER = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
LOGGER.addHandler(ch)


class Configuration:

    def __init__(self, loglevel="INFO"):
        self.loglevel = "INFO"
        LOGGER.setLevel(getattr(logging, loglevel))

    def json_config(self,config_file_name: str) -> dict:
        try:
            with open(config_file_name, "r") as fd:
                raw_yaml = yaml.safe_load(fd)
            if raw_yaml is None:
                raise ValueError(
                    f"{config_file_name} is empty."
                ) from Exception
        except IOError:
            raise ValueError(
                    f"{config_file_name} not found."
            ) from Exception
        except yaml.YAMLError as exc:
            raise ValueError(
                    f"{config_file_name} yaml not wel formed."
            ) from Exception

        return json.dumps(raw_yaml)
