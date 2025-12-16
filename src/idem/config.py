from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn, TypedDict, cast

import yaml


class RawAppConfig(TypedDict):
    root_paths: list[str]
    db_path: str
    max_workers: int
    max_inflight: int
    chunk_size: int
    batch_size: int


class RawConfigFile(TypedDict):
    config: RawAppConfig


CONFIG_FILENAME: Path = Path("config.yaml")


def type_error(value: object) -> NoReturn:
    raise TypeError(f"Unexpected value of wrong type: {value!r}")


@dataclass(slots=True)
class AppConfig:
    root_paths: list[Path]
    db_path: Path
    max_workers: int
    max_inflight: int
    chunk_size: int
    batch_size: int

    @staticmethod
    def load(path: Path = CONFIG_FILENAME) -> AppConfig:
        if not path.exists():
            raise FileNotFoundError("Missing config file. Run idem init first.")

        with path.open("r", encoding="UTF-8") as f:
            raw_loaded_obj: object | None = cast(object, yaml.safe_load(f))

        if not raw_loaded_obj:
            raise ValueError("Config file is empty or invalid YAML.")

        if not isinstance(raw_loaded_obj, dict):
            type_error(raw_loaded_obj)

        raw_dict: dict[str, object] = cast(dict[str, object], raw_loaded_obj)

        cfg_raw: object | None = raw_dict.get("config")
        if not isinstance(cfg_raw, dict):
            type_error(cfg_raw)

        cfg: RawAppConfig = cast(RawAppConfig, cast(object, cfg_raw))

        appConfig: AppConfig = AppConfig(
            root_paths=[Path(path) for path in cfg["root_paths"]],
            db_path=Path(cfg["db_path"]),
            max_workers=cfg["max_workers"],
            max_inflight=cfg["max_inflight"],
            chunk_size=cfg["chunk_size"],
            batch_size=cfg["batch_size"],
        )

        return appConfig

    def save(self, path: Path = CONFIG_FILENAME) -> None:
        raw: RawConfigFile = {"config": self.to_raw()}
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(raw, f, sort_keys=False)

    def to_raw(self) -> RawAppConfig:
        return {
            "root_paths": [str(path) for path in self.root_paths],
            "db_path": self.db_path.name,
            "max_workers": self.max_workers,
            "max_inflight": self.max_inflight,
            "chunk_size": self.chunk_size,
            "batch_size": self.batch_size,
        }
