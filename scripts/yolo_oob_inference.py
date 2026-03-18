from typing import List
from pathlib import Path
import logging
from dataclasses import dataclass
from abc import ABC

from ultralytics import YOLO

@dataclass(frozen=True, slots=True, kw_only=True)
class BasePredictParams(ABC):
    dest_img_dir: Path
    weights_path: Path
    device: str
    classes: List[int]
    run_name: str

@dataclass(frozen=True, slots=True, kw_only=True)
class FromDirPredictParams(BasePredictParams):
    src_img_dir: Path

def predict_from_dir(params: FromDirPredictParams):
    """
    Run inference on images from source dir, save results to destination dir.
    """

    params.dest_img_dir.mkdir(parents=True, exist_ok=True)

    src_img_paths: list[Path] = list(params.src_img_dir.iterdir())
    src_img_paths.sort(key=lambda p: p.name)

    model: YOLO = YOLO(params.weights_path)
    for src_path in src_img_paths: # type: Path
        model.predict(
            source=src_path,
            device=params.device,
            classes=params.classes,
            project=params.dest_img_dir,
            name=params.run_name,
            save=True,
            save_txt=True,
            save_conf=True
        )
    logging.info("done")

@dataclass(frozen=True, slots=True, kw_only=True)
class FromTxtPredictParams(BasePredictParams):
    src_txt_path: Path

def predict_from_txt(params: FromTxtPredictParams):
    params.dest_img_dir.mkdir(parents=True)
    model: YOLO = YOLO(params.weights_path)
    model.predict(
        source=params.src_txt_path,
        device=params.device,
        classes=params.classes,
        project=params.dest_img_dir,
        name=params.run_name,
        save=True,
        save_txt=True,
        save_conf=True
    )

