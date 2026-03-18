from enum import IntEnum
from typing import List, TypeAlias, Tuple, NamedTuple, Sequence, Dict, Any

import cv2
import numpy as np
from numpy import float64
from numpy.typing import NDArray

from common.utils.custom_types import PlanarPosition
from tram_analytics.v1.pipeline.components.vehicle_info.coord_conversion.homography_config import (
    DefiningPointsConfig, PointConfigItem, image_point_from_config, world_point_from_config, HomographyConfig,
    get_cv2_kwargs_for_homography
)

ImageWorldCoordTuple: TypeAlias = Tuple[PlanarPosition, PlanarPosition]
CoordConverterDefiningPoints: TypeAlias = List[ImageWorldCoordTuple]

class Homographies(NamedTuple):
    # shape: (3, 3)
    image_to_world: NDArray[float64]
    # shape: (3, 3)
    world_to_image: NDArray[float64]

class ConversionDirection(IntEnum):
    IMAGE_TO_WORLD = 1
    WORLD_TO_IMAGE = 2


def _cartesian_to_homogeneous(src: NDArray[float64]) -> NDArray[float64]:
    if src.size == 0:
        return np.empty(shape=(0, ), dtype=float64)
    if len(src.shape) != 2:
        raise ValueError("src must be a 2-dimensional array")
    # shape of src: (num_points, num_dims)
    # shape of dest: (num_points, num_dims+1)

    # shape: (num_points, 1)
    num_points: int = src.shape[0]
    Z: NDArray[float64] = np.ones(shape=(num_points, 1), dtype=float64)
    dest: NDArray[float64] = np.concatenate([src, Z], axis=1, dtype=float64)
    return dest

def _homogeneous_to_cartesian(src: NDArray[float64]) -> NDArray[float64]:
    # shape of src: (num_points, num_dims)
    # shape of dest: (num_points, num_dims-1)
    if src.size == 0:
        return np.empty(shape=(0, ), dtype=float64)
    if not (len(src.shape) == 2 and src.shape[-1] > 1):
        raise ValueError("src must be a 2-dimensional array with dim 1 of length greater than 1")
    num_dims_cartesian: int = src.shape[-1] - 1
    # shape: (num_points, )
    Z: NDArray[float64] = src[:, -1]
    # shape: (num_points, 1)
    Z_expanded = np.expand_dims(Z, axis=1)
    # shape: (num_points, num_dims_cartesian)
    Z_mask: NDArray[float64] = np.repeat(Z_expanded, repeats=num_dims_cartesian, axis=1)
    # shape: (num_points, num_dims_cartesian)
    homogeneous_without_z: NDArray[float64] = src[:, :-1]
    # shape: (num_points, num_dims_cartesian)
    cartesian: NDArray[float64] = np.divide(homogeneous_without_z, Z_mask)
    return cartesian


class CoordConverter:

    def __init__(self, config: HomographyConfig) -> None:
        self._homographies: Homographies = self._build_homographies(config)

    @staticmethod
    def _config_input_to_arrays_old(config: CoordConverterDefiningPoints) -> Tuple[NDArray[float64], NDArray[float64]]:
        img_positions: List[PlanarPosition] = []
        world_positions: List[PlanarPosition] = []
        for img_pos, world_pos in config: # type: PlanarPosition, PlanarPosition
            img_positions.append(img_pos)
            world_positions.append(world_pos)
        # shape: (num_points, 2)
        img_pts_numpy: NDArray[float64] = np.array(img_positions, dtype=float64)
        # shape: (num_points, 2)
        world_pts_numpy: NDArray[float64] = np.array(world_positions, dtype=float64)
        return img_pts_numpy, world_pts_numpy

    @staticmethod
    def _config_input_to_arrays(def_points_config: DefiningPointsConfig) -> Tuple[NDArray[float64], NDArray[float64]]:
        img_positions: List[PlanarPosition] = []
        world_positions: List[PlanarPosition] = []
        for item in def_points_config.root: # type: PointConfigItem
            img_pos: PlanarPosition = image_point_from_config(item.image)
            world_pos: PlanarPosition = world_point_from_config(item.world)
            img_positions.append(img_pos)
            world_positions.append(world_pos)
        # shape: (num_points, 2)
        img_pts_numpy: NDArray[float64] = np.array(img_positions, dtype=float64)
        # shape: (num_points, 2)
        world_pts_numpy: NDArray[float64] = np.array(world_positions, dtype=float64)
        return img_pts_numpy, world_pts_numpy

    def _build_homographies(self, config: HomographyConfig) -> Homographies:
        img_pts, world_pts = self._config_input_to_arrays(config.defining_points) # type: NDArray[float64], NDArray[float64]
        cv2_kwargs: Dict[str, Any] = get_cv2_kwargs_for_homography(config.method)
        img_to_world, mask = cv2.findHomography(
            srcPoints=img_pts, dstPoints=world_pts, **cv2_kwargs
        ) # type: NDArray[float64], NDArray[float64] | None
        if img_to_world.size == 0:
            raise ValueError("Could not build a homography from the provided points")
        world_to_img: NDArray[float64] = np.linalg.inv(img_to_world)
        return Homographies(image_to_world=img_to_world, world_to_image=world_to_img)

    def _convert_coords_numpy(self, src_euclidean: NDArray[float64],
                              *, direction: ConversionDirection) -> NDArray[float64]:
        # shape of input: (num_points, 2)
        # shape of output: (num_points, 2)
        if len(src_euclidean) == 0:
            return np.empty(shape=(0, ), dtype=float64)
        # shape: (3, 3)
        homography: NDArray[float64] = (
            self._homographies.image_to_world if direction is ConversionDirection.IMAGE_TO_WORLD
            else self._homographies.world_to_image
        )
        # shape: (num_points, 3)
        # src_homogeneous: NDArray[float64] = cv2.convertPointsToHomogeneous(src_euclidean).squeeze(axis=1)
        src_homogeneous: NDArray[float64] = _cartesian_to_homogeneous(src_euclidean)
        # shape: (3, num_points)
        dest_homogeneous_transposed: NDArray[float64] = np.dot(
            # shape: (3, 3)
            homography,
            # shape: (3, num_points)
            src_homogeneous.T
        )
        # shape: (num_points, 3)
        dest_homogeneous: NDArray[float64] = dest_homogeneous_transposed.T
        # shape: (num_points, 2)
        # dest_euclidean: NDArray[float64] = cv2.convertPointsFromHomogeneous(dest_homogeneous).squeeze(axis=1)
        dest_euclidean: NDArray[float64] = _homogeneous_to_cartesian(dest_homogeneous)
        return dest_euclidean

    def _convert_coords(self, src_coords: Sequence[PlanarPosition],
                        *, direction: ConversionDirection) -> List[PlanarPosition]:
        src_numpy: NDArray[float64] = np.array(src_coords, dtype=float64)
        dest_numpy: NDArray[float64] = self._convert_coords_numpy(src_numpy, direction=direction)
        dest_coords: List[PlanarPosition] = [
            (coord_list[0], coord_list[1])
            for coord_list in dest_numpy.tolist()
        ]
        return dest_coords

    def image_to_world(self, img_coords: List[PlanarPosition]) -> List[PlanarPosition]:
        return self._convert_coords(img_coords, direction=ConversionDirection.IMAGE_TO_WORLD)

    def world_to_image(self, img_coords: List[PlanarPosition]) -> List[PlanarPosition]:
        return self._convert_coords(img_coords, direction=ConversionDirection.WORLD_TO_IMAGE)