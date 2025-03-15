from abc import ABC, abstractmethod
from typing import Optional

import numpy as np


class FeatureCalculator(ABC):
    def depends_on(self) -> Optional[str]:
        return

    @abstractmethod
    def feed_frame(self, frame: Optional[np.ndarray]) -> Optional[float]:
        pass

    @abstractmethod
    def name(self) -> str:
        pass


class STDCalculator(FeatureCalculator):
    def __init__(self, extractor: str, name: str = None):
        self.extractor = extractor
        self._name = name

    def depends_on(self) -> str:
        return self.extractor

    def feed_frame(self, frame: Optional[np.ndarray]) -> Optional[float]:
        if frame is not None:
            return float(frame.std())

    def name(self) -> str:
        return self._name or f'{self.extractor}_std'


class MeanCalculator(FeatureCalculator):
    def __init__(self, extractor: str, name: str = None):
        self.extractor = extractor
        self._name = name

    def depends_on(self) -> str:
        return self.extractor

    def feed_frame(self, frame: Optional[np.ndarray]) -> Optional[float]:
        if frame is not None:
            return float(frame.mean())

    def name(self) -> str:
        return self._name or f'{self.extractor}_mean'


class FHV13Calculator(FeatureCalculator):
    def depends_on(self) -> Optional[str]:
        return 'FHV13_frames'

    def name(self) -> str:
        return 'FHV13'

    def feed_frame(self, frame: Optional[np.ndarray]) -> Optional[float]:
        if frame is None:
            return

        hv_mask = frame[:, :, 0]
        not_hv_mask = frame[:, :, 1]
        return max(float(hv_mask.mean()), 3) / max(not_hv_mask.mean(), 3)
