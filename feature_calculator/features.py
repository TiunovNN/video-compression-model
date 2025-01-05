from abc import ABC, abstractmethod
from typing import Optional

import numpy as np
from scipy import ndimage


class FeatureCalculator(ABC):
    @abstractmethod
    def feed_frame(self, frame: np.ndarray) -> Optional[float]:
        pass

    @abstractmethod
    def name(self) -> str:
        pass


class SICalculator(FeatureCalculator):
    def feed_frame(self, frame: np.ndarray) -> Optional[float]:
        if frame.ndim == 3:
            # take only Y component
            frame = frame[0]

        assert frame.ndim == 2, frame.ndim
        sob_x = ndimage.sobel(frame, axis=0)
        sob_y = ndimage.sobel(frame, axis=1)

        # crop output to valid window, calculate gradient magnitude
        t = np.hypot(sob_x.astype(np.float64), sob_y.astype(np.float64))
        t = t[1:-1, 1:-1]
        result = t.std()
        return float(result)

    def name(self) -> str:
        return 'SI'


class TICalculator(FeatureCalculator):
    def __init__(self):
        self.prev_frame = None

    def feed_frame(self, frame: np.ndarray) -> Optional[float]:
        if frame.ndim == 3:
            # take only Y component
            frame = frame[0]

        if self.prev_frame is None:
            self.prev_frame = frame
            return

        ti = (frame - self.prev_frame).std()
        self.prev_frame = frame
        return float(ti)

    def name(self) -> str:
        return 'TI'
