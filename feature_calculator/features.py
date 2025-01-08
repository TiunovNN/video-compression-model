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
    """
    Spatial information
    https://www.itu.int/rec/T-REC-P.910-200804-I/en
    """

    def feed_frame(self, frame: np.ndarray) -> Optional[float]:
        if frame.ndim == 3:
            # take only Y component
            frame = frame[0]

        assert frame.ndim == 2, frame.ndim
        frame = frame.astype(np.int16)
        sob_x = ndimage.sobel(frame, axis=0)
        sob_y = ndimage.sobel(frame, axis=1)

        result = np.hypot(sob_x, sob_y).std()
        return float(result)

    def name(self) -> str:
        return 'SI'


class TICalculator(FeatureCalculator):
    """
    Temporal information
    https://www.itu.int/rec/T-REC-P.910-200804-I/en
    """

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


class CTICalculator(FeatureCalculator):
    """
    Contrast Information
    """

    def feed_frame(self, frame: np.ndarray) -> Optional[float]:
        if frame.ndim == 3:
            # take only Y component
            frame = frame[0]

        assert frame.ndim == 2, frame.ndim
        return float(frame.std())

    def name(self) -> str:
        return 'CTI'
