from abc import ABC, abstractmethod
from typing import Optional

import numpy as np
from scipy import ndimage
from skimage.feature import graycomatrix, graycoprops


class Extractor(ABC):
    def depends_on(self) -> Optional[str]:
        return

    @abstractmethod
    def extract(self, frame: np.ndarray) -> Optional[np.ndarray]:
        pass

    @abstractmethod
    def name(self) -> str:
        pass


class YExtractor(Extractor):
    """
    Takes only Y component
    """

    def extract(self, frame: np.ndarray) -> np.ndarray:
        if frame.ndim == 3:
            # take only Y component
            frame = frame[0]

        assert frame.ndim == 2, frame.ndim
        return frame

    def name(self) -> str:
        return 'Y'


class SIExtractor(Extractor):
    """
    Spatial information
    https://www.itu.int/rec/T-REC-P.910-200804-I/en
    """

    def depends_on(self) -> str:
        return 'Y'

    def extract(self, frame: np.ndarray) -> np.ndarray:
        frame = frame.astype(np.int16)
        sob_x = ndimage.sobel(frame, axis=0)
        sob_y = ndimage.sobel(frame, axis=1)

        return np.hypot(sob_x, sob_y)

    def name(self) -> str:
        return 'SI'


class TICalculator(Extractor):
    """
    Temporal information
    https://www.itu.int/rec/T-REC-P.910-200804-I/en
    """

    def __init__(self):
        self.prev_frame = None

    def depends_on(self) -> str:
        return 'Y'

    def extract(self, frame: np.ndarray) -> Optional[np.ndarray]:
        if self.prev_frame is None:
            self.prev_frame = frame
            return

        ti = frame - self.prev_frame
        self.prev_frame = frame
        return ti

    def name(self) -> str:
        return 'TI'


class GLCMExtractor(Extractor):
    """
    Gray level co-occurrence matrix
    """

    def depends_on(self) -> str:
        return 'Y'

    def extract(self, frame: np.ndarray) -> np.ndarray:
        return graycomatrix(
            frame,
            [1],
            [0, np.pi / 4, np.pi / 2, 3 * np.pi / 4],
            levels=256,
            normed=True
            )

    def name(self) -> str:
        return 'GLCM'


class GLCMPropertyExtractor(Extractor):
    def __init__(self, property_name: str):
        self.property = property_name

    def depends_on(self) -> str:
        return 'GLCM'

    def extract(self, frame: np.ndarray) -> np.ndarray:
        return graycoprops(frame, self.property)

    def name(self) -> str:
        return f'GLCM_{self.property}'
