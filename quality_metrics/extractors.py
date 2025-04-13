from abc import ABC, abstractmethod
from typing import Optional

import numpy as np


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


class UExtractor(Extractor):
    """
    Takes only U component
    """

    def extract(self, frame: np.ndarray) -> np.ndarray:
        if frame.ndim == 3:
            # take only U component
            frame = frame[1]

        assert frame.ndim == 2, frame.ndim
        return frame

    def name(self) -> str:
        return 'U'


class VExtractor(Extractor):
    """
    Takes only V component
    """

    def extract(self, frame: np.ndarray) -> np.ndarray:
        if frame.ndim == 3:
            # take only V component
            frame = frame[2]

        assert frame.ndim == 2, frame.ndim
        return frame

    def name(self) -> str:
        return 'V'
