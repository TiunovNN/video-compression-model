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


class FHV13Extractor(Extractor):
    """
    Spatial gradient HV13

    Video Quality Measurement Techniques
    Stephen Wolf
    Margaret Pinson
    """
    BANDPASS_FILTER_WEIGHT = [
        -.0052625,
        -.0173446,
        -.0427401,
        -.0768961,
        -.0957739,
        -.0696751,
        0,
        .0696751,
        .0957739,
        .0768961,
        .0427401,
        .0173446,
        .0052625,
    ]
    # Порог восприятия для SI13
    P_si13 = 12
    # Порог восприятия для HV
    P_hv = 3
    # Параметры для расчета f_HV13
    DELTA_THETA = 0.225  # радиан
    R_MIN = 20

    @classmethod
    def generate_sobel_13(cls):
        matrix = np.repeat(np.array(cls.BANDPASS_FILTER_WEIGHT)[np.newaxis, :], 13, axis=0)
        return matrix

    def __init__(self):
        self.sobel_13_x = self.generate_sobel_13()
        self.sobel_13_y = self.generate_sobel_13().T

    def depends_on(self) -> Optional[str]:
        return 'Y'

    def name(self) -> str:
        return 'FHV13_frames'

    def extract(self, frame: np.ndarray) -> Optional[np.ndarray]:
        gx = ndimage.convolve(frame, self.sobel_13_x, mode='reflect')
        gy = ndimage.convolve(frame, self.sobel_13_y, mode='reflect')
        R = np.hypot(gx, gy)
        theta = np.arctan2(gx, gy)
        hv_mask = np.zeros_like(R)
        for m in range(4):
            # Угловой сектор для горизонтальных и вертикальных градиентов
            angle_center = m * np.pi / 2
            angle_min = angle_center - self.DELTA_THETA
            angle_max = angle_center + self.DELTA_THETA

            # Применяем маску для текущего квадранта
            mask_condition = (R >= self.R_MIN) & (theta > angle_min) & (theta < angle_max)
            hv_mask[mask_condition] = R[mask_condition]
        not_hv_mask = np.zeros_like(R)
        for m in range(4):
            # Угловой сектор для диагональных градиентов
            angle_center = m * np.pi / 2
            angle_min = angle_center + self.DELTA_THETA
            angle_max = angle_center - np.pi / 2 + self.DELTA_THETA

            # Применяем маску для текущего диагонального сектора
            mask_condition = (R >= self.R_MIN) & (theta >= angle_min) & (theta <= angle_max)
            not_hv_mask[mask_condition] = R[mask_condition]

        return np.dstack((hv_mask, not_hv_mask))
