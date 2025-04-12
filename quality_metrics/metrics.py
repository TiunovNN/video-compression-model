import logging
from abc import ABC, abstractmethod
from typing import Optional

import numpy as np
from scipy import signal, ndimage
from skimage.transform import pyramid_reduce
from skimage.util import img_as_float


class MetricCalculator(ABC):
    def depends_on(self) -> Optional[str]:
        return

    @abstractmethod
    def feed_frame(
        self,
        frame_source: Optional[np.ndarray],
        frame_distorted: Optional[np.ndarray],
    ) -> Optional[float]:
        pass

    @abstractmethod
    def name(self) -> str:
        pass


class MSSSIMCalculator(MetricCalculator):
    """
    Multi-scale Structural Similarity Index Measurement
    https://ece.uwaterloo.ca/~z70wang/publications/msssim.pdf

    max_val: Maximum value of pixels (e.g., 255 for 8-bit images)
    window_size: Size of the Gaussian window
    sigma: Standard deviation of the Gaussian window
    """

    def __init__(
        self,
        extractor: str,
        name: str = None,
        max_val=255,
        window_size=11,
        sigma=1.5,
    ):
        self.extractor = extractor
        self._name = name
        self.max_val = max_val
        # Default weights come from the Wang et al. paper
        self.weights = np.array([0.0448, 0.2856, 0.3001, 0.2363, 0.1333])
        self.levels = len(self.weights)
        self.window = self.gaussian_window(window_size, sigma)
        # Constants from the original paper to stabilize division
        self.C1 = (0.01 * self.max_val) ** 2
        self.C2 = (0.03 * self.max_val) ** 2

    def depends_on(self) -> str:
        return self.extractor

    def feed_frame(
        self,
        frame_source: Optional[np.ndarray],
        frame_distorted: Optional[np.ndarray],
    ) -> Optional[float]:
        image_source = img_as_float(frame_source)
        image_distorted = img_as_float(frame_distorted)

        if image_source.shape != image_distorted.shape:
            raise ValueError("Input images must have the same dimensions")

        mssim = []

        for i in range(self.levels - 1):
            cs_map = self.ssim_single_scale(image_source, image_distorted, False)
            mssim.append(np.mean(cs_map))

            image_source = pyramid_reduce(image_source, preserve_range=True)
            image_distorted = pyramid_reduce(image_distorted, preserve_range=True)

        ssim_map = self.ssim_single_scale(image_source, image_distorted, True)
        mssim.append(np.mean(ssim_map))
        mssim = np.array(mssim)

        ms_ssim_val = np.prod(mssim ** self.weights)

        return float(ms_ssim_val)

    @staticmethod
    def gaussian_window(size, sigma):
        """
        Create a Gaussian window of specified size and standard deviation
        """
        x = np.arange(-(size // 2), size // 2 + 1)
        x_grid, y_grid = np.meshgrid(x, x)
        window = np.exp(-(x_grid ** 2 + y_grid ** 2) / (2 * sigma ** 2))
        return window / np.sum(window)

    def ssim_single_scale(
        self,
        frame_source: np.ndarray,
        frame_distorted: np.ndarray,
        calc_luminance: bool
    ) -> np.ndarray:
        mu1 = signal.fftconvolve(frame_source, self.window, mode='same')
        mu2 = signal.fftconvolve(frame_distorted, self.window, mode='same')

        mu1_sq = mu1 ** 2
        mu2_sq = mu2 ** 2
        mu1_mu2 = mu1 * mu2

        sigma1_sq = signal.fftconvolve(frame_source ** 2, self.window, mode='same') - mu1_sq
        sigma2_sq = signal.fftconvolve(frame_distorted ** 2, self.window, mode='same') - mu2_sq
        sigma12 = signal.fftconvolve(frame_source * frame_distorted, self.window, mode='same') - mu1_mu2

        numerator = 2 * sigma12 + self.C2
        denominator = sigma1_sq + sigma2_sq + self.C2

        if calc_luminance:
            numerator *= (2 * mu1_mu2 + self.C1)
            denominator *= (mu1_sq + mu2_sq + self.C1)

        return numerator / denominator

    def name(self) -> str:
        return self._name or f'{self.extractor}_mssim'
