"""Codebook construction for PolarQuant.

Derived from the Apache-2.0 `turboquant_plus` reference implementation, but adapted
to avoid a SciPy dependency so Mission Control can vendor and validate the
algorithm in-place.

After random rotation, each coordinate follows Beta(d/2, d/2) on [-1/√d, 1/√d],
which converges to N(0, 1/d) for large d. We use optimal scalar quantizers for this
distribution.

Paper provides closed-form centroids for 1-bit and 2-bit. For higher bit-widths,
we use Lloyd's algorithm on the Gaussian approximation.
"""

import math

import numpy as np


def optimal_centroids(bit_width: int, d: int) -> np.ndarray:
    """Compute optimal MSE centroids for the post-rotation coordinate distribution.

    Args:
        bit_width: Number of bits per coordinate (1, 2, 3, 4, ...).
        d: Vector dimension (affects centroid scale).

    Returns:
        Sorted array of 2^bit_width centroids.
    """
    n_centroids = 1 << bit_width

    if bit_width == 1:
        c = np.sqrt(2.0 / (np.pi * d))
        return np.array([-c, c])

    if bit_width == 2:
        return np.array([-1.51, -0.453, 0.453, 1.51]) / np.sqrt(d)

    # For b >= 3, use Lloyd's algorithm on N(0, 1/d)
    return _lloyds_gaussian(n_centroids, sigma=1.0 / np.sqrt(d))


def _lloyds_gaussian(n_centroids: int, sigma: float, n_iter: int = 100) -> np.ndarray:
    """Lloyd's algorithm (iterative k-means) for optimal scalar quantization of N(0, sigma²).

    Args:
        n_centroids: Number of quantization levels (2^b).
        sigma: Standard deviation of the Gaussian.
        n_iter: Number of Lloyd iterations.

    Returns:
        Sorted array of optimal centroids.
    """
    # Initialize boundary positions from uniform quantiles
    boundaries = np.array(
        [_gaussian_ppf(prob, sigma) for prob in np.linspace(0, 1, n_centroids + 1)[1:-1]],
        dtype=np.float64,
    )
    centroids = np.zeros(n_centroids)

    # Initial centroids: conditional expectations within each region
    centroids[0] = _gaussian_conditional_expectation(sigma, -np.inf, boundaries[0])
    for i in range(1, n_centroids - 1):
        centroids[i] = _gaussian_conditional_expectation(sigma, boundaries[i - 1], boundaries[i])
    centroids[-1] = _gaussian_conditional_expectation(sigma, boundaries[-1], np.inf)

    for _ in range(n_iter):
        # Update boundaries (midpoints between consecutive centroids)
        boundaries = (centroids[:-1] + centroids[1:]) / 2.0

        # Update centroids (conditional expectations within each region)
        centroids[0] = _gaussian_conditional_expectation(sigma, -np.inf, boundaries[0])
        for i in range(1, n_centroids - 1):
            centroids[i] = _gaussian_conditional_expectation(sigma, boundaries[i - 1], boundaries[i])
        centroids[-1] = _gaussian_conditional_expectation(sigma, boundaries[-1], np.inf)

    return np.sort(centroids)


def _gaussian_conditional_expectation(sigma: float, a: float, b: float) -> float:
    """E[X | a < X < b] where X ~ N(0, sigma²).

    Uses the formula: E[X | a < X < b] = sigma² * (φ(a/σ) - φ(b/σ)) / (Φ(b/σ) - Φ(a/σ))
    where φ is the PDF and Φ is the CDF of standard normal.
    """
    a_std = a / sigma if np.isfinite(a) else a
    b_std = b / sigma if np.isfinite(b) else b

    # Use sf() for upper tail to avoid CDF cancellation at extreme values
    # prob = P(a < X/σ < b) using the more numerically stable formulation
    if not math.isfinite(a_std):
        prob = _standard_normal_cdf_scalar(b_std)
    elif not math.isfinite(b_std):
        prob = 1.0 - _standard_normal_cdf_scalar(a_std)
    else:
        prob = _standard_normal_cdf_scalar(b_std) - _standard_normal_cdf_scalar(a_std)

    if prob < 1e-15:
        # For semi-infinite intervals, use asymptotic approximation
        if np.isfinite(a) and not np.isfinite(b):
            return a + sigma  # E[X | X > a] ≈ a + σ for extreme a
        elif not np.isfinite(a) and np.isfinite(b):
            return b - sigma
        elif np.isfinite(a) and np.isfinite(b):
            return (a + b) / 2.0
        else:  # pragma: no cover — both infinite always has prob=1
            return 0.0

    pdf_diff = _standard_normal_pdf_scalar(a_std) - _standard_normal_pdf_scalar(b_std)
    return sigma * pdf_diff / prob


def _standard_normal_pdf_scalar(value: float) -> float:
    if not math.isfinite(value):
        return 0.0
    return math.exp(-0.5 * value * value) / math.sqrt(2.0 * math.pi)


def _standard_normal_cdf_scalar(value: float) -> float:
    if value == float("inf"):
        return 1.0
    if value == float("-inf"):
        return 0.0
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _gaussian_ppf(probability: float, sigma: float) -> float:
    """Approximate the inverse Gaussian CDF with a stable binary search."""
    probability = float(probability)
    if probability <= 0.0:
        return float("-inf")
    if probability >= 1.0:
        return float("inf")
    low = -12.0 * sigma
    high = 12.0 * sigma
    for _ in range(96):
        mid = (low + high) / 2.0
        if _standard_normal_cdf_scalar(mid / sigma) < probability:
            low = mid
        else:
            high = mid
    return (low + high) / 2.0


def nearest_centroid_indices(values: np.ndarray, centroids: np.ndarray) -> np.ndarray:
    """Find nearest centroid index for each value. Vectorized.

    Args:
        values: Array of values to quantize, shape (...).
        centroids: Sorted centroid array, shape (n_centroids,).

    Returns:
        Integer indices into centroids array, same shape as values.
    """
    # Use searchsorted for sorted centroids — O(n log k) instead of O(n * k)
    # Find the insertion point, then check left and right neighbors
    boundaries = (centroids[:-1] + centroids[1:]) / 2.0
    return np.searchsorted(boundaries, values.ravel()).reshape(values.shape)
