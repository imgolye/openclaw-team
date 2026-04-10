"""QJL: Quantized Johnson-Lindenstrauss Transform.

1-bit quantization via random projection → sign to compress vectors while
preserving inner products. Note: this implementation stores a full d×d projection
matrix (O(d²) memory). For large d, a structured/seeded approach would be needed.

Key property: unbiased and optimal at 1-bit.
    Q_qjl(x) = sign(S · x) where S ~ N(0,1)^(d×d)
    Q_qjl_inv(z) = √(π/2) / d · S^T · z
"""

import numpy as np


QJL_CONST = np.sqrt(np.pi / 2)


class QJL:
    """Quantized Johnson-Lindenstrauss 1-bit quantizer.

    Usage:
        qjl = QJL(d=128, seed=42)
        signs, norm = qjl.quantize(residual)
        r_hat = qjl.dequantize(signs, norm)
    """

    def __init__(self, d: int, seed: int = 123):
        """
        Args:
            d: Vector dimension.
            seed: Random seed for projection matrix.
        """
        self.d = d
        rng = np.random.default_rng(seed)
        # Random projection matrix S ∈ R^(d×d), entries ~ N(0,1)
        self.S = rng.standard_normal((d, d))

    def quantize(self, r: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """Quantize residual vector(s) to sign bits.

        Args:
            r: Residual vector(s), shape (d,) or (batch, d).

        Returns:
            (signs, norms) where:
                signs: {+1, -1}^d or (batch, d), stored as int8
                norms: scalar or (batch,) — ||r||_2, needed for dequantization
        """
        single = r.ndim == 1
        if single:
            r = r[np.newaxis, :]

        # Compute norms before projection
        norms = np.linalg.norm(r, axis=1)  # (batch,)

        # Project: S @ r.T → (d, batch), transpose to (batch, d)
        projected = (self.S @ r.T).T

        # Sign quantization: +1 or -1
        signs = np.sign(projected).astype(np.int8)
        # Handle exact zeros (extremely rare) — map to +1
        signs[signs == 0] = 1

        # Zero vectors: signs are meaningless, norm=0 ensures dequantize returns zero
        if single:
            return signs[0], norms[0]
        return signs, norms

    def dequantize(self, signs: np.ndarray, norms: np.ndarray) -> np.ndarray:
        """Dequantize sign bits back to approximate residual.

        Args:
            signs: Sign bits, shape (d,) or (batch, d).
            norms: Residual norms, scalar or (batch,).

        Returns:
            Approximate residual, same shape as original.
        """
        single = signs.ndim == 1
        if single:
            signs = signs[np.newaxis, :].astype(np.float64)
            norms = np.array([norms])
        else:
            signs = signs.astype(np.float64)

        # x̃_qjl = √(π/2) / d · γ · S^T @ signs
        # S^T @ signs.T → (d, batch), transpose to (batch, d)
        reconstructed = (self.S.T @ signs.T).T  # (batch, d)

        # Scale by √(π/2) / m * norm, where m = number of projections (= d here)
        # If projection matrix changes from (d,d) to (m,d), update this divisor
        scale = QJL_CONST / self.d * norms  # (batch,)
        reconstructed *= scale[:, np.newaxis]

        return reconstructed[0] if single else reconstructed
