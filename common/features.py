"""Feature building, shared by batch training and the live job.

OWNER: Ansh (Role 2). Not built yet. Spec: CONTRACT.md section 5.

Why it lives in common/: the training data and the live data must get exactly
the same feature columns. If the two paths had separate code, live predictions
would silently go wrong.
"""

from __future__ import annotations


def add_features(df, with_targets: bool = True):
    """Return df with the lag, rolling, time and (optionally) target columns.

    df: Spark DataFrame of clean readings (one row per station per hour).
    with_targets: True for training data; False in the live job (no future known).
    """
    raise NotImplementedError("Role 2 (Ansh) builds this. See CONTRACT.md section 5.")
