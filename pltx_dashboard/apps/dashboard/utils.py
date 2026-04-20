import json
import numpy as np
import pandas as pd
import os
from django.conf import settings


class DashboardEncoder(json.JSONEncoder):
    """Handles numpy/pandas types that the default encoder chokes on."""

    def default(self, obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (pd.Timestamp,)):
            return str(obj)
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        return super().default(obj)


def serialize_payload(payload):
    """Serialise a payload dict so numpy/pandas types don't trip up JSONField."""
    return json.loads(json.dumps(payload, cls=DashboardEncoder))


def clean_currency(x):
    """
    Cleans string representation of currency (e.g. '₹44,275.00', '$100.00') into float.
    Removes commas and currency symbols.
    """
    if isinstance(x, str):
        x = x.replace("₹", "").replace("$", "").replace(",", "").strip()
    try:
        return float(x)
    except (ValueError, TypeError):
        return 0.0


def clean_number(x):
    """
    Cleans string representation of numbers (e.g. '2,559') into integer.
    """
    if isinstance(x, str):
        x = x.replace(",", "").strip()
    try:
        return int(float(x))  # float() handles cases like '10.0'
    except (ValueError, TypeError):
        return 0


def resolve_path(path):
    """Resolve a path relative to the project root if it's not absolute."""
    if not path:
        return None
    # Strip shell-style backslash escaping (e.g. "Devarth\ Nanavaty" → "Devarth Nanavaty")
    path = path.replace("\\ ", " ")
    if os.path.isabs(path):
        return path

    # Try relative to settings.BASE_DIR (pltx_dashboard/)
    p1 = os.path.join(settings.BASE_DIR, path)
    if os.path.exists(p1):
        return p1

    # Try relative to project root (one level up from pltx_dashboard/)
    p2 = os.path.join(os.path.dirname(settings.BASE_DIR), path)
    if os.path.exists(p2):
        return p2

    return p1  # Fallback to default behavior


def extract_days(value):
    """Extract number of days from a string like '15 days'."""
    try:
        return int(str(value).split()[0])
    except Exception:
        return 0
