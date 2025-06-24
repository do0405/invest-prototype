"""Mark Minervini screening package."""

from .filter_stock import run_integrated_screening
from .advanced_financial import run_advanced_financial_screening
from .pattern_detection import run_pattern_detection_on_financial_results

__all__ = [
    "run_integrated_screening",
    "run_advanced_financial_screening",
    "run_pattern_detection_on_financial_results",
]
__version__ = "1.0.0"

