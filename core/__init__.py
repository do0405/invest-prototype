#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Core package for the investment screening system.

This package contains core functionality and utilities used throughout
the investment screening and portfolio management system.
"""

from __future__ import annotations

# Import core exceptions for easy access
from .exceptions import (
    SystemException,
    DataException,
    ConfigurationException,
    NetworkException,
    ValidationException,
)

__all__ = [
    'SystemException',
    'DataException',
    'ConfigurationException',
    'NetworkException',
    'ValidationException',
]

__version__ = '1.0.0'