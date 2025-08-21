#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Core exceptions module for the investment screening system.

This module defines custom exception classes used throughout the application.
"""

from __future__ import annotations


class SystemException(Exception):
    """Base system exception for the investment screening system.
    
    This exception is raised when system-level errors occur that are not
    related to specific business logic but rather to system operations,
    configuration issues, or infrastructure problems.
    """
    
    def __init__(self, message: str, error_code: str = None, details: dict = None):
        """Initialize SystemException.
        
        Args:
            message: Human-readable error message
            error_code: Optional error code for programmatic handling
            details: Optional dictionary with additional error details
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}
    
    def __str__(self) -> str:
        """Return string representation of the exception."""
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message
    
    def __repr__(self) -> str:
        """Return detailed representation of the exception."""
        return f"SystemException(message='{self.message}', error_code='{self.error_code}', details={self.details})"


class DataException(SystemException):
    """Exception raised for data-related errors.
    
    This includes issues with data collection, processing, validation,
    or storage operations.
    """
    pass


class ConfigurationException(SystemException):
    """Exception raised for configuration-related errors.
    
    This includes missing configuration files, invalid configuration values,
    or environment setup issues.
    """
    pass


class NetworkException(SystemException):
    """Exception raised for network-related errors.
    
    This includes API failures, connection timeouts, or external service
    unavailability.
    """
    pass


class ValidationException(SystemException):
    """Exception raised for validation errors.
    
    This includes input validation failures, data format errors,
    or business rule violations.
    """
    pass