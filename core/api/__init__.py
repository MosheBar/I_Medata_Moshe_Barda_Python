"""
API package initialization.
"""
from .app import app
from .dependencies import get_db

__all__ = ['app', 'get_db'] 