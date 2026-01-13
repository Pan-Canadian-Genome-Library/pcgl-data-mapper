"""
Core Mappers Package

Base mapper classes and generic entity mappers for PCGL data mapping.

This package provides the foundation for all entity-specific mappers:
- EntityMapper: Main implementation class with direct and expansion pattern support
- MappingConfig: Configuration container for YAML-based mappings

Design Philosophy:
- Configuration-driven: 80% config, 20% custom code
- Study-independent: Core logic works across all studies
- Extensible: Easy to add study-specific customizations
- Type-safe: Comprehensive type hints and validation

"""

from .base import EntityMapper, MappingConfig, StudyDataMapper

__all__ = [
    'EntityMapper',
    'MappingConfig',
    'StudyDataMapper'
]

__version__ = '1.0.0'
