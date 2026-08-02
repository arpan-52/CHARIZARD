# charizard/utils/general/config_parser.py
"""
Configuration parser for Charizard pipeline.

Parses:
- pokedex.yaml (main config)
- models.yaml (optional user polcal models)

Keeps raw flow dict - charizard.py reads it directly.
"""

import os
import yaml
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any


@dataclass
class PipelineConfig:
    """Complete pipeline configuration"""

    # Paths
    ms_path: str
    ms_name: str
    working_dir: str

    # Raw config sections
    environment: Dict[str, Any]
    data: Dict[str, Any]
    sources: Dict[str, Any]
    flow: Dict[str, Any]

    # Parsed convenience fields
    target_spws: int
    resources: Dict[str, Dict]

    # Container config (udocker)
    container: Dict[str, Any] = field(default_factory=dict)

    # Source overrides (parsed for convenience)
    auto_detect: bool = True
    calibrator_overrides: Dict[str, Any] = field(default_factory=dict)
    uvrange_overrides: Dict[str, str] = field(default_factory=dict)

    # User models (from models.yaml)
    user_models: Dict[str, Any] = field(default_factory=dict)


def parse_config(pokedex_path: str, models_path: Optional[str] = None) -> PipelineConfig:
    """
    Parse pokedex.yaml and optional models.yaml, return PipelineConfig.
    
    Args:
        pokedex_path: Path to pokedex config file
        models_path: Optional path to user models file
    
    Returns:
        PipelineConfig with all settings
    """
    
    with open(pokedex_path, 'r') as f:
        pokedex = yaml.safe_load(f)
    
    # Extract sections
    environment = pokedex.get('environment', {})
    data = pokedex.get('data', {})
    sources = pokedex.get('sources', {})
    flow = pokedex.get('flow', {})
    
    # MS path
    ms_path = data.get('ms', '')
    ms_name = os.path.basename(ms_path).replace('.ms', '')
    
    # Working directory
    working_dir = environment.get('working_dir', './')
    if not os.path.isabs(working_dir):
        working_dir = os.path.abspath(working_dir)
    
    # Resources with defaults
    default_resources = {
        'default': {'nodes': 1, 'ppn': 4, 'walltime': '04:00:00', 'mem_gb': 128},
        'flagging': {'nodes': 1, 'ppn': 8, 'walltime': '08:00:00', 'mem_gb': 256},
        'crosscal': {'nodes': 1, 'ppn': 8, 'walltime': '12:00:00', 'mem_gb': 128},
        'selfcal': {'nodes': 1, 'ppn': 4, 'walltime': '12:00:00', 'mem_gb': 128},
        'imaging': {'nodes': 1, 'ppn': 8, 'walltime': '24:00:00', 'mem_gb': 512},
    }
    
    resources = {k: dict(v) for k, v in default_resources.items()}
    # 'resources' may sit at the top level of pokedex.yaml or under
    # 'environment:' - accept both (environment wins if both are present)
    user_resources = {}
    user_resources.update(pokedex.get('resources', {}) or {})
    user_resources.update(environment.get('resources', {}) or {})
    for key, val in user_resources.items():
        if isinstance(val, dict):
            if key in resources:
                resources[key].update(val)
            else:
                resources[key] = val
    
    # Source overrides
    overrides = sources.get('overrides', {})
    calibrator_overrides = overrides.get('calibrators', {})
    uvrange_overrides = overrides.get('uvranges', {})
    
    # Load user models if provided
    user_models = {}
    if models_path and os.path.exists(models_path):
        with open(models_path, 'r') as f:
            user_models = yaml.safe_load(f) or {}
    
    return PipelineConfig(
        # Paths
        ms_path=ms_path,
        ms_name=ms_name,
        working_dir=working_dir,

        # Raw sections
        environment=environment,
        data=data,
        sources=sources,
        flow=flow,

        # Parsed fields
        target_spws=data.get('processing_spw', 1),
        resources=resources,

        # Container (udocker)
        container=environment.get('container', {}),

        # Source config
        auto_detect=sources.get('auto_detect', True),
        calibrator_overrides=calibrator_overrides,
        uvrange_overrides=uvrange_overrides,

        # User models
        user_models=user_models,
    )
