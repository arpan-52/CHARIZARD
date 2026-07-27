# charizard/utils/general/resources.py
"""
Job resource plumbing.

One place that turns a pokedex `resources:` block into housekeeper.submit()
keyword arguments, so a key added to the config actually reaches the scheduler.
"""

from typing import Any, Dict, Optional


def submit_resources(resources: Optional[Dict[str, Any]],
                     default_walltime: str = '04:00:00',
                     walltime: Optional[str] = None,
                     ppn: Optional[int] = None) -> Dict[str, Any]:
    """Map a `resources:` entry onto housekeeper.submit() kwargs.

    Args:
        resources:        One block from config.resources (e.g. resources['imaging']).
        default_walltime: Fallback when the block has no walltime of its own.
        walltime:         Hard override; wins over the block's walltime.
        ppn:              Hard override; wins over the block's ppn.

    Returns:
        dict with nodes/ppn/walltime, plus mem_gb when the config sets one.
    """
    resources = resources or {}

    kwargs: Dict[str, Any] = {
        'nodes': int(resources.get('nodes') or 1),
        'ppn': int(ppn if ppn is not None else (resources.get('ppn') or 4)),
        'walltime': walltime or resources.get('walltime') or default_walltime,
    }

    # Only pass mem_gb when it is actually configured - housekeeper treats
    # None as "let the scheduler decide", which is the right default.
    mem_gb = resources.get('mem_gb')
    if mem_gb:
        kwargs['mem_gb'] = int(mem_gb)

    return kwargs
