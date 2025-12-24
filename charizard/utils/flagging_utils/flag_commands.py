# charizard/utils/flagging_utils/flag_commands.py
"""
Flag command generation utilities.
"""

from typing import List, Optional


def calculate_edge_channels(nchan: int, percent: float = 5) -> List[str]:
    """Calculate edge channels to flag."""
    edge_width = max(1, int(nchan * (percent / 100)))
    edge_spw = []
    
    if edge_width > 0:
        edge_spw.append(f"0~{edge_width-1}")
        edge_spw.append(f"{nchan-edge_width}~{nchan-1}")
    
    return edge_spw


def write_flag_commands(output_file: str, 
                        mode: str = 'w',
                        flags_to_include: Optional[List[str]] = None,
                        **kwargs):
    """
    Generate flag commands for CASA flagdata(mode='list').
    
    Args:
        output_file: Output file path
        mode: File mode ('w' or 'a')
        flags_to_include: List of flag types ['shadow','autocorr','edge','clip','quack','badant']
        **kwargs: Parameters for specific flags:
            - nchan: Number of channels (for edge)
            - nspws: Number of SPWs (for edge)
            - edge_percent: Edge percentage (default 5)
            - quack_interval: Quack interval in seconds (default 10)
            - scan_spw_ants: Dict for badant flags
    """
    if flags_to_include is None:
        flags_to_include = ['shadow', 'autocorr', 'edge', 'clip', 'quack']
    
    commands = []
    
    if 'shadow' in flags_to_include:
        commands.extend([
            "# Shadow flagging",
            "mode='shadow' reason='shadow'",
        ])
    
    if 'autocorr' in flags_to_include:
        commands.extend([
            "# Autocorrelations",
            "mode='manual' autocorr=True reason='autocorr'",
        ])
    
    if 'edge' in flags_to_include and 'nchan' in kwargs and 'nspws' in kwargs:
        nchan = kwargs['nchan']
        nspws = kwargs['nspws']
        edge_percent = kwargs.get('edge_percent', 5)
        edge_width = max(1, int(nchan * (edge_percent / 100)))
        
        edge_spw = []
        for spw in range(nspws):
            edge_spw.append(f"{spw}:0~{edge_width-1}")
            edge_spw.append(f"{spw}:{nchan-edge_width}~{nchan-1}")
        
        commands.extend([
            "# Edge channels",
            f"mode='manual' spw='{','.join(edge_spw)}' reason='edgespw' name='edgespw'",
        ])
    
    if 'clip' in flags_to_include:
        commands.extend([
            "# Clip zeros",
            "mode='clip' correlation='ABS_ALL' clipzeros=True reason='clip_zeros'",
        ])
    
    if 'quack' in flags_to_include:
        quack_interval = kwargs.get('quack_interval', 10)
        commands.extend([
            "# Quack flagging",
            f"mode='quack' quackinterval={quack_interval} quackmode='beg' quackincrement=False reason='quackbeg'",
            f"mode='quack' quackinterval={quack_interval} quackmode='endb' quackincrement=False reason='quackend'",
        ])
    
    if 'badant' in flags_to_include and 'scan_spw_ants' in kwargs:
        commands.append("# Bad antenna commands")
        scan_spw_ants = kwargs['scan_spw_ants']
        for scan in sorted(scan_spw_ants.keys()):
            spw_list, ant_list = [], []
            for spw, antennas in sorted(scan_spw_ants[scan].items()):
                if antennas:
                    spw_list.append(str(spw))
                    ant_list.extend(list(antennas))
            
            if ant_list:
                ant_list = list(dict.fromkeys(ant_list))
                commands.append(
                    f"mode='manual' scan='{scan}' spw='{','.join(spw_list)}' "
                    f"antenna='{','.join(ant_list)}' reason='bad_antenna'"
                )
    
    with open(output_file, mode) as f:
        f.write('\n'.join(commands))
        if commands:
            f.write('\n')
