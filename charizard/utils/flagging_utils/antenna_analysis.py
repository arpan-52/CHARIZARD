# charizard/utils/flagging_utils/antenna_analysis.py
"""
Antenna analysis utilities.
- Find dead/bad antennas
- Find best reference antenna
"""

import os
import time
import json
import warnings
import numpy as np
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
from multiprocessing import Pool

from casacore import tables
from ..general.jobs import refresh_dir, wait_and_check
from ..general.resources import submit_resources

# Peak bytes of DATA+FLAG held in memory per read. Sized so that ppn parallel
# workers stay well inside a normal node allocation.
CHUNK_BYTES = 256 * 1024 ** 2

# Grace period for a finished job's output file to become visible here.
#
# The file is written by a compute node and read back on the submit host - a
# different NFS client, which with default mount options can lag by up to
# `acdirmax` (60 s) before it sees the new entry. 150 s clears that with margin.
# Only paid in full when a job genuinely produced nothing.
ARTIFACT_WAIT_SECONDS = 150.0


def remove_table_lock(ms_path: str):
    """Remove table lock file if exists."""
    lock_file = os.path.join(ms_path, 'table.lock')
    if os.path.exists(lock_file):
        try:
            os.remove(lock_file)
        except:
            pass


def get_ms_info_for_antenna(ms_path: str):
    """Get basic MS info for antenna analysis."""
    try:
        with tables.table(ms_path, ack=False) as tb:
            scans = np.unique(tb.getcol('SCAN_NUMBER'))
            fields = np.unique(tb.getcol('FIELD_ID'))
            spws = np.unique(tb.getcol('DATA_DESC_ID'))
            ant1 = tb.getcol('ANTENNA1', 0, min(10000, tb.nrows()))
            ant2 = tb.getcol('ANTENNA2', 0, min(10000, tb.nrows()))
            active_ants = np.unique(np.concatenate([ant1, ant2]))
        
        with tables.table(ms_path + '/ANTENNA', ack=False) as tb:
            ant_names = list(tb.getcol('NAME'))
            mock_mask = np.ones(len(ant_names), dtype=bool)
            mock_mask[active_ants] = False
            mock_ants = np.where(mock_mask)[0]
        
        with tables.table(ms_path + '/SPECTRAL_WINDOW', ack=False) as tb:
            nchan = tb.getcol('NUM_CHAN')
        
        return scans, fields, spws, ant_names, nchan, mock_ants
    finally:
        remove_table_lock(ms_path)


def _wait_for_artifacts(paths, timeout: float = None,
                        interval: float = 2.0) -> set:
    """Wait for job output files to appear and be non-empty.

    Guards two races at once: the scheduler staging a finished job's files back,
    and shared-filesystem metadata lag between the compute node that wrote the
    file and the login node checking for it.

    All paths share one deadline, so a genuine failure costs `timeout` once
    rather than once per SPW, and it returns as soon as everything is ready.

    Returns:
        The subset of `paths` that became usable.
    """
    if timeout is None:
        timeout = ARTIFACT_WAIT_SECONDS

    deadline = time.time() + timeout
    pending = set(paths)
    ready = set()

    while True:
        for path in list(pending):
            try:
                if os.path.getsize(path) > 0:
                    ready.add(path)
                    pending.discard(path)
            except OSError:
                pass  # not visible yet

        if not pending or time.time() >= deadline:
            return ready

        # Re-read the containing directories so a cached NFS client revalidates
        for path in pending:
            refresh_dir(path)

        time.sleep(interval)


def _parallel_hand_indices(npols: int):
    """Correlation indices of the parallel hands (RR/LL or XX/YY)."""
    if npols == 4:
        return [0, 3]
    if npols == 2:
        return [0, 1]
    return [0]


def _read_row_amplitudes(sel, npols: int, chunk_bytes: int = CHUNK_BYTES,
                         max_rows: int = None):
    """Collapse each row's parallel-hand amplitude to one number, in chunks.

    The per-channel amplitudes are only ever used to take a median, so
    collapsing the channel axis up front turns an nrow x nchan x npol array
    into one float per row. On a 64-antenna MeerKAT block that is the
    difference between a few MB and tens of GB.

    The collapse is a median, not a mean, to stay faithful to the original
    per-channel statistic: a mean would let an antenna that is dead across
    only part of the band average back above the detection threshold, and
    would be pulled around by narrowband RFI.

    Returns:
        (row_amp, n_valid, n_total) - one entry per row. row_amp is NaN for
        rows with no unflagged channel.
    """
    nrows = sel.nrows() if max_rows is None else min(sel.nrows(), max_rows)
    pols = _parallel_hand_indices(npols)

    # Size the chunk by bytes, not rows - nchan varies by orders of magnitude
    # between a 1k VLA spw and a 32k MeerKAT one.
    shape = sel.getcell('DATA', 0).shape          # (nchan, npol)
    nchan = int(shape[0])
    bytes_per_row = nchan * npols * (8 + 1)       # complex64 DATA + bool FLAG
    chunk_rows = max(1, int(chunk_bytes // max(bytes_per_row, 1)))

    row_amp = np.empty(nrows, dtype=np.float64)
    n_valid = np.empty(nrows, dtype=np.int64)

    for start in range(0, nrows, chunk_rows):
        n = min(chunk_rows, nrows - start)
        data = sel.getcol('DATA', start, n)        # (n, nchan, npol)
        flag = sel.getcol('FLAG', start, n)

        amp = np.abs(data[:, :, pols]).mean(axis=2)      # (n, nchan)
        valid = ~flag[:, :, pols].any(axis=2)            # (n, nchan)

        # Median over unflagged channels. Stay in float32 so the masked copy
        # does not silently double this chunk's footprint.
        masked = np.where(valid, amp, np.float32('nan'))
        with warnings.catch_warnings():
            # all-flagged rows are expected and become NaN, which is the signal
            warnings.simplefilter('ignore', RuntimeWarning)
            row_amp[start:start + n] = np.nanmedian(masked, axis=1)

        n_valid[start:start + n] = valid.sum(axis=1)

        del data, flag, amp, valid, masked

    n_total = np.full(nrows, nchan, dtype=np.int64)
    return row_amp, n_valid, n_total


def _grouped_medians(keys: np.ndarray, values: np.ndarray, n_keys: int):
    """Median of `values` per key, ignoring NaNs. NaN where a key has no data.

    Sorts once and splits, rather than masking the full array per key.
    """
    medians = np.full(n_keys, np.nan)

    finite = np.isfinite(values)
    if not finite.any():
        return medians

    k = keys[finite]
    v = values[finite]

    order = np.argsort(k, kind='stable')
    k = k[order]
    v = v[order]

    boundaries = np.flatnonzero(np.diff(k)) + 1
    for group_keys, group_vals in zip(np.split(k, boundaries), np.split(v, boundaries)):
        if group_vals.size:
            medians[group_keys[0]] = np.median(group_vals)

    return medians


def process_spw_field(params):
    """Process one SPW/field combination for bad antenna detection."""
    ms_path, spw, field, ant_names, mock_ants = params

    try:
        with tables.table(ms_path, ack=False) as tb:
            sel = tb.query(f"DATA_DESC_ID = {spw} AND FIELD_ID = {field}",
                           sortlist='SCAN_NUMBER')

            if sel.nrows() == 0:
                sel.close()
                return None

            try:
                scans = sel.getcol('SCAN_NUMBER')
                ant1 = sel.getcol('ANTENNA1')
                ant2 = sel.getcol('ANTENNA2')
                npols = int(sel.getcell('DATA', 0).shape[-1])

                row_amp, n_valid, n_total = _read_row_amplitudes(sel, npols)
            finally:
                sel.close()

            num_ants = len(ant_names)

            # Every row contributes to both of its antennas
            ants = np.concatenate([ant1, ant2])
            row_scans = np.tile(scans, 2)
            amps = np.tile(row_amp, 2)
            valid = np.tile(n_valid, 2)
            total = np.tile(n_total, 2)

            unique_scans, scan_idx = np.unique(row_scans, return_inverse=True)

            # ---- global reference amplitude (per-antenna median over all scans)
            ant_medians = _grouped_medians(ants, amps, num_ants)
            good_amps = ant_medians[np.isfinite(ant_medians) & (ant_medians > 0)]

            if good_amps.size == 0:
                return None

            reference_amp = float(np.percentile(good_amps, 75))
            threshold = reference_amp * 0.05

            # ---- per (scan, antenna) statistics
            keys = scan_idx * num_ants + ants
            n_keys = len(unique_scans) * num_ants

            scan_ant_medians = _grouped_medians(keys, amps, n_keys)
            flagged = np.bincount(keys, weights=(total - valid), minlength=n_keys)
            sampled = np.bincount(keys, weights=total, minlength=n_keys)

            with np.errstate(invalid='ignore', divide='ignore'):
                flag_percent = np.where(sampled > 0, 100.0 * flagged / sampled, 100.0)

            mock_set = set(int(a) for a in mock_ants)
            scan_bad_antennas = {}

            for s, scan in enumerate(unique_scans):
                scan_bad_ants = []
                for ant in range(num_ants):
                    if ant in mock_set:
                        continue

                    key = s * num_ants + ant
                    med_amp = scan_ant_medians[key]

                    # No unflagged data for this antenna/scan at all
                    if not np.isfinite(med_amp):
                        continue

                    # Effectively already flagged - leave it to the flagger
                    if flag_percent[key] > 90:
                        continue

                    if med_amp < threshold:
                        scan_bad_ants.append({
                            'name': ant_names[ant],
                            'median_amplitude': float(med_amp),
                            'ratio_to_reference': float(med_amp / reference_amp),
                            'flag_percentage': float(flag_percent[key])
                        })

                if scan_bad_ants:
                    scan_bad_antennas[int(scan)] = scan_bad_ants

            return {
                'field': int(field),
                'spw': int(spw),
                'mock_antennas': [ant_names[i] for i in mock_ants],
                'scan_bad_antennas': scan_bad_antennas,
                'reference_amplitude': reference_amp
            }

    except Exception as e:
        print(f"Error processing Field {field}, SPW {spw}: {str(e)}")
        return None
    finally:
        remove_table_lock(ms_path)


def write_user_and_standard_flags(f, user_bad_ants: List[str] = None):
    """Append user-specified bad antennas and the standard flag commands."""
    if user_bad_ants:
        for ant in user_bad_ants:
            f.write(f"mode='manual' antenna='{ant}' reason='user_specified'\n")

    f.write("# Standard flags\n")
    f.write("mode='manual' autocorr=True reason='autocorr'\n")
    f.write("mode='clip' correlation='ABS_ALL' clipzeros=True reason='clip_zeros'\n")
    f.write("mode='quack' quackinterval=10 quackmode='beg' quackincrement=False reason='quackbeg'\n")
    f.write("mode='quack' quackinterval=10 quackmode='endb' quackincrement=False reason='quackend'\n")


def find_dead_antennas(ms_path: str, output_file: str, n_processes: int = None,
                       user_bad_ants: List[str] = None):
    """
    Find dead/bad antennas in MS and write to badants.txt.
    
    Args:
        ms_path: Path to measurement set
        output_file: Path to output file (badants.txt)
        n_processes: Number of parallel processes
        user_bad_ants: List of user-specified bad antennas
    """
    if n_processes is None:
        n_processes = max(1, 8)
    
    if user_bad_ants is None:
        user_bad_ants = []
    
    print(f"Analyzing {ms_path} for dead antennas...")
    scans, fields, spws, ant_names, nchan, mock_ants = get_ms_info_for_antenna(ms_path)
    
    tasks = [(ms_path, spw, field, ant_names, mock_ants) 
             for field in fields 
             for spw in spws]
    
    print(f"Processing {len(tasks)} field/SPW combinations using {n_processes} processes...")
    with Pool(n_processes) as pool:
        all_results = pool.map(process_spw_field, tasks)
    
    # Aggregate results
    antenna_scan_map = defaultdict(lambda: defaultdict(set))
    
    for result in all_results:
        if result and result['scan_bad_antennas']:
            field = result['field']
            for scan, bad_ants in result['scan_bad_antennas'].items():
                for ant_info in bad_ants:
                    ant_name = ant_info['name']
                    antenna_scan_map[ant_name][field].add(scan)
    
    # Write results
    with open(output_file, 'w') as f:
        # Dead antennas
        for ant_name, field_scans in antenna_scan_map.items():
            for field, scan_set in field_scans.items():
                if scan_set:
                    scan_list = sorted(list(scan_set))
                    scan_str = ','.join(map(str, scan_list))
                    f.write(f"mode='manual' antenna='{ant_name}' field='{field}' scan='{scan_str}' reason='dead_antenna'\n")

        write_user_and_standard_flags(f, user_bad_ants)
    
    print(f"Bad antenna results written to: {output_file}")
    
    # Clean up lock
    remove_table_lock(ms_path)
    
    return len(antenna_scan_map)


def find_best_refant(ms_path: str, n_processes: int = None) -> Tuple[str, Dict]:
    """
    Find the best reference antenna based on SNR metric.
    
    Best refant = highest (median_amp / std_amp) * (1 - flag_fraction)
    
    Should be called AFTER flagging for best results.
    
    Args:
        ms_path: Path to measurement set
        n_processes: Number of parallel processes
    
    Returns:
        (best_refant_name, antenna_stats_dict)
    """
    if n_processes is None:
        n_processes = max(1, 8)
    
    print(f"Finding best reference antenna in {ms_path}...")
    
    try:
        with tables.table(ms_path + '/ANTENNA', ack=False) as tb:
            ant_names = list(tb.getcol('NAME'))
        
        with tables.table(ms_path, ack=False) as tb:
            # Sample subset for speed
            nrows = min(tb.nrows(), 100000)
            if nrows == 0:
                print("WARNING: MS has no rows, cannot determine refant")
                return ant_names[0] if ant_names else None, {}
            ant1 = tb.getcol('ANTENNA1', 0, nrows)
            ant2 = tb.getcol('ANTENNA2', 0, nrows)
            npols = int(tb.getcell('DATA', 0).shape[-1])

            # Channel-averaged per row, read in chunks - see _read_row_amplitudes
            row_amp, n_valid, n_total = _read_row_amplitudes(tb, npols,
                                                             max_rows=nrows)

        num_ants = len(ant_names)

        # Every row contributes to both of its antennas
        ants = np.concatenate([ant1, ant2])
        amps = np.tile(row_amp, 2)
        valid = np.tile(n_valid, 2)
        total = np.tile(n_total, 2)

        finite = np.isfinite(amps)
        # Unflagged (row, channel) samples per antenna - the quantity the
        # minimum-data guard below has always been expressed in.
        counts = np.bincount(ants, weights=valid, minlength=num_ants)
        flagged = np.bincount(ants, weights=(total - valid), minlength=num_ants)
        sampled = np.bincount(ants, weights=total, minlength=num_ants)

        medians = _grouped_medians(ants, amps, num_ants)

        # Scatter about the median, per antenna
        stds = np.full(num_ants, np.nan)
        order = np.argsort(ants[finite], kind='stable')
        sorted_ants = ants[finite][order]
        sorted_amps = amps[finite][order]
        boundaries = np.flatnonzero(np.diff(sorted_ants)) + 1
        for gk, gv in zip(np.split(sorted_ants, boundaries),
                          np.split(sorted_amps, boundaries)):
            if gv.size:
                stds[gk[0]] = np.std(gv)

        # Calculate SNR metric for each antenna
        refant_scores = {}
        for ant_idx in range(num_ants):
            # Same guard as before: too little surviving data to judge
            if counts[ant_idx] < 100:
                continue

            median_amp = medians[ant_idx]
            std_amp = stds[ant_idx]

            if not np.isfinite(median_amp) or not np.isfinite(std_amp):
                continue
            if std_amp == 0 or median_amp == 0:
                continue

            flag_fraction = (flagged[ant_idx] / sampled[ant_idx]
                             if sampled[ant_idx] > 0 else 1.0)

            # SNR metric: (median/std) * (1 - flag_fraction)
            snr_metric = (median_amp / std_amp) * (1 - flag_fraction)

            refant_scores[ant_names[ant_idx]] = {
                'snr_metric': float(snr_metric),
                'median_amp': float(median_amp),
                'std_amp': float(std_amp),
                'flag_fraction': float(flag_fraction)
            }

        if not refant_scores:
            print("WARNING: Could not determine best refant, using first antenna")
            return ant_names[0], {}
        
        best_refant = max(refant_scores, key=lambda x: refant_scores[x]['snr_metric'])
        print(f"Best reference antenna: {best_refant} (SNR metric: {refant_scores[best_refant]['snr_metric']:.3f})")
        
        # Save to file
        refant_file = os.path.join(os.path.dirname(ms_path), 'refant.json')
        with open(refant_file, 'w') as f:
            json.dump({'best_refant': best_refant, 'scores': refant_scores}, f, indent=2)
        
        return best_refant, refant_scores
        
    finally:
        remove_table_lock(ms_path)


# =============================================================================
# PIPELINE STEP FUNCTIONS - called from charizard.py
# =============================================================================

def run_bad_antenna_detection(hk, config, active_spws: List[str], logger, 
                               whitelist: List[str]) -> Optional[List[str]]:
    """
    Run bad antenna detection step.
    
    Generates SHORT scripts that import from charizard.
    Does NOT find refant here.
    """
    
    env = config.environment
    flow = config.flow
    init_cal = flow.get('initial_calibration_flagging', {})
    flagging = init_cal.get('flagging', {})
    bad_ant_config = flagging.get('bad_antennas', {})
    
    user_bad_ants = bad_ant_config.get('list', [])
    if isinstance(user_bad_ants, str):
        user_bad_ants = [a.strip() for a in user_bad_ants.split(',') if a.strip()]

    # auto=false: skip detection, just write the user list + standard flags
    if not bad_ant_config.get('auto', True):
        logger.info("bad_antennas.auto=false - skipping detection, "
                    "writing user list + standard flags only")
        for spw in active_spws:
            with open(f"{spw}/badants.txt", 'w') as f:
                write_user_and_standard_flags(f, user_bad_ants)
        return active_spws

    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('default', {})
    ppn = resources.get('ppn', 4)
    
    job_ids = []
    job_map = {}
    
    for spw in active_spws:
        # SHORT script - just imports and calls
        script = f'''#!/usr/bin/env python3
# Bad antenna detection for {spw}
from charizard.utils.flagging_utils.antenna_analysis import find_dead_antennas

find_dead_antennas(
    ms_path='{spw}/cal.ms',
    output_file='{spw}/badants.txt',
    n_processes={ppn},
    user_bad_ants={repr(user_bad_ants)}
)
print("Done!")
'''
        
        script_file = f"badant_{spw}.py"
        with open(script_file, 'w') as f:
            f.write(script)
        
        command = f"""cd {os.getcwd()}
{preamble}
python3 {script_file}
"""

        job = hk.submit(
            command=command,
            name=f"badant_{spw}",
            job_subdir=spw,
            **submit_resources(resources, '02:00:00', ppn=ppn)
        )

        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = spw
            logger.info(f"Submitted bad antenna detection for {spw}: {job.job_id}")

        time.sleep(0.5)
    
    if not job_ids:
        logger.error("No bad antenna jobs submitted")
        return None
    
    # Wait for jobs
    logger.substep(f"Waiting for {len(job_ids)} bad antenna jobs...")
    results = wait_and_check(hk, job_ids, whitelist=whitelist, logger=logger)
    
    # Process results
    successful = []
    failed = []
    
    # The artifact is the verdict, not the log - same rule run_split uses.
    # A short job can leave the queue before the scheduler has staged its .out
    # back, and housekeeper reports that as "No log files found" -> failure,
    # even though the job succeeded. Log problems are worth reporting, but only
    # a missing or empty badants.txt actually means the step failed.
    for job_id, (job, log_result) in results.items():
        spw = job_map.get(job_id, 'unknown')

        if not log_result.success:
            logger.warning(f"{spw}: bad antenna job had log errors")
            if log_result.error_lines:
                for err in log_result.error_lines[:5]:
                    logger.warning(f"  >> {err}")

    spws = list(job_map.values())
    ready = _wait_for_artifacts([f"{s}/badants.txt" for s in spws])

    for spw in spws:
        if f"{spw}/badants.txt" in ready:
            successful.append(spw)
            logger.info(f"{spw}: OK")
        else:
            failed.append(spw)
            logger.error(f"{spw}: FAILED (no usable {spw}/badants.txt)")
    
    if not successful:
        return None
    
    return successful


def run_find_refant(hk, config, active_spws: List[str], logger,
                    whitelist: List[str]) -> Tuple[Optional[List[str]], Optional[str]]:
    """
    Find best reference antenna.
    
    Should be called AFTER initial flagging and catboss.
    """
    
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('default', {})
    ppn = resources.get('ppn', 4)
    
    job_ids = []
    job_map = {}
    
    for spw in active_spws:
        # SHORT script
        script = f'''#!/usr/bin/env python3
# Find best refant for {spw}
from charizard.utils.flagging_utils.antenna_analysis import find_best_refant

refant, scores = find_best_refant(
    ms_path='{spw}/cal.ms',
    n_processes={ppn}
)
print(f"Best refant: {{refant}}")
'''
        
        script_file = f"refant_{spw}.py"
        with open(script_file, 'w') as f:
            f.write(script)
        
        command = f"""cd {os.getcwd()}
{preamble}
python3 {script_file}
"""

        job = hk.submit(
            command=command,
            name=f"refant_{spw}",
            job_subdir=spw,
            **submit_resources(resources, walltime='01:00:00', ppn=ppn)
        )

        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = spw
            logger.info(f"Submitted refant finding for {spw}: {job.job_id}")

        time.sleep(0.5)
    
    if not job_ids:
        logger.error("No refant jobs submitted")
        return None, None
    
    # Wait for jobs
    logger.substep(f"Waiting for {len(job_ids)} refant jobs...")
    results = wait_and_check(hk, job_ids, whitelist=whitelist, logger=logger)
    
    # Process results
    successful = []
    refants = []
    
    # Artifact decides, not the log - see the note in run_bad_antenna_detection.
    for job_id, (job, log_result) in results.items():
        spw = job_map.get(job_id, 'unknown')
        if not log_result.success:
            logger.warning(f"{spw}: refant job had log errors")
            if log_result.error_lines:
                for err in log_result.error_lines[:3]:
                    logger.warning(f"  >> {err}")

    spws = list(job_map.values())
    ready = _wait_for_artifacts([f"{s}/refant.json" for s in spws])

    for spw in spws:
        refant_file = f"{spw}/refant.json"

        if refant_file not in ready:
            logger.warning(f"{spw}: missing {refant_file}")
            continue

        try:
            with open(refant_file, 'r') as f:
                data = json.load(f)
            refants.append(data['best_refant'])
            successful.append(spw)
            logger.info(f"{spw}: OK (refant: {data['best_refant']})")
        except (ValueError, KeyError, OSError) as e:
            logger.warning(f"{spw}: could not read {refant_file}: {e}")
    
    if not refants:
        return successful if successful else None, None
    
    # Pick most common refant
    from collections import Counter
    best_refant = Counter(refants).most_common(1)[0][0]
    logger.info(f"Selected refant: {best_refant}")
    
    return successful if successful else active_spws, best_refant
