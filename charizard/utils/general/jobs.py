# charizard/utils/general/jobs.py
"""
Job completion helpers.

Wraps housekeeper's wait_and_check so a scheduler that stages job output
lazily cannot be mistaken for a job that failed.
"""

import os
import time
from typing import Any, Dict, List, Optional, Tuple

# How long to keep re-checking for a job log that has not appeared yet.
#
# Sized against NFS client attribute caching, not scheduler latency. Job output
# is written by a compute node but read back on the submit host, which is a
# different NFS client; with default mount options that host can take up to
# `acdirmax` (60 s on Linux) to see the new directory entry. 150 s leaves a
# clear margin over that.
LOG_GRACE_SECONDS = 150.0
LOG_POLL_INTERVAL = 5.0

_MISSING_LOG_MARKER = "No log files found"


def refresh_dir(path: str) -> None:
    """Nudge an NFS client into revalidating a directory it has cached.

    Reading the directory forces a fresh READDIR once the cached attributes
    expire, so a file created by another node shows up as soon as possible
    rather than only after the next unrelated lookup.
    """
    for target in (path, os.path.dirname(path.rstrip('/'))):
        if not target:
            continue
        try:
            os.listdir(target)
        except OSError:
            pass


def _log_is_missing(log_result) -> bool:
    """True when the check failed only because no log file existed yet."""
    if log_result is None or log_result.success:
        return False
    lines = getattr(log_result, 'error_lines', None) or []
    return any(_MISSING_LOG_MARKER in str(line) for line in lines)


def wait_and_check(hk,
                   job_ids: List[str],
                   whitelist: Optional[List[str]] = None,
                   timeout: Optional[int] = None,
                   logger=None,
                   log_grace: float = None) -> Dict[str, Tuple[Any, Any]]:
    """housekeeper.wait_and_check, tolerant of NFS attribute caching.

    Job output is written by a compute node but read back on the submit host.
    On a shared filesystem those are different NFS clients, and with default
    mount options the submit host can take up to `acdirmax` (60 s) to see a
    directory entry another node created. housekeeper checks the instant the
    job leaves the queue, so a job that plainly succeeded gets reported as
    "No log files found" - a false failure that hits every step lacking an
    artifact check of its own.

    Jobs whose logs are merely not visible yet are re-checked, with the
    directory re-read each round to prompt revalidation, until they appear or
    `log_grace` expires. Genuine failures are untouched: a log that exists and
    contains errors is returned as-is on the first pass, with no added delay.
    """
    if log_grace is None:
        log_grace = LOG_GRACE_SECONDS

    results = hk.wait_and_check(job_ids, whitelist=whitelist, timeout=timeout)

    pending = [jid for jid, (_job, res) in results.items() if _log_is_missing(res)]
    if not pending:
        return results

    if logger:
        logger.info(f"Waiting for {len(pending)} job log(s) to become visible on "
                    f"this host (shared-filesystem lag, up to {log_grace:.0f}s)...")

    jobs_dir = str(getattr(hk, 'jobs_dir', '') or '')

    deadline = time.time() + log_grace
    while pending and time.time() < deadline:
        time.sleep(LOG_POLL_INTERVAL)

        # Re-read the job directories so a cached client picks up new entries
        if jobs_dir:
            refresh_dir(jobs_dir)
            try:
                for sub in os.listdir(jobs_dir):
                    full = os.path.join(jobs_dir, sub)
                    if os.path.isdir(full):
                        refresh_dir(full)
            except OSError:
                pass

        for jid in list(pending):
            try:
                res = hk.check_log(jid, whitelist)
            except Exception:
                continue
            if not _log_is_missing(res):
                job, _old = results[jid]
                results[jid] = (job, res)
                pending.remove(jid)

    if pending and logger:
        logger.warning(f"{len(pending)} job log(s) never appeared; falling back "
                       f"to output checks where available")

    return results
