import os
import sys
import yaml
import logging
import datetime
from datetime import datetime
from collections import defaultdict
from .pokeeggs.formalities import setup_battle
from .pokeeggs.calibrator import *
from .pokeeggs.selfcalibrator import *
from .pokeeggs.ddcal import *



class JobTracker:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.ms_name = config['msinfo']['parent_ms']
        self.checkpoint_file = f"{self.ms_name}_checkpoints.yaml"
        self.job_status = {f"spw{i}": {} for i in range(config['msinfo']['number_of_processing_spw'])}
        # Add active SPWs tracking
        self.active_spws = set(f"spw{i}" for i in range(config['msinfo']['number_of_processing_spw']))
        self.failed_steps = defaultdict(set)  # Track which SPWs failed at which step
        self.load_checkpoints()

    def load_checkpoints(self):
        """Load existing checkpoints"""
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'r') as f:
                self.checkpoints = yaml.safe_load(f) or {}
        else:
            self.checkpoints = {}

    def check_step_outputs(self, step, spw):
        """Verify output files exist for step"""
        if step == 'splitting':
            return (os.path.exists(f"{spw}/cal.ms") and 
                   os.path.exists(f"{spw}/src.ms"))
        elif step == 'bad_antenna':
            return os.path.exists(f"{spw}/badants.txt")
        return False

    def check_log_file(self, spw, step):
        """Check log file for errors"""
        log_file = f"{spw}/{step}.log"
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                return "error" not in f.read().lower()
        return False

    def verify_step_completion(self, step, spw):
        """Check both checkpoint and outputs"""
        if step in self.checkpoints:
            spw_data = self.checkpoints[step].get('spws', {}).get(spw)
            if spw_data and self.check_step_outputs(step, spw):
                self.logger.info(f"Step {step} already completed for {spw}")
                return True
        return False

    def add_jobs(self, step, job_info):
        """Track new jobs"""
        for job_id, spw in job_info:
            if spw in self.active_spws:  # Only track jobs for active SPWs
                self.job_status[spw][step] = {
                    'job_id': job_id,
                    'status': 'running',
                    'timestamp': datetime.now().isoformat()
                }

    def handle_job_completion(self, step, spw, success):
        """Update status when job completes"""
        if success:
            if step not in self.checkpoints:
                self.checkpoints[step] = {'spws': {}}
            self.checkpoints[step]['spws'][spw] = {
                'timestamp': datetime.now().isoformat(),
                'status': 'completed'
            }
            with open(self.checkpoint_file, 'w') as f:
                yaml.dump(self.checkpoints, f)
        else:
            self.active_spws.remove(spw)
            self.failed_steps[step].add(spw)
            self.logger.warning(f"SPW {spw} failed at {step} and will be skipped in future steps")

    def check_brotherhood(self, failed_spws):
        """Check brotherhood and update active SPWs"""
        if failed_spws:
            # Update active SPWs regardless of brotherhood
            self.active_spws -= set(failed_spws)
            if self.config['pipeline'].get('brotherhood', False):
                self.logger.error(f"Brotherhood enabled - Failed SPWs: {failed_spws}")
                return False
            else:
                self.logger.warning(f"Continuing without failed SPWs: {failed_spws}")
                self.logger.info(f"Remaining active SPWs: {self.active_spws}")
        return True

    def get_active_spws(self):
        """Get list of currently active SPWs"""
        return list(self.active_spws)


def configure_logger(name, log_file, level=logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Create file handler which logs even debug messages
    fh = logging.FileHandler(log_file)
    fh.setLevel(level)
    
    # Create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    
    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    # Add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    return logger


def main():
    # Initialize logging
    start_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    log = 'charizard_{}.log'.format(start_time)
    logger = configure_logger('charizard', log)

    # Check command line arguments
    if len(sys.argv) < 2:
        logger.error("Please provide a YAML config file as an argument.")
        return
    
    config_file = sys.argv[1]
    if len(sys.argv) > 2:
        config_file = sys.argv[2]

    # Load configuration
    try:
        config = yaml.safe_load(open(config_file))
        logger.info(f"Got the file, let me go through it and get you some radio images.")
        tracker = JobTracker(config, logger)
    except Exception as e:
        logger.error(f"Cannot read the pokedex (the config file), oops! \n: {e}")
        return

    logger.info("Now checking everything is in place or not")

    # Setup initial data
    if not setup_battle(config, logger, tracker):
        logger.error("Setup failed! Cannot proceed further.")
        return
    logger.info("Setup completed successfully")

    try:
        # Initial Calibration
        if config['pipeline'].get('calibration', {}).get('doit', False):
            try:
                calibration_success = do_calibration(config, logger, tracker)
                if not calibration_success:
                    logger.error("First-generation calibration failed! Cannot proceed further.")
                    return
                logger.info("First-generation calibration completed successfully")
            except Exception as e:
                logger.error(f"Error during calibration: {str(e)}")
                return
        else:
            logger.info("Skipping calibration as per configuration")

        # Self-calibration
        if config['pipeline']['imaging_with_debugging'].get('selfcal', {}).get('doit', False):
            try:
                self_calibration_success = self_calibration(config, logger, tracker)
                if not self_calibration_success:
                    logger.error("Self-calibration failed! Cannot proceed further.")
                    return
                logger.info("Self-calibration completed successfully")
            except Exception as e:
                logger.exception(f"Error during self-calibration")  # This will print the full traceback
                return
        else:
            logger.info("Skipping self-calibration as per configuration")

        # Direction-dependent calibration
        if config['pipeline'].get('ddcal', {}).get('doit', False):
            try:
                ddcal_success = direction_dependent_calibration(config, logger, tracker)
                if not ddcal_success:
                    logger.error("Direction-dependent calibration failed!")
                    return
                logger.info("Direction-dependent calibration completed successfully")
            except Exception as e:
                logger.exception(f"Error during direction-dependent calibration")
                return
        else:
            logger.info("Skipping direction-dependent calibration as per configuration")

        logger.info("All processing completed successfully!")
        end_time = datetime.now()
        duration = end_time - datetime.strptime(start_time, "%Y_%m_%d_%H_%M_%S")
        logger.info(f"Total processing time: {duration}")

    except Exception as e:
        logger.exception(f"Unexpected error during processing")
        return

if __name__ == "__main__":
    main()