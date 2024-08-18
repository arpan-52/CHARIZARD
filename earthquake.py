#This will have all the necessary functions that are needed to used inside the pbs scripts written in dragon_breath... ::~)


import logging
import numpy as np


def determine_cutoff(antenna_means, method='median', factor=1.5):
    """
    Determine a suitable cutoff value based on the mean amplitudes of antennas.
    
    Parameters:
    - antenna_means: List of tuples where each tuple contains (antenna_id, mean_amplitude).
    - method: Method to calculate cutoff. Options are 'median' or 'stddev'.
    - factor: Multiplier for the standard deviation method.
    
    Returns:
    - meancutoff: Calculated cutoff value.
    """
    # Extract mean amplitudes from the list of tuples
    amplitudes = [mean for _, mean in antenna_means]
    
    if method == 'median':
        # Use the median of mean amplitudes as the cutoff value
        meancutoff = np.median(amplitudes)
    elif method == 'stddev':
        # Use mean and standard deviation to set the cutoff value
        mean = np.mean(amplitudes)
        stddev = np.std(amplitudes)
        meancutoff = mean - factor * stddev
    else:
        raise ValueError("Unsupported method: {}".format(method))
    
    return meancutoff

def get_antenna_means(msfilename, scan_number, poldata, mygoodchans, meancutoff):
    """
    Get the mean amplitude values for each antenna in a single scan
    and identify antennas that are outliers based on the cutoff value.
    """
    # Open MS and initialize the MSMetaData tool
    msmd.open(msfilename)
    msmd.select({'scan': scan_number})
    
    # Get antenna list
    antennas = msmd.antennaids()
    
    # Initialize lists to store mean values and bad antennas
    antenna_means = []
    bad_antennas = []
    
    for ant in antennas:
        # Retrieve amplitude data for the specified polarization
        if poldata == 'RR':
            data = msmd.getdata(['DATA'], antenna=ant, scan=scan_number, pol='RR')
        elif poldata == 'LL':
            data = msmd.getdata(['DATA'], antenna=ant, scan=scan_number, pol='LL')
        else:
            raise ValueError("Unsupported polarization: {}".format(poldata))
        
        # Calculate the mean amplitude for the given channels
        amplitudes = data['data'][0, 0, mygoodchans]  # Select the channels
        mean_amplitude = np.mean(np.abs(amplitudes))
        
        antenna_means.append((ant, mean_amplitude))
        
        # Identify outliers based on the mean cutoff
        if mean_amplitude < meancutoff:
            bad_antennas.append(ant)
    
    msmd.close()
    
    return antenna_means, bad_antennas

def flag_bad_antennas(msfilename, scan_number, bad_antennas, flagbadants=True):
    """
    Flag bad antennas in the MS file for a given scan number.
    """
    if not bad_antennas:
        logging.info("No bad antennas found for scan {}".format(scan_number))
        return
    
    logging.info("Bad antennas for scan {}: {}".format(scan_number, bad_antennas))
    
    flag_commands = []
    for ant in bad_antennas:
        flag_command = "mode='manual' antenna='{}' scan='{}'".format(ant, scan_number)
        flag_commands.append(flag_command)
    
    # Execute the flagging commands
    logging.info("Flagging commands:")
    for cmd in flag_commands:
        logging.info(cmd)
    
    if flagbadants:
        logging.info("Now flagging the bad antennas.")
        default(flagdata)
        flagdata(vis=msfilename, mode='list', inpfile=flag_commands)

# Example usage
msfilename = 'your_measurement_set.ms'
scan_number = 1
poldata = 'RR'  # or 'LL'
mygoodchans = range(0, 100)  # Example channel range

# Get the mean amplitudes and determine the cutoff value
antenna_means, bad_antennas = get_antenna_means(msfilename, scan_number, poldata, mygoodchans, meancutoff=0.5)

# Flag bad antennas
flag_bad_antennas(msfilename, scan_number, bad_antennas)
