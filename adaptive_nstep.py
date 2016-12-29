import math

def get_adjusted_nstep(files, min_AR=0.75, max_AR=0.85, die_AR=0.4, delta_nstep=1):
    """Looks at the HMC output files in 'files', computes the acceptance rate and extracts nsteps (outer layer), and adjust nsteps to optimize HMC run.

    Arguments:
        files: Filename(s) of HMC outputs to examine.  Can be either one such filename or a list of them.  Note that if a list is provided, the most
            recent filename (whose nsteps we will adjust) should be the last (-1th) filename in the list.
        min_AR: Minimum acceptance rate (in 0.0-1.0 form).  If the AR drops below this, increase nsteps.
        max_AR: Maximum acceptance rate (in 0.0-1.0 form).  If the AR rises above this, decrease nsteps.
        die_AR: If the acceptance rate has fallen below this, something has gone horribly wrong.  Return 'None', indicating the stream should be halted.
        delta_nstep: How much to adjust nstep by each time.

    Returns:
        nstep (int), the number of steps to run the next HMC run with.
        OR None, if the AR has fallen below 'die_AR'.  This indicates the stream should be halted.
    """
    
    if not isinstance(files, list):
        files = [files]

    accepts = 0
    rejects = 0
    nstep = None
    accept_rates = []
    for fn in files:
        with open(fn, 'r') as f:
            # file_contents = f.read()
            file_lines = f.readlines()
            
        AR_lines = filter(lambda L: 'ACCEPT:' in L or 'REJECT:' in L, file_lines)
        
        for line in AR_lines:
            # Count acceptances and rejections
            if "ACCEPT" in line:
                accepts += 1
            elif "REJECT" in line:
                rejects += 1
            
            # Track effective accept rate
            deltaS = float(line.split()[4][:-1]) # Trailing comma
            accept_rates.append(min(1, math.exp(-deltaS)))

        # accepts += file_contents.count('ACCEPT: ')
        # rejects += file_contents.count('REJECT: ')

            
        # Get nsteps1
        nstep_lines = filter(lambda L: L.startswith('nstep '), file_lines)
        nstep = int(nstep_lines[0].split()[1])
        # first_nstep_idx = file_contents.find('nstep ')
        # nstep = int(file_contents[first_nstep_idx:].split()[1]) # Implicitly: adjust nstep from last file provided

    AR_actual = float(accepts)/float(accepts+rejects)
    AR_effective = sum(accept_rates) / float(len(accept_rates))
    
    # Diagnostic output
    print "Actual accept rate:", AR_actual
    print "Average effective accept rate:", AR_effective

    if min(AR_actual, AR_effective) <= die_AR:
        return None # Signal that AR is below minimum AR; kill stream
    
    AR = AR_effective
    if AR < min_AR:
        return nstep + delta_nstep # Integrator too coarse, need to increase nsteps
    elif AR > max_AR:
        return nstep - delta_nstep # Integrator too fine, decrease nsteps
    else:
        return nstep # Integrator working as desired, keep nsteps same

    
