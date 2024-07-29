# post-process-dl-mcsmip
A post-processing script for the DL model (https://github.com/mariajmolina/ML-extremes-mcs.git) from MCSMIP (in prep.) to comply with common MCS criteria. 

These scripts were run on NCAR's Derecho supercomputer with write_scripts.csh. Workflow is as follows:
1. Split MCS tracks with split_tracks.py if needed for manual parallelization
2. Filter MCSs with mcs_filtering.py based on common MCS criteria outlined by MCSMIP protocols
3. Combine filtering into one file with post-process.py
