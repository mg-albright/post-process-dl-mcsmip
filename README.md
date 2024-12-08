# post-process-dl-mcsmip
A post-processing script for the [DL model](https://github.com/mariajmolina/ML-extremes-mcs.git) from [MCSMIP (Feng et al., in review)](https://essopenarchive.org/doi/full/10.22541/essoar.172405876.67413040) to comply with common MCS criteria. 

Full DL tracking method published [here](https://zenodo.org/records/13248327).

These scripts were run on NCAR's Derecho supercomputer. Workflow is as follows:
1. Split MCS tracks with split_tracks.py if needed for manual parallelization
2. Filter MCSs with write_scripts.csh, which calls mcs_filtering.py, based on common MCS criteria outlined by MCSMIP protocols
3. Combine filtering into one file with post-process.py
