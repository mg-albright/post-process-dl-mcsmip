import sys
import xarray as xr
import numpy as np

# get model name
model = str(sys.argv[1])

# Load tracking NetCDF file
path = f"/glade/derecho/scratch/malbright/mcsmip/tracking/summer/{model}/"
minsize = 1
minoverlap = 50
maxoverlap = 99
thresh = str(sys.argv[2])
radius = 0.0
file = f"blobs_summer_{model}_mintime4_minsize{minsize}_overlap{minoverlap}-{maxoverlap}_thresh{thresh}_radius{radius}"
ds = xr.open_dataset(path + file + ".nc")

# split mcs ids into chunks
ds_na = ds["MCS_mcs_prob"].where(ds > 0)
mcs_ids = np.unique(ds_na.MCS_mcs_prob.data[~np.isnan(ds_na.MCS_mcs_prob.data)])
del ds_na

num_chunks = 3 # choose number of chunks 
chunks = np.array_split(mcs_ids, num_chunks)

chunk_ids = [
    "01",
    "02",
    "03"
]

for i, id in enumerate(chunk_ids):
    np.savetxt(f"{path}/filtering/tracks_mintime4_minsize{minsize}_overlap{minoverlap}-{maxoverlap}_thresh{thresh}_radius{radius}_part_{id}.txt", chunks[i])
