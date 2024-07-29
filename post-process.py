import sys
import pathlib
import xarray as xr
import numpy as np

model = "OBS"  # update model name or loop through all
thresh = 0.03

data_dir = pathlib.Path(
    f"/glade/derecho/scratch/malbright/mcsmip/tracking/summer/{model}"
)
files = sorted(
    data_dir.glob(
        f"blobs_summer_{model}_mintime4_minsize1_overlap50-99_thresh{thresh}_radius0.0_FILTERED_part*.nc"
    )
)

product = xr.open_dataset(files[0])["MCS_mcs_prob"]
for file in files[1:]:
    product = product * xr.open_dataset(file)["MCS_mcs_prob"]
    product = product.where(product <= 0, 1)

path = f"/glade/derecho/scratch/malbright/mcsmip/tracking/summer/{model}"
ds = xr.open_dataset(
    f"{path}/blobs_summer_{model}_mintime4_minsize1_overlap50-99_thresh{thresh}_radius0.0.nc"
)
final_filtered = product * ds.MCS_mcs_prob
final_filtered.to_netcdf(
    f"{path}/blobs_summer_{model}_mintime4_minsize1_overlap50-99_thresh{thresh}_radius0.0_FILTERED.nc",
)
