import sys
import dask
import numpy as np
import xarray as xr

# model name from DYAMOND data
model = str(sys.argv[1])

# Load tracking NetCDF file
path = f"/glade/derecho/scratch/malbright/mcsmip/tracking/summer/{model}/"
file = f"blobs_summer_{model}_mintime4_minsize1_overlap50-99_thresh0.03_radius0.0"
ds = xr.open_dataset(path + file + ".nc")

# load detection NetCDF file
detection_path = "/glade/derecho/scratch/malbright/mcsmip/detection/summer/"
detection_file = f"MCSMIP_Summer_DL_{model}.nc"
detection_tb = xr.open_dataset(detection_path + detection_file).Tb
detection_precip = xr.open_dataset(detection_path + detection_file).precipitation

# Define constants
resolution = 0.1  # degrees
km_per_degree = 111.32  # approximate value for latitude
km2_per_pixel = (resolution * km_per_degree) ** 2


# Define functions to calculate area and rainfall volume
def calculate_area(mask):
    return np.sum(mask) * km2_per_pixel


def calculate_rainfall_volume(precip):
    return np.sum(precip, axis=(1, 2)) * km2_per_pixel


# Criteria 1: Area with Tb <= 241 K >= 40000 km^2 for
# at least 4 continuous hours
def criteria_1(tb_mask):
    # filter Tb for threshold
    tb_filtered = tb_mask <= 241
    # calculate area of each pixel that meets threshold
    area_per_hour = tb_filtered * km2_per_pixel
    # calculate total area at each time step
    rolling_area = np.sum(area_per_hour, axis=(1, 2))
    # Create a kernel for convolution to find consecutive hours
    kernel = np.array([1, 1, 1, 1])
    # Convolve to find consecutive sums exceeding the threshold
    convolved = np.convolve(rolling_area >= 40000, kernel, mode="valid")
    # Check if any consecutive 4 hours meet the condition
    if np.any(convolved >= 4):
        return True
    else:
        return False


# Criteria 2: Tb < 225 K exists during MCS lifetime
def criteria_2(tb_mask):
    return np.any(tb_mask <= 225)


# Criteria 3: Minimum peak precipitation of 10 mm/hr for 4 continuous hours
def criteria_3(precip_mask):
    # determine if any timestep meets precipitaiton threshold
    precip_thresh = np.any(precip_mask >= 10, axis=(1, 2))
    # Create a kernel for convolution to find consecutive hours
    kernel = np.array([1, 1, 1, 1])
    # Convolve to find consecutive hours meeting the threshold
    convolved = np.convolve(precip_thresh, kernel, mode="valid")
    # Check if any consecutive 4 hours meet the condition
    if np.any(convolved >= 4):
        return True
    else:
        return False


# Criteria 4: Minimum rainfall volume of 20000 km^2 mm/hr at least
# once in the lifetime
def criteria_4(precip):
    return np.any(calculate_rainfall_volume(precip) >= 20000)


# load chunk of mcs ids
txt_file = str(sys.argv[2])
mcs_ids = np.loadtxt(path + "filtering/" + txt_file)


# Function to process MCS IDs
def process_mcs(mcs_id, ds, detection_tb, detection_precip):
    mcs = ds.where(ds["MCS_mcs_prob"] == mcs_id, drop=True)
    tb_mask = (
        detection_tb.sel(time=mcs.time, lat=mcs.lat, lon=mcs.lon)
    ) * (mcs / mcs)
    tb_mask = tb_mask.MCS_mcs_prob
    precip_mask = (
        detection_precip.sel(time=mcs.time, lat=mcs.lat, lon=mcs.lon)
    ) * (mcs / mcs)
    precip_mask = precip_mask.MCS_mcs_prob

    # Check if all criteria are met at least once for the MCS
    if not (
        criteria_1(tb_mask)
        and criteria_2(tb_mask)
        and criteria_3(precip_mask)
        and criteria_4(precip_mask)
    ):
        # Set MCS to 0 if all criteria are not met at least once
        ds["MCS_mcs_prob"].values[ds["MCS_mcs_prob"] == mcs_id] = 0


# Process each MCS 
for mcs_id in mcs_ids:
    process_mcs(mcs_id, ds, detection_tb, detection_precip)

# Save the modified MCS dataset
part = str(sys.argv[3])
ds.to_netcdf(f"{path}{file}_FILTERED_part{part}.nc")
