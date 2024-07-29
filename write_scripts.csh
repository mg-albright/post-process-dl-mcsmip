#!/bin/csh -fx 

set dollar = `echo '$'`
echo $dollar
set working_path="/glade/derecho/scratch/malbright/mcsmip/tracking/summer"
foreach model ( OBS OBSv7 )  # set model names
foreach part ( 01 02 03 )  # define number of chunks for manual parallelization, if needed
cd ${working_path}/${model}/filtering

cat << EOF > mcs_filtering_thresh0.03_part_${part}.csh 
#!/bin/csh -fx
##=======================================================================
#PBS -N filter_mcs
#PBS -A PROJ_NAME
#PBS -l walltime=12:00:00
#PBS -q main
#PBS -j oe
#PBS -l select=1:ncpus=36:mpiprocs=36:mem=100GB

setenv TMPDIR ${SCRATCH}/temp
mkdir -p ${TMPDIR}

module load conda
conda activate npl-2024a

python ${working_path}/mcs_filtering_from_split_compressed_serial.py ${model} tracks_mintime4_minsize1_overlap50-99_thresh0.03_radius0.0_updated_part_${part}.txt ${part}
EOF
qsub -V mcs_filtering_part_${part}.csh 
end
end
