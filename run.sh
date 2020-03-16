#!/bin/bash
source /opt/conda/etc/profile.d/conda.sh
conda activate .
set -xeuo pipefail

export OMP_NUM_THREADS=1
# DOWNSIZED_OUTPUT_DIR="/storage/www/data/timestreams/cameras/downsized/${CAMTYPE}/${CAMNAME}~${FORMAT}_720x"

DOWNSIZED_SIZE=${DOWNSIZED_SIZE:-720x}

CAMERA_NAME=${CAMERA_NAME:-$(basename "${SOURCE_DIR}")}

AUDIT_FILE=${AUDIT_FILE:-/tmp/audit.tsv}
RMSCRIPT=${RMSCRIPT:-/tmp/rmscript.sh}
TRASH_DIR=${TRASH_DIR:-/trash/}
mkdir -p "${TRASH_DIR}"
mkdir -p "${DOWNSIZED_OUTPUT_DIR}" "${BUNDLE_OUTPUT_DIR}"

# CAMUPLOAD_CAM="/g/data/xe2/phenomics/camupload/${CAMERA}"
# renamed to SOURCE_DIR

#######################################################################
#                            Do the ingest                            #
#######################################################################


tstk ingest \
    --informat "${IMAGE_FORMAT}" \
    --bundle day \
    --ncpus $(nproc) \
    --downsized-output="${DOWNSIZED_OUTPUT_DIR}" \
    --downsized-bundle none \
    --downsized-size="${DOWNSIZED_SIZE}" \
    --audit-output="${AUDIT_FILE}" \
    --output=${BUNDLE_OUTPUT_DIR} \
    ${SOURCE_DIR}


# python3 influx-ingest.py \
#         --camera "$CAMERA_NAME" \
#         --file-type "$FORMAT" \
#         --audit-file "$AUDIT_FILE" 

tstk verify \
    --informat "${IMAGE_FORMAT}" \
    --resource "${BUNDLE_OUTPUT_DIR}" \
    --rm-script "${RMSCRIPT}" \
    --move-dest "${TRASH_DIR}" \
    "${SOURCE_DIR}"

chmod +x ${RMSCRIPT}

bash -xe "${RMSCRIPT}"
