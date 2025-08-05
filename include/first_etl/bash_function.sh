#!/bin/bash

add_log(){
    echo "_____________________________ i am abch"
    local CURRENT_DATE=$(date '+%Y-%m-%d')
    local head=$1
    local message=$2
    
     # Get absolute path of this script's directory
    local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

    # Create logs folder path relative to this script
    local log_dir="${script_dir}/../logs/first_etl"
    mkdir -p "$log_dir"  # ensure directory exists

    # Set file path using CURRENT_DATE (make sure it's defined)
    local file="${log_dir}/${CURRENT_DATE}_log.log"

    echo "${head} ${message}" >> "${file}"
}


process_data(){
    # echo "json data; $1"
    local file=$1
    local output='/shared_volume/proccessed_data.csv'
    if [ ! -f "$file" ]; then
        echo "$file : file does not exist"
        return 1
    fi
    jq -r '.[] | [.name.common, .capital[0], .region, .area, .population] | @tsv' "$file" > "$output"
    echo "$output"
}

export -f add_log

