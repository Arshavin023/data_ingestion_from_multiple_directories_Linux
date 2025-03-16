#!/bin/bash

# Directory to search
base_dir="/home/lamisplus/server/temp"

# Loop through each subdirectory in the base directory
for dir in "$base_dir"/*/; do
  if [ -d "$dir" ]; then
    echo "Processing directory: $dir"

    # Navigate to the subdirectory
    cd "$dir" || continue

    # Run the find commands to delete files
    find . -type f -name '*202404*' -exec rm {} +
    find . -type f -name '*202403*' -exec rm {} +
    find . -type f -name '*202402*' -exec rm {} +
    find . -type f -name '*202401*' -exec rm {} +
    find . -type f -name '*202405*' -exec rm {} +
    find . -type f -name '*202409*' -exec rm {} +

    # Return to the base directory
    cd "$base_dir" || exit
  fi
done
