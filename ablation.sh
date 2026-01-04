#!bin/bash

num_args=$#
echo "the num of args is $num_args"

# Initialize an associative array to store all arguments
declare -A args=(
  ["delta1"]=0.01
  ["delta2"]=0.002
  ["tau"]=3
  ["lambda"]=0.5
)

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    if [[ $1 == --* ]]; then
        # Extract argument name without the -- prefix
        arg_name="${1:2}"
        
        # If there's a next parameter and it doesn't start with --, use it as value
        if [[ -n $2 && $2 != --* ]]; then
            args["$arg_name"]="$2"
            shift 2
        else
            # For flags without values
            args["$arg_name"]="true"
            shift
        fi
    else
        # Handle non-option arguments if needed
        shift
    fi
done

# Convert arguments to individual variables
for key in "${!args[@]}"; do
    # Create variable with arg name equal to arg value
    # Using eval is generally discouraged but necessary here
    eval "$key='${args[$key]}'"
done

# Create log filename - either from explicit parameter or auto-generated
if [[ -n "${args["log_file"]}" ]]; then
    log_file="${args["log_file"]}"
else
    # Generate default log name from parameters
    log_parts=""
    for key in "${!args[@]}"; do
        # Skip the log_file parameter itself and empty values
        if [[ "$key" != "log_file" && -n "${args[$key]}" ]]; then
            log_parts="${log_parts}${key}-${args[$key]}_"
        fi
    done
    # Add uniqueness with script name and timestamp
    script_name=$(basename "$0" .sh)
    log_file="${script_name}_${log_parts%_}_$(date +%Y%m%d_%H%M%S).log"
fi
#
# # Create a log filename from all arguments
# log_parts=""
# for key in "${!args[@]}"; do
#     log_parts="${log_parts}${key}-${args[$key]}_"
# done

log_file="logs/dynamic_${log_file}"
echo "filename: ${log_file}"

# Redirect all output to the log file
exec > >(tee -a "$log_file") 2>&1

# Echo the variables to demonstrate they work
echo "Arguments processed:"
for key in "${!args[@]}"; do
    echo "$key = ${args[$key]}"
done

# Your script logic goes here...

# exp_id=tpch_shifting_1_MAB_80
echo python simulation/sim_c3ucb_vR.py --dynamic_flag "$@"

echo "Processing arguments and passing to Python script..."

# Build Python command with the same arguments
python_cmd="python simulation/sim_c3ucb_vR.py --dynamic_flag "
for key in "${!args[@]}"; do
    # Properly quote the values to handle spaces and special characters
    python_cmd+=" --$key '${args[$key]}'"
done

echo "Executing: $python_cmd"

# Execute the Python command
eval "$python_cmd"
