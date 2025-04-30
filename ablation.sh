#!bin/bash

num_args=$#
echo "the num of args is $num_args"

# --- Argument Parsing Loop ---
# while [[ $# -gt 0 ]]; do
#   case "$1" in
#     # --- Handle --key=value ---
#     --*=*)
#       # Extract key: Remove '--' prefix and everything after '='
#       key="${1%%=*}"
#       key="${key#--}"
#       # Extract value: Remove everything before and including '='
#       value="${1#*=}"
#
#       # Basic sanitization for variable name (optional but good practice)
#       # Allows letters, numbers, underscore; must start with letter or underscore
#       if [[ "$key" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
#          # Use declare to dynamically create the variable
#          # Using declare is safer than eval
#          declare "$key=$value"
#          echo "DEBUG: Set variable $key=$value (from $1)" >&2 # Debugging output
#       else
#          echo "Warning: Invalid variable name '$key' derived from argument '$1'. Skipping." >&2
#       fi
#       shift # Consume '--key=value' argument
#       ;;
#
#     # --- Handle --key value (and flags like --verbose) ---
#     --*)
#       # Extract key: Remove '--' prefix
#       key="${1#--}"
#
#       # Check if the next argument exists and is NOT another option
#       if [[ -n "$2" && "$2" != --* ]]; then
#         # It's a --key value pair
#         value="$2"
#         if [[ "$key" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
#            declare "$key=$value"
#            echo "DEBUG: Set variable $key=$value (from $1 $2)" >&2 # Debugging output
#         else
#            echo "Warning: Invalid variable name '$key' derived from argument '$1'. Skipping." >&2
#         fi
#         shift 2 # Consume both '--key' and 'value' arguments
#       else
#         # It's likely a flag (e.g., --verbose) or --key without a value
#         # Set the flag variable to 'true'
#         if [[ "$key" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
#            declare "$key=true"
#            echo "DEBUG: Set flag $key=true (from $1)" >&2 # Debugging output
#         else
#            echo "Warning: Invalid variable name '$key' derived from argument '$1'. Skipping." >&2
#         fi
#         shift # Consume only the '--key' (flag) argument
#       fi
#       ;;
#
#     # --- Handle Non-option Arguments (Positional Arguments) ---
#     *)
#       # You can collect these in an array, process them, or ignore them
#       echo "DEBUG: Ignoring non-option argument: $1" >&2
#       # Example: Collect positional arguments
#       # POSITIONAL_ARGS+=("$1")
#       shift # Consume the argument
#       ;;
#   esac
# done
# # --- End Argument Parsing ---
#
# # --- Example Usage of the Variables ---
# # echo "--- Parsed Variables ---"
# # echo "Username: ${username:-<not set>}" # Use default value if not set
# # echo "Password: ${password:-<not set>}" # Note: Avoid passing passwords like this!
# # echo "Port: ${port:-<not set>}"
# # echo "Verbose Flag: ${verbose:-<not set>}"
# # echo "Output File: ${output_file:-<not set>}"
# # echo "Mode: ${mode:-<not set>}"
#
# # Example check
# if [[ "${verbose}" == "true" ]]; then
#   echo "Verbose mode is enabled."
# fi

# echo "--- Wrapper Script: Received arguments: $@" >&2 # Optional: Debugging
# echo "--- Wrapper Script: Executing: python a.py $@" >&2 # Optional: Debugging

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
