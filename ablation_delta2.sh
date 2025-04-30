#!bin/bash

echo "--- running $0"
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
echo $script_dir

cur_f=$(basename $0)
cur_fn="${cur_f%.*}"
parent_log="logs/${cur_fn}_$(date +%Y%m%d_%H%M%S).log"
echo $parent_log

# Redirect all output to both terminal and log file
exec > >(tee -a "$parent_log") 2>&1

echo "Parent script started, logging to $parent_log"
delta2_list=(0.01 0.02 0.002 0.0002 0.00002 0.000002 0.0000002)
list_string='['
first=true
for k in ${delta2_list[@]}
do
  if "$first"; then
    list_string+="${k}"
    first=false
  else
    list_string+=", ${k}"
  fi
  # echo "bash ${script_dir}/ablation.sh --delta2 $k" ${@}
  bash ${script_dir}/ablation.sh --delta2 $k ${@}
done

list_string+=']'
echo $list_string

rt_file=${parent_log%.*}_rt.txt
awk -F': ' '/round_time_list:/ {print $2}' ${parent_log} > ${rt_file}

# python -c "import ast; print(ast.literal_eval('${list_string}'));"
python exp_data_plot.py --file ${rt_file} --param_list "${list_string}"

echo "Parent script completed"
