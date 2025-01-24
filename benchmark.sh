#!/bin/bash

captureStats() {
  local task="$1"
  local container="$2"
  local log_file="docker_stats_$task.log"
  local stop_file="stop_capture_$task"

  rm -f "$stop_file"

  while [ ! -f "$stop_file" ]; do
    docker stats --no-stream --format "{{.Name}}: CPU {{.CPUPerc}} | Mem {{.MemUsage}} | Net {{.NetIO}} | Block {{.BlockIO}}" >> "$log_file" &
    sleep 0.2
  done

  rm -f "$stop_file"
}

executeTask() {
  local task="$1"
  local container="$2"
  local command="$3"
  local iterations="${4:-5}"

  echo "Executing task '$task' $iterations times for benchmarking..."

  for ((i=1; i<=iterations; i++)); do
    echo "Iteration $i for task '$task'..."
    echo ""

    local log_file="docker_stats_$task.log"
    local stop_file="stop_capture_$task"
    : > "$log_file"
    
    captureStats "$task" "$container" &
    stats_pid=$!
    
    sleep 2
    
    start_time=$(date +%s%N)
    echo "----------------------------------------------------------------------------------------"
    docker exec -it "$container" /bin/bash -c "$command"
    end_time=$(date +%s%N)
    exec_time=$(((end_time - start_time) / 1000000))
    
    touch "$stop_file"
    wait $stats_pid

    echo "Iteration: $i: $exec_time ms"
    echo ""
    
    sleep 2
    
    echo "Iteration: $i: $exec_time ms" >> "$log_file"
    cat "$log_file" >> "$task"
    echo >> "$task"
    
    echo "----------------------------------------------------------------------------------------"
  done
  echo "Finished executing task '$task' $iterations times."
  rm "docker_stats_$task.log"
  
  echo "########################################################################################"
  echo ""
}

# -----------------------------------------------------------------------------
# Edit your commands here
# -----------------------------------------------------------------------------

importer="importer"
app="app"
db="orientdb"

#preprocessingCommand=""
importCommand='./dbcli import /data'

task10Command='./dbcli task 10'
task16Command='./dbcli task 16 "Tourism_in_Uttarakhand" 6 6'
task17Command='./dbcli task 17 "19th-century_works" "1887_directorial_debut_films" 6'
task18Command='./dbcli task 18 "19th-century_works" "1887_directorial_debut_films" 5'

# -----------------------------------------------------------------------------
# Uncomment commands for tasks you want to run
# -----------------------------------------------------------------------------

echo "Starting benchmarking..."

#executeTask "preprocessing" "$importer" "$preprocessingCommand" 1
executeTask "import" "$importer" "$importCommand" 1

iterationsNumber=5
executeTask "largestNumberOfChildren" "$app" "$task10Command" "$iterationsNumber"
executeTask "neighborhoodPopularity" "$app" "$task16Command" "$iterationsNumber"
executeTask "shortestPathPopularity" "$app" "$task17Command" "$iterationsNumber"
executeTask "directPathWithHighPopularity" "$app" "$task18Command" "$iterationsNumber"

echo "Benchmarking completed. Results saved to individual task files."
