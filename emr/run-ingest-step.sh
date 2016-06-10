DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/environment.sh

JAR=$CODE_TARGET/treecanopy-ingest-assembly-0.2.0.jar

ON_FAILURE=CANCEL_AND_WAIT

INGEST_CLASS=treecanopy.Ingest

TILE_ARGS="--deploy-mode,cluster"
TILE_ARGS=${TILE_ARGS},--class,$INGEST_CLASS,--driver-memory,$DRIVER_MEMORY,--executor-memory,$EXECUTOR_MEMORY,--num-executors,$NUM_EXECUTORS,--executor-cores,$EXECUTOR_CORES
TILE_ARGS=${TILE_ARGS},--conf,spark.yarn.executor.memoryOverhead=$EXECUTOR_YARN_OVERHEAD
TILE_ARGS=${TILE_ARGS},$JAR

echo "TILE: $TILE_ARGS"

aws emr add-steps --cluster-id $CLUSTER_ID --steps \
  Name=Ingest-Tree-Canopy,ActionOnFailure=$ON_FAILURE,Type=Spark,Jar=$JAR,Args=[$TILE_ARGS]
