DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/environment.sh

pushd ../
aws s3 cp ./target/scala-2.10/treecanopy-ingest-assembly-0.2.0.jar $CODE_TARGET/ --region $AWS_REGION
popd

aws s3 cp bootstrap-geowave.sh $EMR_TARGET/bootstrap-geowave.sh --region $AWS_REGION
aws s3 cp post-bootstrap.sh $EMR_TARGET/post-bootstrap.sh --region $AWS_REGION
