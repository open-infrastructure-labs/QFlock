set -e
WORKING_DIR=$(pwd)
# By mapping the .m2 directory you can do an mvn install from
# within the container and use the result on your normal
# system.  And this also is a significant speedup in subsequent
# builds because the dependencies are downloaded only once.
echo "Creating ${WORKING_DIR}/build/.m2"
mkdir -p ${WORKING_DIR}/build/.m2
mkdir -p ${WORKING_DIR}/build/.gnupg
mkdir -p ${WORKING_DIR}/build/.ivy2
mkdir -p ${WORKING_DIR}/build/.cache
mkdir -p ${WORKING_DIR}/build/.sbt