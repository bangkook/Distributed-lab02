#!/bin/bash

# Exit on any error
set -e

# Step 1: Clean previous build artifacts
echo "Cleaning previous build..."
rm -rf build
rm -f kmeans.jar

# Step 2: Create build directory
echo "Creating build directory..."
mkdir -p build

# Step 3: Compile Java files
echo "Compiling Java sources..."
javac -classpath "$(hadoop classpath)" -d build src/*.java

# Step 4: Package into JAR file
echo "Creating JAR file..."
jar -cvf kmeans.jar -C build/ . config.xml

# Step 5: Remove previous output from HDFS
echo "Removing previous output from HDFS..."
hdfs dfs -rm -r -f /user/dis_lab2/output
hdfs dfs -rm -r -f /user/dis_lab2/input
hdfs dfs -mkdir /user/dis_lab2/input
hdfs dfs -put data/iris.data /user/dis_lab2/input


# Step 6: Run Hadoop job
echo "Running Hadoop KMeans job..."
hadoop jar kmeans.jar kmeans_hadoop.src.KmeansDriver /user/dis_lab2/input/iris.data /user/dis_lab2/output

