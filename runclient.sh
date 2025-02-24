#!/bin/bash

source settings.sh

JAVA_CC=$JAVA_HOME/bin/javac
export CLASSPATH=".:gen-java:lib/*"


echo --- Running client
# args: 4 threads, 10 seconds, keys drawn from a set of 1000
$JAVA_HOME/bin/java A3Client $ZKSTRING /$USER 8 120 1000000

echo --- Analyzing linearizability
mkdir lintest_input
cp execution.log lintest_input/
mkdir lintest_output
$JAVA_HOME/bin/java -Xmx4g ca.uwaterloo.watca.LinearizabilityTest lintest_input/ lintest_output/ > /dev/null
cp lintest_output/scores.log scores.txt
echo Number of get operations returning junk: `cat scores.txt | grep 'Score = 2' | wc -l`
echo Number of other linearizability violations: `cat scores.txt | grep 'Score = 1' | wc -l`
rm -r lintest_input
rm -r lintest_output
echo
echo NOTE: Shut down both the primary and the secondary storage nodes after running this script, and then restart them.  Failure to do so will result in false linearizability violations the next time you run the client.
echo 
