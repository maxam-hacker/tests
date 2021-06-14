echo "Removing files (if needed)..."

rm -rf \
    ./src/test/pixonic/*.class \
    ./src/test/pixonic/ingester/*.class

rm ingester.jar

echo "Compiling..."
javac -d bin \
	-Xlint \
    ./src/test/pixonic/*.java \
    ./src/test/pixonic/ingester/*.java

cd ./bin

echo "Jar processing..."
jar \
    cfm ../ingester.jar ../MANIFEST.MF \
    ./test/pixonic/*.class \
    ./test/pixonic/ingester/*.class