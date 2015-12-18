set -e
set -x

if [ -z "$RDKAFKA_DOTNET_DIR" ]; then
    RDKAFKA_DOTNET_DIR="$(dirname "$0")/.."
fi

mkdir -p "$RDKAFKA_DOTNET_DIR/.cache"
pushd "$RDKAFKA_DOTNET_DIR/.cache"

if [ ! -e nuget.exe ]; then
    wget https://dist.nuget.org/win-x86-commandline/latest/nuget.exe
fi

mono nuget.exe install RdKafka.Internal.librdkafka-Darwin -Prerelease -Version 0.9.1-pre

DYLIB=`pwd`/RdKafka.Internal.librdkafka-Darwin.0.9.1-pre/runtimes/osx/native/librdkafka.1.dylib
chmod +x $DYLIB

popd

RUNTIMES="osx.10.11-x64 osx.10.10-x64 osx.10.9-x64"

for RUNTIME in $RUNTIMES
do
    TARGET="$RDKAFKA_DOTNET_DIR/src/RdKafka/runtimes/$RUNTIME/native"
    mkdir -p $TARGET
    cp $DYLIB $TARGET/librdkafka.dylib
done

# Due to dnx bug on shutdown, otherwise really shouldn't be there
cp $DYLIB $RDKAFKA_DOTNET_DIR/librdkafka.dylib
