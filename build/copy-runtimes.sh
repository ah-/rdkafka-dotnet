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

LIB_VERSION="0.9.1-ci-6"

mono nuget.exe install RdKafka.Internal.librdkafka-Darwin -Prerelease -Version $LIB_VERSION
mono nuget.exe install RdKafka.Internal.librdkafka-Windows -Prerelease -Version $LIB_VERSION

popd

DYLIB=$RDKAFKA_DOTNET_DIR/.cache/RdKafka.Internal.librdkafka-Darwin.$LIB_VERSION/runtimes/osx/native/librdkafka.1.dylib
chmod +x $DYLIB

OSX_RUNTIMES="osx.10.11-x64 osx.10.10-x64 osx.10.9-x64"

for RUNTIME in $OSX_RUNTIMES
do
    TARGET="$RDKAFKA_DOTNET_DIR/src/RdKafka/runtimes/$RUNTIME/native"
    mkdir -p $TARGET
    cp $DYLIB $TARGET/librdkafka.dylib
done

TARGET="$RDKAFKA_DOTNET_DIR/src/RdKafka/runtimes/win7-x86/native"
mkdir -p $TARGET
cp $RDKAFKA_DOTNET_DIR/.cache/RdKafka.Internal.librdkafka-Windows.$LIB_VERSION/runtimes/win7-x86/native/* $TARGET

# Due to dnx bug on shutdown, otherwise really shouldn't be there
cp $DYLIB $RDKAFKA_DOTNET_DIR/librdkafka.dylib
