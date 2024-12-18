#!/bin/bash
RUSTFLAGS=-Awarnings cargo build --release || exit 1

CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-./target}

$CARGO_TARGET_DIR/release/rusthist --test-config --config-example
$CARGO_TARGET_DIR/release/iotrelay --test-config --config-example
$CARGO_TARGET_DIR/release/broker2db --test-config --config-example
$CARGO_TARGET_DIR/release/telemetry_service --test-config --config-example
$CARGO_TARGET_DIR/release/realtime --test-config --config-example

$CARGO_TARGET_DIR/release/rusthist --test-config
$CARGO_TARGET_DIR/release/iotrelay --test-config
$CARGO_TARGET_DIR/release/broker2db --test-config
$CARGO_TARGET_DIR/release/telemetry_service --test-config
$CARGO_TARGET_DIR/release/realtime --test-config

# mv iotrelay iotrelay-`date -r iotrelay '+%FT%T'`
# mv rusthist rusthist-`date -r rusthist '+%FT%T'`
# mv broker2db broker2db-`date -r broker2db '+%FT%T'`
# mv telemetry_service telemetry_service-`date -r telemetry_service '+%FT%T'`

cp -f $CARGO_TARGET_DIR/release/iotrelay .
cp -f $CARGO_TARGET_DIR/release/rusthist .
cp -f $CARGO_TARGET_DIR/release/broker2db .
cp -f $CARGO_TARGET_DIR/release/telemetry_service .
cp -f $CARGO_TARGET_DIR/release/realtime .
