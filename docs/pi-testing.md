# Raspberry Pi host-backed testing

The default CI-safe test suite uses fake sysfs/dev roots for capability detection. Real hardware execution tests should be run on Raspberry Pi hosts with matching media devices and FFmpeg/libcamera libraries installed.

## Pi 4 hardware encode checklist

- Host model reports Raspberry Pi 4 in `/proc/device-tree/model` or `/sys/firmware/devicetree/base/model`.
- `/sys/class/video4linux/*/name` or `/sys/class/media/*/model` exposes the `bcm2835-codec` H.264 encode path.
- FFmpeg exposes the `h264_v4l2m2m` encoder.
- Manifest target is `RASPBERRY_PI_4`.

## Pi 5 stateless decode checklist

- Host model reports Raspberry Pi 5.
- `/dev/media*` and `/sys/class/media/*/model` expose rpivid/pisp or equivalent stateless decode topology.
- FFmpeg exposes `h264_v4l2request` or `hevc_v4l2request` as appropriate.
- Manifest target is `RASPBERRY_PI_5` and hardware encode is not requested.

## Running host-gated tests

```bash
# Run Pi 4 hardware encode flow test
CAML_PI_HOST_TESTS=1 cargo test --features pi --test pi4_hardware_encode_flow

# Run Pi 5 stateless decode flow test
CAML_PI_HOST_TESTS=1 cargo test --features pi --test pi5_stateless_decode_flow
```

When `CAML_PI_HOST_TESTS` is unset, these tests return early with a skip message so normal development and generic CI remain deterministic.

