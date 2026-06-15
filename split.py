import re
import sys

def split_file():
    with open("crates/caml-ffmpeg/src/lib.rs", "r") as f:
        lines = f.readlines()
        
    modules = {
        "lib": [],
        "capabilities": [],
        "source": [],
        "rtsp": [],
        "device": [],
        "transcode": [],
        "h264": [],
        "error": []
    }
    
    current_module = "lib"
    for i, line in enumerate(lines):
        if "fn open_rtsp_input" in line:
            current_module = "rtsp"
        elif "fn open_device_input" in line:
            current_module = "device"
        elif "fn transcode_packets" in line or "struct VideoTranscoder" in line:
            current_module = "transcode"
        elif "fn extract_h264_config" in line or "fn normalize_h264_payload" in line or "avcc" in line:
            current_module = "h264"
            
        modules[current_module].append(line)
        
    for mod, content in modules.items():
        if content:
            with open(f"crates/caml-ffmpeg/src/{mod}.rs", "w") as f:
                f.writelines(content)

split_file()
