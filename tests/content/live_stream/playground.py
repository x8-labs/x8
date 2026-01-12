import os
import time

from x8.content.live_stream import LiveStream


def run():
    live_stream = LiveStream(
        source="https://nmxlive.akamaized.net/hls/live/529965/Live_1/index.m3u8",  # noqa
        min_resolution=1000,
        __provider__={
            "type": "av",
            "parameters": {"get_playlists": True, "get_segments": True},
        },
    )

    info = live_stream.get_info()
    print(info)
    frame = live_stream.get_frame()
    print(frame)
    frame.image.save("c:/tmp/1.jpg")
    audio = live_stream.get_audio(duration=10)
    audio.audio.save("c:/tmp/1.mp3")
    print(audio)
    live_stream.close()
    time.sleep(0)
    frame = live_stream.get_frame()
    print(frame)
    frame.image.save("c:/tmp/2.jpg")


def get_file_path(file):
    return os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "data",
        file,
    )


if __name__ == "__main__":
    run()
