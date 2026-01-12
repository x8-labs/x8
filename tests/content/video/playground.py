import os

from x8.content.video import Video


def run():
    video = Video(
        url="https://storage.cloud.google.com/game-spotit/Tiger.mp4",
    )
    info = video.get_info()
    print(info)
    frame = video.seek_frame(timestamp=5, backward=True)
    print(frame)
    return
    # frame.image.show()
    frame.image.save("C:/tmp/test.jpg")
    audio = video.get_audio(start=11, end=20, format="wav")
    print(audio)
    print(audio.audio.get_info())
    audio.audio.save("C:/tmp/a.mp3", format="mp3")

    """
    while True:
        frame = video.next_frame()
        if frame is None:
            break
        if frame.key_frame:
            print(frame)
    """


def get_file_path(file):
    return os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "data",
        file,
    )


if __name__ == "__main__":
    run()
