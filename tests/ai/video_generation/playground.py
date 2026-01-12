import time

from x8.content.video import Video

from ._providers import get_component


def run():
    client = get_component(provider_type="openai")
    res = client.generate(prompt="Hamsters doing cartwheels on snow in Mars")
    print(res)

    while True:
        res = client.get(res.result.id)
        print("-------")
        print(res)
        if res.result.status in ["completed", "failed"]:
            break
        time.sleep(1)

    res = client.download(res.result.id)
    video = Video.load(res.result)
    video.save("a.mp4")
    print(video)


if __name__ == "__main__":
    run()
