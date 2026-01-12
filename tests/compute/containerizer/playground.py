from x8.compute.containerizer import Containerizer


def run():
    component = Containerizer()
    result = component.prepare(handle="api", path="../samples/compute")
    print(result)
    result = component.build(
        source=result.source, image_name="api", nocache=False
    )
    print(result)
    result = component.run(image_name=result.name)
    print(result)

    print(component.list_containers())
    print(component.list_images())
    component.stop(container_id=result.id)
    component.remove(container_id=result.id)
    component.delete(image_name="api")
    print(component.list_containers())
    print(component.list_images())


if __name__ == "__main__":
    run()
