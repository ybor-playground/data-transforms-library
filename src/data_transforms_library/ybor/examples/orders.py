import json
from data_transforms_library.ybor.driver.driver import Driver


def main():
    with open(
            "orders_azure.json",
            "r",
    ) as file:
        config = json.load(file)

    driver = Driver(config)
    driver.run()


if __name__ == "__main__":
    main()
