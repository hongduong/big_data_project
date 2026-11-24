"""Điểm vào mỏng: gửi bản tin intraday mẫu vào Kafka."""

import argparse

from stocklake.streaming.producers.sample import send_sample


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-n", type=int, default=1, help="Số bản tin")
    args = parser.parse_args()
    print(send_sample(args.n))


if __name__ == "__main__":
    main()
