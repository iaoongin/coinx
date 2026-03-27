import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from coinx.collector.binance.series import collect_and_store_series


def build_parser():
    parser = argparse.ArgumentParser(description="抓取 Binance 历史序列并写入数据库")
    parser.add_argument(
        "series_type",
        choices=[
            "top_long_short_position_ratio",
            "top_long_short_account_ratio",
            "open_interest_hist",
            "klines",
            "global_long_short_account_ratio",
        ],
        help="序列类型",
    )
    parser.add_argument("--symbol", required=True, help="交易对，例如 BTCUSDT")
    parser.add_argument("--period", required=True, help="周期，例如 5m、15m、1h")
    parser.add_argument("--limit", required=True, type=int, help="抓取条数")
    return parser


def main():
    args = build_parser().parse_args()
    result = collect_and_store_series(
        series_type=args.series_type,
        symbol=args.symbol,
        period=args.period,
        limit=args.limit,
    )
    print(
        f"已完成采集: type={result['series_type']}, symbol={result['symbol']}, "
        f"period={result['period']}, affected={result['affected']}"
    )


if __name__ == "__main__":
    main()
