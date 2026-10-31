"""安装现有 LEAN 市场元数据，历史申报数量仍由中国规则控制。"""
import argparse
import csv
from pathlib import Path
import shutil


def install(source, target):
    for name in ('market-hours', 'symbol-properties'):
        shutil.copytree(source / name, target / name, dirs_exist_ok=True)
    properties = target / 'symbol-properties/symbol-properties-database.csv'
    with properties.open(encoding='utf-8-sig', newline='') as stream:
        rows = list(csv.reader(stream))
    for row in rows:
        if row and row[0] == 'jqchina':
            row[7] = row[9] = '1'
    with properties.open('w', encoding='utf-8', newline='') as stream:
        csv.writer(stream).writerows(rows)
    for market in ('jqchina', 'usa'):
        for name in ('map_files', 'factor_files'):
            (target / 'equity' / market / name).mkdir(parents=True, exist_ok=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--target', type=Path, required=True)
    args = parser.parse_args()
    install(args.source, args.target)
