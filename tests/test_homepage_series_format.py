import pytest
from coinx.repositories.homepage_series import _fmt, _FMT_COL_WIDTHS


class TestFmtDisplayWidth:
    """验证 _fmt 日志格式化的 display width 对齐"""

    @pytest.mark.parametrize('label,expected_dw', [
        ('OI查询：', 8),
        ('OI查询完成：', 12),
        ('Kline查询：', 11),
        ('Kline查询完成：', 15),
        ('Taker查询：', 11),
        ('Taker查询完成：', 15),
        ('交易所加载开始：', 16),
        ('交易所加载完成：', 16),
        ('交易所门禁否决：', 16),
        ('交易所聚合完成：', 16),
        ('交易所聚合为空态：', 18),
        ('交易所加载失败：', 16),
        ('OI 加载完成：', 13),
        ('Kline 加载完成：', 16),
        ('Taker 加载完成：', 16),
        ('线程开始加载：', 14),
        ('线程加载完成：', 14),
        ('线程加载失败：', 14),
        ('开始并行加载：', 14),
        ('并行加载完成：', 14),
    ])
    def test_display_width(self, label, expected_dw):
        dw = sum(2 if ord(c) > 0x2e80 else 1 for c in label)
        assert dw == expected_dw, f'{label}: dw={dw} != {expected_dw}'

    def test_all_labels_align_same(self):
        labels = [
            'OI查询：', 'OI查询完成：', 'Kline查询：', 'Kline查询完成：',
            'Taker查询：', 'Taker查询完成：', '交易所加载开始：',
            'OI 加载完成：', 'Kline 加载完成：', 'Taker 加载完成：',
            '交易所加载完成：', '线程开始加载：', '线程加载完成：',
            '线程加载失败：', '开始并行加载：', '并行加载完成：',
            '交易所加载失败：', '交易所门禁否决：', '交易所聚合完成：',
            '交易所聚合为空态：',
        ]
        display_lengths = []
        for l in labels:
            line = _fmt(l, exchange='test')
            dl = sum(2 if ord(c) > 0x2e80 else 1 for c in line)
            display_lengths.append(dl)
        assert len(set(display_lengths)) == 1, \
            f'display widths differ: {dict(zip(labels, display_lengths))}'

    def test_label_width_constant(self):
        assert _FMT_COL_WIDTHS[0] > 0
        labels = [
            'OI查询：', 'OI查询完成：', 'Kline查询：', 'Kline查询完成：',
            'Taker查询：', 'Taker查询完成：', '交易所加载开始：',
            'OI 加载完成：', 'Kline 加载完成：', 'Taker 加载完成：',
            '交易所加载完成：', '线程开始加载：', '线程加载完成：',
            '线程加载失败：', '开始并行加载：', '并行加载完成：',
            '交易所加载失败：', '交易所门禁否决：', '交易所聚合完成：',
            '交易所聚合为空态：',
        ]
        for l in labels:
            dl = sum(2 if ord(c) > 0x2e80 else 1 for c in l)
            assert dl <= _FMT_COL_WIDTHS[0], f'{l}: dw={dl} > _FMT_COL_WIDTHS[0]={_FMT_COL_WIDTHS[0]}'

    def test_kwargs_appended_correctly(self):
        line = _fmt('OI查询：', exchange='binance', symbol数=32, 耗时='123ms')
        assert 'exchange=binance' in line
        assert 'symbol数=32' in line
        assert '耗时=123ms' in line
        # 前 3 个 kv 应依次出现在 col1-col3 位置
        assert line.index('exchange=binance') < line.index('symbol数=32') < line.index('耗时=123ms')

    def test_no_exchange_kwarg(self):
        line = _fmt('开始并行加载：', 交易所数=4, 列表=['binance', 'okx'])
        assert '交易所数=4' in line
        assert '列表=' in line

    def test_unicode_outside_cjk(self):
        line = _fmt('测试：', exchange='test')
        dw = sum(2 if ord(c) > 0x2e80 else 1 for c in line)
        assert dw > _FMT_COL_WIDTHS[0]
