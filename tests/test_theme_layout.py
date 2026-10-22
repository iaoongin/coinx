from pathlib import Path


def test_page_surface_radius_is_defined_by_the_shared_theme():
    stylesheet = Path('src/coinx/web/static/dark-theme.css').read_text(encoding='utf-8')

    assert '--page-surface-radius: var(--card-radius-lg);' in stylesheet


def test_bordered_outer_page_surfaces_use_the_shared_radius_token():
    expected = {
        'src/coinx/web/static/dark-theme.css': 'border-radius: var(--page-surface-radius);',
        'src/coinx/web/templates/coins_config.html': 'border-radius: var(--page-surface-radius);',
        'src/coinx/web/templates/funding_rate.html': 'border-radius: var(--page-surface-radius);',
        'src/coinx/web/templates/hedge_calculator.html': 'border-radius: var(--page-surface-radius);',
        'src/coinx/web/templates/index.html': 'border-radius: var(--page-surface-radius);',
        'src/coinx/web/templates/legacy_home.html': 'border-radius: var(--page-surface-radius);',
        'src/coinx/web/templates/market_rank.html': 'border-radius: var(--page-surface-radius);',
        'src/coinx/web/templates/market_structure_score.html': 'border-radius: var(--page-surface-radius);',
        'src/coinx/web/templates/notification_management.html': 'border-radius: var(--page-surface-radius);',
        'src/coinx/web/templates/rss_management.html': 'border-radius: var(--page-surface-radius);',
        'src/coinx/web/templates/task_jobs.html': 'border-radius: var(--page-surface-radius);',
    }

    for filename, declaration in expected.items():
        assert declaration in Path(filename).read_text(encoding='utf-8')
