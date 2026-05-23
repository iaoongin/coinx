from pathlib import Path


def test_mysql_auth_dependencies_include_cryptography():
    requirements = Path('requirements.txt').read_text(encoding='utf-8').splitlines()
    packages = {
        line.split('==', 1)[0].strip().lower()
        for line in requirements
        if line.strip() and not line.lstrip().startswith('#')
    }

    assert 'pymysql' in packages
    assert 'cryptography' in packages
