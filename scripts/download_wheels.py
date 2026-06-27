"""Download all requirements as wheels for offline install"""
import urllib.request, json, os, sys

DOWNLOAD_DIR = 'wheels'
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

requirements = [
    'flask==2.3.2', 'Flask-JWT-Extended>=4.6.0', 'gunicorn==22.0.0',
    'requests==2.31.0', 'PySocks==1.7.1', 'apscheduler==3.10.4',
    'pymysql==1.1.0', 'cryptography==42.0.8', 'sqlalchemy==2.0.35',
    'psutil==5.9.8', 'python-dotenv==1.0.0',
]

def get_wheel_url(name, version):
    api = f'https://pypi.org/pypi/{name}/{version}/json'
    req = urllib.request.Request(api, headers={'User-Agent': 'pip/23.0'})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read())
    for u in data['urls']:
        if u['filename'].endswith('.whl') and 'win' in u['filename'].lower():
            return u['url'], u['filename']
    for u in data['urls']:
        if u['filename'].endswith('.whl') and 'any' in u['filename'].lower():
            return u['url'], u['filename']
    return None, None

for req in requirements:
    if '>=' in req:
        name, ver = req.split('>=')
    elif '==' in req:
        name, ver = req.split('==')
    else:
        name, ver = req, None

    print(f'Downloading {name}...', end=' ')
    try:
        if ver:
            url, fname = get_wheel_url(name, ver)
        else:
            print('skipped (no version)')
            continue
        if url:
            path = os.path.join(DOWNLOAD_DIR, fname)
            if not os.path.exists(path):
                urllib.request.urlretrieve(url, path)
            print(f'OK ({fname})')
        else:
            print('no wheel found')
    except Exception as e:
        print(f'FAIL: {e}')

print(f'\nDone. Files in {DOWNLOAD_DIR}/:')
for f in sorted(os.listdir(DOWNLOAD_DIR)):
    print(f'  {f}')
