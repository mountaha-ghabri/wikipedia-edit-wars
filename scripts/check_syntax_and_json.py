import py_compile
import pathlib
import json
import sys
errors = False
print('Checking Python syntax in scripts/...')
for p in pathlib.Path('scripts').glob('*.py'):
    try:
        py_compile.compile(str(p), doraise=True)
        print('OK', p)
    except Exception as e:
        print('ERROR', p, e)
        errors = True

print('\nChecking JSON in config/...')
for p in pathlib.Path('config').glob('*.json'):
    try:
        with open(p, 'r', encoding='utf-8') as f:
            json.load(f)
        print('OK', p)
    except Exception as e:
        print('ERROR', p, e)
        errors = True

if errors:
    print('\nOne or more errors detected')
    sys.exit(1)
print('\nAll checks passed')
