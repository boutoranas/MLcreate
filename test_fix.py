import re
import os
import json

metadata_file = '/workspace/models/4a153f76-432e-4159-bd79-66444413517a.pkl.spark/metadata/part-00000'

# Read original
with open(metadata_file, 'r') as f:
    original = f.read()

# Apply fix
fixed = re.sub(r':NaN\b', ':null', original)
fixed = re.sub(r':Infinity\b', ':null', fixed)
fixed = re.sub(r':-Infinity\b', ':null', fixed)

print(f'Original has :NaN: {":NaN" in original}')
print(f'Fixed has :NaN: {":NaN" in fixed}')
print(f'Bytes changed: {len(original)} -> {len(fixed)}')

# Verify it's valid JSON
try:
    parsed = json.loads(fixed)
    print('Fixed JSON is valid!')
except Exception as e:
    print(f'Fixed JSON error: {e}')
