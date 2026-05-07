import os
metadata_file = '/workspace/models/4a153f76-432e-4159-bd79-66444413517a.pkl.spark/metadata/part-00000'
if os.path.exists(metadata_file):
    with open(metadata_file, 'r') as f:
        content = f.read()
    print(f'File size: {len(content)} bytes')
    print(f'Has NaN: {"NaN" in content}')
    # Show first occurrence of NaN if it exists
    if "NaN" in content:
        idx = content.find("NaN")
        print(f'NaN found at position {idx}')
        print(f'Context: {content[max(0, idx-50):idx+50]}')
