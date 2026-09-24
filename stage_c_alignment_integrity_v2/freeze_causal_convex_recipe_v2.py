"""Engineering-only freeze; routine operators must use the published pin."""
import argparse
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from causal_convex_operator_v2 import recipe_for, sha

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--paths', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    recipe = recipe_for(json.loads(args.paths.read_bytes()))
    with args.output.open('xb') as f:
        f.write((json.dumps(recipe, sort_keys=True, separators=(',', ':'), allow_nan=False)+'\n').encode())
    print(json.dumps({'recipe_path': str(args.output), 'recipe_sha256': sha(args.output)}))
