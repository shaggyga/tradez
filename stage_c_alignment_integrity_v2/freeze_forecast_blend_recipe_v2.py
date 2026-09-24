"""Freeze the exact operator recipe before a retained-evidence blend run."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from forecast_blend_operator_v2 import recipe_for, sha


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--paths", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise ValueError("refuse_to_replace_frozen_blend_recipe")
    paths = json.loads(args.paths.read_text(encoding="utf-8"))
    recipe = recipe_for(paths)
    args.output.write_bytes((json.dumps(recipe, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode())
    print(json.dumps({"recipe_path": str(args.output), "recipe_sha256": sha(args.output)}, sort_keys=True))


if __name__ == "__main__":
    main()
