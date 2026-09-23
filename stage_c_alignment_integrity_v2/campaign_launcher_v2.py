"""Machine-path launcher; no policy changes, model fitting, or broker access."""
import argparse,json,subprocess,sys,hashlib
from pathlib import Path
ROOT=Path(__file__).resolve().parent

def main():
    p=argparse.ArgumentParser()
    p.add_argument('-Action',choices=('status','run','resume','verify','inspect'),default='status')
    p.add_argument('-Config',type=Path,default=ROOT.parent/'FOREX_INSPECTOR_LOCAL.json')
    p.add_argument('-Query',type=Path)
    a=p.parse_args()
    try:
        c=json.loads(a.Config.read_text(encoding='utf-8'))
        recipe=ROOT/'CAMPAIGN_INSPECTOR_OPERATOR_RECIPE.json'
        if hashlib.sha256(recipe.read_bytes()).hexdigest()!=c['recipe_sha256']:
            raise ValueError('Pinned recipe changed. Preserve evidence; do not regenerate the approval.')
        cmd=[c['python'],'-I','-B',str(ROOT/'campaign_inspector_operator_v2.py'),a.Action,
             '--recipe',str(recipe),'--recipe-sha256',c['recipe_sha256'],'--paths',c['paths'],'--runs-dir',c['runs_dir']]
        if a.Action=='inspect':
            if a.Query is None:raise ValueError('Inspect requires an explicit original-record query JSON file.')
            cmd+=['--query',str(a.Query.resolve(strict=True))]
        return subprocess.call(cmd)
    except (OSError,ValueError,KeyError) as e:
        print(str(e),file=sys.stderr);return 2

if __name__=='__main__':sys.exit(main())
