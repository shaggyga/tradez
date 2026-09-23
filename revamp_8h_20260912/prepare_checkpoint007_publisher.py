from pathlib import Path
import difflib,hashlib,json
ROOT=Path(__file__).resolve().parent;OLD=ROOT/'publication_checkpoint_006';NEW=ROOT/'publication_checkpoint_007'
original=(OLD/'publish_checkpoint_v1.py').read_bytes()
assert hashlib.sha256(original).hexdigest()=='c82553c4dcb4c33c4fa9d3d65a3cbf307ed26b15ad2ec62476d8b0e94536c702'
assert not NEW.exists();NEW.mkdir()
text=original.decode().replace('checkpoint006','checkpoint007').replace('CHECKPOINT_006','CHECKPOINT_007').replace('checkpoint 006','checkpoint 007')
text=text.replace('FOREX_CURRENT_STATE_20260913.md','FOREX_MARKET_OPEN_SETUP_20260913.md')
start=text.index('REQUIRED_ROLES=');end=text.index('\nEXTENSIONS=',start)
text=text[:start]+"REQUIRED_ROLES=frozenset(('clock_rejection','archive_acceptance','news_acceptance','joint_acceptance','joint_registry','historical_seed_install','operating_profile','source_package','prior_science','current_process'))\nGATED_ROLES=frozenset(('joint_acceptance','joint_registry','historical_seed_install','source_package'))"+text[end:]
text=text.replace("import argparse,datetime,hashlib,json,math,os,re,urllib.parse","import argparse,datetime,hashlib,json,math,os,re,stat,urllib.parse")
text=text.replace("tuple(self.docs/name for name in DOC_NAMES)+(self.vault/'README.md',)","tuple(self.docs/name for name in DOC_NAMES)+(self.docs/'FOREX_CURRENT_STATE_20260913.md',self.vault/'README.md')")
text=text.replace("require(len(before_specs)==6,'six_guide_pins_required')","require(len(before_specs)==7,'seven_guide_pins_required')")
old="for part in (candidate,*candidate.parents):\n        if part.exists():require(not part.is_symlink() and not part.is_junction(),'reparse_path')"
new="for part in reversed((candidate,*candidate.parents)):\n        try:info=os.lstat(part)\n        except FileNotFoundError:break\n        require(not stat.S_ISLNK(info.st_mode) and not (getattr(info,'st_file_attributes',0)&0x400),'reparse_path')"
assert text.count(old)==1;text=text.replace(old,new)
test=(OLD/'test_publisher_v1.py').read_text().replace('checkpoint006','checkpoint007').replace('FOREX_CURRENT_STATE_20260913.md','FOREX_MARKET_OPEN_SETUP_20260913.md').replace("len(receipt['guide_hash_chain']),6","len(receipt['guide_hash_chain']),7")
for name,content in [('publish_checkpoint_v1.py',text),('test_publisher_v1.py',test),('run_tests_v1.py',(OLD/'run_tests_v1.py').read_text())]:
    (NEW/name).write_text(content)
(NEW/'PUBLISHER_DERIVATION.diff').write_text(''.join(difflib.unified_diff(original.decode().splitlines(True),text.splitlines(True),fromfile='checkpoint006',tofile='checkpoint007')))
(NEW/'DERIVATION.json').write_text(json.dumps({'predecessor_sha256':hashlib.sha256(original).hexdigest(),'new_source_sha256':hashlib.sha256((NEW/'publish_checkpoint_v1.py').read_bytes()).hexdigest(),'scope':'Dated documents/evidence roles and seven preserved guides; ancestor-first lstat reparse gate. No process, database, service or network capability.'},indent=2)+'\n')
print(str(NEW))
