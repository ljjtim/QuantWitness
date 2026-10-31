"""安装来源包全部配套bundle的精确依赖，并复验实际版本。"""
import argparse
import importlib.metadata
import json
from pathlib import Path
import subprocess
import sys


def install(package, output):
    locks={}
    manifests=[]
    for root in (package/'bundles',package.parent/'bundles'):
        if not root.is_dir():
            continue
        for path in sorted(root.glob('*/manifest.json')):
            manifest=json.loads(path.read_text(encoding='utf-8-sig'))
            manifests.append(str(path))
            for name,version in manifest['dependency_lock'].items():
                if name in locks and locks[name]!=version:
                    raise ValueError(f'来源bundle依赖冲突：{name} {locks[name]} / {version}')
                locks[name]=version
    if not manifests:
        raise ValueError('来源包没有配套bundle')
    output.mkdir(parents=True,exist_ok=True)
    requirements=output/'bundle-requirements.txt'
    requirements.write_text(''.join(f'{name}=={version}\n' for name,version in sorted(locks.items())),encoding='utf-8')
    if locks:
        subprocess.run([sys.executable,'-B','-m','pip','install','-r',str(requirements)],check=True)
    actual={name:importlib.metadata.version(name) for name in locks}
    if actual!=locks:
        raise ValueError('安装后实际依赖与bundle锁不一致')
    subprocess.run([sys.executable,'-B','-m','pip','check'],check=True)
    result=dict(status='pass',source_package=str(package),manifests=manifests,dependency_lock=locks,installed_versions=actual)
    (output/'bundle-dependencies.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--package',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    install(args.package.resolve(),args.output.resolve())
