# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path


repo_root = Path(SPECPATH).parents[1]
support_root = repo_root / 'FuturesLiveCockpit' / '_internal'

a = Analysis(
    [str(support_root / 'futures_live_cockpit.py')],
    pathex=[str(repo_root / 'src')],
    binaries=[],
    datas=[
        (str(repo_root / 'configs' / 'alpha_tiered.yaml'), 'configs'),
        (
            str(repo_root / 'configs' / 'live_cockpit_smoke_plan.json'),
            'configs',
        ),
        (
            str(repo_root / 'src' / 'futures_rebuild' / 'live_cockpit' / 'assets'),
            'futures_rebuild/live_cockpit/assets',
        ),
        (str(repo_root / 'THIRD_PARTY_NOTICES.md'), '.'),
        (str(support_root / 'FuturesLiveCockpit.spec'), '.'),
        (str(support_root / 'futures_live_cockpit.py'), '.'),
    ],
    hiddenimports=[
        'databento',
        'pandas',
        'yaml',
        'webview',
        'webview.platforms.edgechromium',
        'futures_rebuild.live_cockpit.smoke',
        'futures_rebuild.live_cockpit.live_model',
        'futures_rebuild.live_cockpit.model_runtime',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'numba',
        'pytest',
        'scipy',
        'futures_rebuild.live_cockpit.execution',
    ],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='FuturesLiveCockpit',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='FuturesLiveCockpit',
)
