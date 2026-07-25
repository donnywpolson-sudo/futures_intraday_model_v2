# -*- mode: python ; coding: utf-8 -*-

a = Analysis(
    ['futures_live_cockpit.py'],
    pathex=['src'],
    binaries=[],
    datas=[
        ('configs/alpha_tiered.yaml', 'configs'),
        ('configs/live_cockpit_smoke_plan.json', 'configs'),
        ('src/futures_rebuild/live_cockpit/assets', 'futures_rebuild/live_cockpit/assets'),
        ('THIRD_PARTY_NOTICES.md', '.'),
    ],
    hiddenimports=[
        'databento',
        'pandas',
        'yaml',
        'webview',
        'webview.platforms.edgechromium',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['numba', 'pytest', 'scipy'],
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
