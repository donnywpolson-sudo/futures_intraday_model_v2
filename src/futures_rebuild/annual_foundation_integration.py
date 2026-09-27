"""Exact, inactive reader successors for annual packaging. No activation CLI."""
from pathlib import Path
from .canonical import sha256_file
from .causal_timeframe_storage import contained
from .annual_foundation_release import require

BASE = 'state/unpublished_evidence/annual_foundation_migration_v1/reader_integration_v1'


def render(name, text):
    """Reviewed substitutions only; source drift cannot silently alter a patch."""
    def replace(before, after, count=1):
        nonlocal text
        require(text.count(before) == count, 'Annual patch predecessor differs: '+name+' / '+before[:80])
        text = text.replace(before, after)

    if name == 'annual_foundation_storage.py':
        replace('def _read_candidate(directory, *, manifest_sha256, months, maximum_group_bytes):',
                'def _read_candidate(directory, *, manifest_sha256, months, maximum_group_bytes, _packing=None):')
        replace('    manifest = read_record(manifest_path)\n',
                '    manifest = read_record(manifest_path)\n    if _packing is not None:\n        require(manifest.get("packing") == _packing, "Annual embedded packing differs")\n        manifest = _packing\n', 2)
        # validate_candidate is preparation-only and has no injected packing.
        tail = text.index('def validate_candidate(')
        text = text[:tail]+text[tail:].replace('    if _packing is not None:\n        require(manifest.get("packing") == _packing, "Annual embedded packing differs")\n        manifest = _packing\n', '', 1)
    elif name == 'hourly_quality_reader.py':
        replace('    def _native_month(self, market, month):',
                '    def _native_month(self, market, month, *, _annual_rows=None):')
        replace("        rows = read_table(payload, name='observations')", """        if 'annual_release' in entry:
            from .annual_foundation_release import read_months
            rows = (read_months(self.root, [entry])[month]['observations']
                    if _annual_rows is None else _annual_rows)
        else:
            rows = read_table(payload, name='observations')""")
        replace("            rows = [r for month in months(start_ns, end_ns) for r in self._native_month(market, month)\n                    if start_ns <= r['bar_start_ns'] < end_ns]", """            from .annual_foundation_release import read_months
            selected = months(start_ns, end_ns)
            annual = [self.entries[(market, m)] for m in selected
                      if 'annual_release' in self.entries.get((market, m), {})]
            bundles = read_months(self.root, annual)
            rows = [r for month in selected for r in self._native_month(market, month,
                    _annual_rows=bundles[month]['observations'] if month in bundles else None)
                    if start_ns <= r['bar_start_ns'] < end_ns]""")
    elif name == 'daily_quality_reader.py':
        replace('        recovered_evidence={}\n', """        recovered_evidence={}
        from .annual_foundation_release import read_months
        selected_months = months(start_ns, end_ns)
        annual = [e for e in self.index['entries'] if e['timeframe']=='1d'
                  and e['market']==market and e['month'] in selected_months and 'annual_release' in e]
        annual_bundles = read_months(self.root, annual)
""")
        replace("            bundle=read_bundle(contained(self.root,entry['directory']))",
                "            bundle=(annual_bundles[month] if 'annual_release' in entry\n                    else read_bundle(contained(self.root,entry['directory'])))")
    elif name == 'causal_timeframe_publication.py':
        replace("    path = contained(root, entry['manifest'])", """    if 'annual_release' in entry:
        from .annual_foundation_release import validate_month
        validate_month(root, entry)
    path = contained(root, entry['manifest'])""")
        replace("        if e['disposition'] == 'CERTIFIED':\n", """        if 'annual_release' in e:
            from .annual_foundation_release import validate_month_reference
            if e['timeframe'] not in ('1h','1d') or e['disposition'] != 'CERTIFIED':
                raise IntegrityError('Annual storage cannot authorize unavailable or unsupported entries')
            validate_month_reference(e)
        if e['disposition'] == 'CERTIFIED':
""")
        replace("        if without_proof != plan['index']: raise IntegrityError('published index differs from approved plan')",
                """        if 'annual_packaging' in index:
            from .annual_foundation_release import predecessor_index
            without_proof = predecessor_index(root, without_proof)
        if without_proof != plan['index']: raise IntegrityError('published index differs from approved plan')""")
    elif name == 'active_data_view.py':
        replace('        expected_parquet, expected_sidecar = _canonical_target(market, year)', """        if 'annual_release_key' in raw:
            from .annual_foundation_release import minute_paths
            expected_parquet, expected_sidecar = minute_paths(raw)
        else:
            expected_parquet, expected_sidecar = _canonical_target(market, year)""")
        replace('    expected = {"catalog.json"}\n', """    expected = {"catalog.json"}
    # Prior compact files remain rollback material; only canonical extras qualify.
    if any('annual_release_key' in e for e in catalog['entries']):
        allow_unreferenced_append_files = True
""")
        replace('        parquet_relative = str(raw["parquet_path"]).removeprefix("data/active/")', """        if 'annual_release_key' in raw:
            continue
        parquet_relative = str(raw["parquet_path"]).removeprefix("data/active/")""", 2)
        # verify_view must validate annual payload and standard permission bodies too.
        start = text.index('def verify_view(')
        before = """        if 'annual_release_key' in raw:
            continue
"""
        after = """        if 'annual_release_key' in raw:
            from .annual_foundation_release import verify_minute
            verify_minute(active.parent.parent, raw)
            continue
"""
        require(text[start:].count(before) == 1, 'Minute verification patch differs')
        text = text[:start]+text[start:].replace(before, after)
        replace('    relative = PurePosixPath(str(entry["parquet_path"])).relative_to(\n', """    if 'annual_release_key' in entry:
        from .annual_foundation_release import verify_minute
        return verify_minute(root, entry)
    relative = PurePosixPath(str(entry["parquet_path"])).relative_to(
""")
    elif name == 'daily_foundation_admission.py':
        replace('        sidecar = json.loads(contained(root, path).read_bytes())', """        if 'annual_release_key' in entry:
            from .annual_foundation_release import minute_permission
            sidecar = minute_permission(root, entry)
        else:
            sidecar = json.loads(contained(root, path).read_bytes())""")
    elif name == 'hourly_source_admission.py':
        replace("        sidecar = record(entry['sidecar_path'], entry['sidecar_sha256'])", """        sidecar = record(entry['sidecar_path'], entry['sidecar_sha256'])
        if 'annual_release_key' in entry:
            from .annual_foundation_release import minute_permission
            sidecar = minute_permission(root, entry)""")
    elif name == 'daily_identity_publication.py':
        replace('    _verify_reader_binding_successor(root, reference, cert)',
                '    packaging = _verify_reader_binding_successor(root, reference, cert)')
        replace("    require(all(current[k]==v for k,v in cert['legacy_implementation'].items() if k!='daily_quality_reader.py'),",
                "    require(packaging or all(current[k]==v for k,v in cert['legacy_implementation'].items() if k!='daily_quality_reader.py'),")
        replace("    original = cert['implementation']\n", """    from .annual_foundation_release import recovery_packaging
    if recovery_packaging(root, reference, cert):
        return True
    original = cert['implementation']
""")
    else:
        raise ValueError('Unreviewed annual successor: '+name)
    compile(text, name, 'exec')
    return text


FILES = ('annual_foundation_storage.py', 'active_data_view.py', 'causal_timeframe_publication.py',
         'hourly_quality_reader.py', 'daily_quality_reader.py', 'daily_foundation_admission.py',
         'daily_identity_publication.py', 'hourly_source_admission.py')


def prepare(root, budget):
    """Exclusive preparation writes only; caller meters the new attempt."""
    from .canonical import io_path
    root = io_path(Path(root).resolve())
    result = {}
    for name in FILES:
        relative = 'src/futures_rebuild/'+name
        original = contained(root, relative)
        before = original.read_bytes()
        text = before.decode('utf-8').replace('\r\n', '\n')
        candidate = render(name, text).encode()
        saved = budget.directory/'predecessors'/name
        target = budget.directory/'candidate'/name
        for path, raw in ((saved, before), (target, candidate)):
            path.parent.mkdir(parents=True, exist_ok=True)
            with budget.open(path) as stream:
                stream.write(raw)
        require(sha256_file(original) == sha256_file(saved), 'Active implementation changed during preparation')
        result[relative] = dict(predecessor={'path':saved.relative_to(Path(root)).as_posix(),
                'sha256':sha256_file(saved)}, candidate={'path':target.relative_to(Path(root)).as_posix(),
                'sha256':sha256_file(target)})
    return result


from contextlib import contextmanager


@contextmanager
def projected(root, overrides):
    """Exact candidate bytes in a private validation process; never a live reader.

The existing projection pattern changes file locations, not validation results.
All module state is restored even when a generated or historical check refuses.
"""
    import importlib
    import os
    from contextlib import ExitStack
    from unittest.mock import patch
    from .canonical import io_path
    root = Path(root).resolve()
    key = lambda p: os.path.normcase(str(io_path(Path(p).resolve())))
    mapping = {}
    for target, source in overrides.items():
        require(source.startswith('state/unpublished_evidence/'), 'Projection is not task-owned')
        mapping[key(contained(root, target))] = contained(root, source)
    project = lambda p: mapping.get(key(p), p)
    original_contained, original_hash = contained, sha256_file
    def projected_contained(base, relative):
        return project(original_contained(base, relative))
    def projected_hash(path):
        return original_hash(project(path))
    names = [n[:-3] for n in FILES]+['annual_foundation_release',
        'daily_source_admission', 'causal_timeframe_view', 'causal_1d']
    modules = {name:importlib.import_module('futures_rebuild.'+name) for name in names}
    with ExitStack() as stack:
        # Definitions execute in dependency order. Constructors and validators are
        # the actual successors, not monkeypatched success/no-op functions.
        for name in names:
            module = modules[name]
            namespace = dict(module.__dict__)
            relative = 'src/futures_rebuild/'+name+'.py'
            if name != 'causal_1d':
                source = original_contained(root, overrides[relative]) if relative in overrides else Path(module.__file__)
                exec(compile(source.read_text(), str(source), 'exec'), namespace)
            if 'contained' in namespace:
                namespace['contained'] = projected_contained
            if 'sha256_file' in namespace:
                namespace['sha256_file'] = projected_hash
            if name == 'causal_1d':
                namespace['__file__'] = str(root/relative)
                # implementation_binding's globals belong to the original module.
                stack.enter_context(patch.object(module, 'sha256_file', projected_hash))
                stack.enter_context(patch.object(module, '__file__', str(root/relative)))
                continue
            if name == 'active_data_view':
                loader = namespace['_load_canonical']
                namespace['_load_canonical'] = lambda path, description, f=loader: f(project(path), description)
            stack.enter_context(patch.dict(module.__dict__, namespace))
        yield
