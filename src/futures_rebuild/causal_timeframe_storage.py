"""Bounded compact monthly storage. No publication, deletion, or data authority."""
from __future__ import annotations

import ctypes
import io
import json
import os
import shutil
import time
from dataclasses import dataclass, asdict
from pathlib import Path, PurePosixPath

import pyarrow as pa
import pyarrow.parquet as pq

from .canonical import assert_no_linklike_ancestors, assert_plain_file, io_path, sha256_file, sha256_json, canonical_bytes
from .causal_observation_parquet import FILENAMES, SCHEMAS, _encode_group, _encode_physical, _decode_group, _decode_physical
from .errors import ContractError, IntegrityError


def contained(root: Path, relative: str) -> Path:
    if (type(relative) is not str or not relative or '\\' in relative or ':' in relative
            or any(p in ('', '.', '..') for p in relative.split('/')) or PurePosixPath(relative).is_absolute()):
        raise IntegrityError('noncanonical timeframe path')
    path = root / relative
    assert_no_linklike_ancestors(path)
    if not path.resolve().is_relative_to(root.resolve()):
        raise IntegrityError('timeframe path escapes root')
    return io_path(path)


def signed(payload: dict) -> dict:
    return {**payload, 'id': sha256_json(payload)}


def read_record(path: Path) -> dict:
    assert_no_linklike_ancestors(path)
    assert_plain_file(path)
    value = json.loads(io_path(path).read_bytes())
    if value.get('id') != sha256_json({k:v for k,v in value.items() if k != 'id'}):
        raise IntegrityError('timeframe record identity differs')
    return value


def memory_peak() -> int:
    if os.name == 'nt':
        class Counters(ctypes.Structure):
            _fields_ = [('cb', ctypes.c_ulong), ('faults', ctypes.c_ulong)] + [
                (n, ctypes.c_size_t) for n in ('peak_ws','ws','peak_paged','paged',
                'peak_nonpaged','nonpaged','commit','peak_commit')]
        c = Counters()
        c.cb = ctypes.sizeof(c)
        fn = ctypes.windll.psapi.GetProcessMemoryInfo
        fn.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_ulong]
        if not fn(ctypes.c_void_p(-1), ctypes.byref(c), c.cb):
            raise OSError('process memory measurement unavailable')
        return max(c.peak_ws, c.peak_commit)
    import resource
    import sys
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == 'darwin' else 1024)


def process_io():
    if os.name!='nt': return None
    class Counters(ctypes.Structure):
        _fields_=[(n,ctypes.c_ulonglong) for n in ('read_ops','write_ops','other_ops','read','write','other')]
    counters=Counters()
    fn=ctypes.windll.kernel32.GetProcessIoCounters
    fn.argtypes=[ctypes.c_void_p,ctypes.c_void_p]
    if not fn(ctypes.c_void_p(-1),ctypes.byref(counters)):
        raise OSError('process I/O counters unavailable')
    return counters.read,counters.write


def allocation(directory, maximum_files):
    """Metadata-only retained allocation; count hard-linked storage once."""
    logical=allocated=count=0
    seen=set()
    pending=[io_path(directory)]
    while pending:
        for path in pending.pop().iterdir():
            assert_no_linklike_ancestors(path)
            if path.is_dir():
                pending.append(path)
                continue
            assert_plain_file(path)
            count+=1
            if count>maximum_files: raise IntegrityError('retained file count limit reached')
            s=path.stat()  # Path.stat preserves Windows file identity unlike DirEntry.stat.
            logical+=s.st_size
            identity=(s.st_dev,s.st_ino)
            if identity in seen: continue
            seen.add(identity)
            if os.name=='nt':
                import msvcrt
                class StandardInfo(ctypes.Structure):
                    _fields_=[('allocation',ctypes.c_longlong),('size',ctypes.c_longlong),
                              ('links',ctypes.c_ulong),('delete_pending',ctypes.c_ubyte),('directory',ctypes.c_ubyte)]
                info=StandardInfo()
                fn=ctypes.WinDLL('kernel32',use_last_error=True).GetFileInformationByHandleEx
                fn.argtypes=[ctypes.c_void_p,ctypes.c_int,ctypes.c_void_p,ctypes.c_ulong]
                with path.open('rb') as handle:
                    if not fn(msvcrt.get_osfhandle(handle.fileno()),1,ctypes.byref(info),ctypes.sizeof(info)):
                        raise ctypes.WinError(ctypes.get_last_error())
                allocated+=info.allocation
            elif hasattr(s,'st_blocks'): allocated+=s.st_blocks*512
            else: raise IntegrityError('filesystem allocation measurement unavailable')
    return {'retained_logical_bytes':logical,'retained_allocated_bytes':allocated,'retained_files':count}


@dataclass(frozen=True)
class Limits:
    maximum_written_bytes: int
    maximum_memory_bytes: int
    maximum_seconds: int
    minimum_free_bytes: int = 64 * 2**30
    maximum_group_bytes: int = 256 * 2**20
    maximum_day_source_rows: int = 250_000
    maximum_files: int = 20_000

    def __post_init__(self):
        if any(type(v) is not int or v <= 0 for v in asdict(self).values()):
            raise ContractError('positive exact integer resource limits required')


class Budget:
    def __init__(self, directory: Path, limits: Limits):
        self.directory, self.limits = directory, limits
        self.started = time.monotonic()
        self.written = self.files = self.peak_memory = 0
        self.read_bytes = 0
        self.finalizing = False
        self.terminal_reserve = 0
        self.initial_io = process_io()
        self.existing_files=sum(1 for _,_,files in os.walk(io_path(directory)) for _ in files)
        if self.existing_files>limits.maximum_files:
            raise IntegrityError('retained file count already exceeds the cap')

    def check(self, reserve=0):
        if type(reserve) is not int or reserve < 0:
            raise ContractError('invalid write reservation')
        self.peak_memory = max(self.peak_memory, memory_peak())
        if (self.written + reserve + self.terminal_reserve > self.limits.maximum_written_bytes
                or not self.finalizing and (self.peak_memory > self.limits.maximum_memory_bytes
                or time.monotonic()-self.started > self.limits.maximum_seconds)
                or shutil.disk_usage(self.directory).free < self.limits.minimum_free_bytes + reserve + self.terminal_reserve):
            raise IntegrityError('timeframe resource limit reached before next operation')

    def record(self):
        self.peak_memory = max(self.peak_memory, memory_peak())
        now=process_io()
        return {**allocation(self.directory,self.limits.maximum_files),
                'application_written_bytes': self.written, 'created_files': self.files,
                'explicit_hash_read_bytes': self.read_bytes,
                'process_read_transfer_bytes':None if now is None else now[0]-self.initial_io[0],
                'process_write_transfer_bytes':None if now is None else now[1]-self.initial_io[1],
                'peak_process_memory_bytes': self.peak_memory, 'seconds': time.monotonic()-self.started,
                'write_measurement': 'APPLICATION_BYTES_INCLUDING_HEADERS_FOOTERS_NOT_NAND_WRITES'}

    def open(self, path):
        self.check()
        if self.existing_files+self.files >= self.limits.maximum_files:
            raise IntegrityError('file limit reached')
        assert_no_linklike_ancestors(path)
        stream = io_path(path).open('xb')
        self.files += 1
        return CountedOutput(stream, self)

    def save(self, path, record):
        raw = canonical_bytes(record) + b'\n'
        with self.open(path) as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())

    def hash(self,path):
        self.check()
        value=sha256_file(path)
        self.read_bytes+=path.stat().st_size
        self.check()
        return value


class CountedOutput(io.BufferedIOBase):
    """Count every accepted application write, including Arrow footer writes."""
    def __init__(self, stream, budget):
        self.stream, self.budget = stream, budget

    def writable(self): return True
    def tell(self): return self.stream.tell()
    def fileno(self): return self.stream.fileno()
    def flush(self):
        if not self.stream.closed:
            self.stream.flush()

    def write(self, data):
        self.budget.check(len(data))
        count = self.stream.write(data)
        self.budget.written += count
        if count != len(data):
            raise OSError('short timeframe write')
        return count

    def close(self):
        if not self.closed:
            try:
                self.flush()
            finally:
                self.stream.close()
                super().close()


class MonthlyWriter:
    """Five persistent writers, one aligned UTC-day group at a time."""
    def __init__(self, directory, budget):
        assert_no_linklike_ancestors(directory)
        io_path(directory).mkdir(parents=True, exist_ok=False)
        self.directory, self.budget = directory, budget
        self.streams, self.writers = {}, {}
        self.counts = {name: 0 for name in FILENAMES}
        self.groups = 0
        try:
            for name, filename in FILENAMES.items():
                stream = budget.open(directory / filename)
                self.streams[name] = stream
                self.writers[name] = pq.ParquetWriter(stream, SCHEMAS[name], compression='zstd',
                    compression_level=9, use_dictionary=True, write_statistics=True,
                    version='2.6', data_page_version='2.0', use_deprecated_int96_timestamps=False)
        except BaseException:
            self.close()
            raise

    def write(self, tables):
        self.budget.check()
        if set(tables) != set(FILENAMES) or not 0 < len(tables['observations']) <= 86_400:
            raise IntegrityError('invalid daily table set')
        for name in FILENAMES:
            physical = _encode_group(name, tables[name], tables['observations'])
            encoded = [_encode_physical(name, row) for row in physical]
            table = pa.Table.from_pylist(encoded, schema=SCHEMAS[name])
            if table.nbytes > self.budget.limits.maximum_group_bytes:
                raise IntegrityError('row group memory ceiling exceeded')
            # Reserve space for compression expansion and footer completion before the flush.
            self.budget.check(table.nbytes * 2 + 2**20)
            self.writers[name].write_table(table, row_group_size=max(1,len(encoded)))
            self.counts[name] += len(encoded)
            del table, encoded, physical
        self.groups += 1

    def close(self):
        failure = None
        for writer in self.writers.values():
            try: writer.close()
            except BaseException as exc: failure = failure or exc
        self.writers.clear()
        for stream in self.streams.values():
            try:
                stream.flush()
                os.fsync(stream.fileno())
            except BaseException as exc: failure = failure or exc
            finally: stream.close()
        self.streams.clear()
        if failure: raise failure


def iter_bundle_groups(directory, *, maximum_group_bytes=256*2**20):
    files = {}
    try:
        for name, filename in FILENAMES.items():
            path=directory/filename
            assert_no_linklike_ancestors(path)
            assert_plain_file(path)
            file=pq.ParquetFile(io_path(path))
            files[name]=file
            if not file.schema_arrow.equals(SCHEMAS[name],check_metadata=True):
                raise IntegrityError('monthly schema differs')
        count=files['observations'].metadata.num_row_groups
        if count <= 0 or any(f.metadata.num_row_groups != count for f in files.values()):
            raise IntegrityError('daily row groups are not aligned')
        for i in range(count):
            logical={}
            for name,file in files.items():
                meta=file.metadata.row_group(i)
                if meta.total_byte_size > maximum_group_bytes or meta.num_rows > 250_000:
                    raise IntegrityError('oversized row group refused before decode')
                rows=[_decode_physical(name,r) for r in file.read_row_group(i).to_pylist()]
                logical[name]=_decode_group(name,rows,logical.get('observations',()))
            yield logical
    finally:
        for f in files.values(): f.close()


def file_inventory(directory,budget=None):
    if {p.name for p in io_path(directory).iterdir()} != set(FILENAMES.values()):
        raise IntegrityError('monthly bundle contains missing or unregistered files')
    for filename in FILENAMES.values():
        assert_no_linklike_ancestors(directory/filename)
        assert_plain_file(directory/filename)
    return {name:{'filename':filename, 'size':io_path(directory/filename).stat().st_size,
                  'sha256':(sha256_file if budget is None else budget.hash)(io_path(directory/filename))}
            for name,filename in FILENAMES.items()}


def table_metadata(directory):
    """Read bounded Parquet footers; bind each table's exact schema and counts."""
    from hashlib import sha256
    result={}
    for name,filename in FILENAMES.items():
        with pq.ParquetFile(io_path(directory/filename)) as file:
            if not file.schema_arrow.equals(SCHEMAS[name],check_metadata=True):
                raise IntegrityError('timeframe table schema differs')
            result[name]={'rows':file.metadata.num_rows,'row_groups':file.metadata.num_row_groups,
                'schema_sha256':sha256(file.schema_arrow.serialize().to_pybytes()).hexdigest()}
    return result
