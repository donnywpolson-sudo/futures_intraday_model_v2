"""Exact non-research recovery adoption, following the native daily build gate.

Preparation returns candidate bytes only. Adoption and execution need task approval.
"""
from pathlib import Path
from hashlib import sha256

from .canonical import sha256_file
from .causal_timeframe_storage import read_record, signed
from .daily_identity_campaign import contained, require
from .daily_identity_publication import OPERATION
from .causal_timeframe_publication import CLAIM
from .errors import UnauthorizedOperation

BASE='state/unpublished_evidence/daily_identity_recovery_v1/authorization_v2'
ADOPTION='configs/approved_daily_identity_recovery.json'
POLICY='src/futures_rebuild/research_gateway_policy.py'
MODULE='src/futures_rebuild/daily_identity_authorization.py'
COMMAND='DAILY_IDENTITY_RECOVERY_VALIDATION_AND_CONDITIONAL_ROLLOUT'


def require_scope(root, requested):
    adopted=read_record(contained(root,ADOPTION))
    path=adopted.get('coordinator_scope_path','')
    if not path.startswith(BASE+'/') or not path.endswith('.json'):
        raise UnauthorizedOperation('Invalid daily recovery scope reference')
    scope_path=contained(root,path)
    scope=read_record(scope_path)
    expected={'plan_id':scope['plan_id'],'claim':CLAIM,
        'approval_command':COMMAND,'approval_plan_id':scope['plan_id'],
        'approval_plan_sha256':scope['plan']['sha256']}
    if (adopted.get('schema')!='daily_identity_recovery_adoption/1'
        or adopted.get('state')!='ADOPTED_INACTIVE_RECEIPT_REQUIRED'
        or adopted.get('operation')!=OPERATION or dict(requested)!=expected
        or scope.get('schema')!='daily_identity_recovery_coordinator/1'
        or adopted.get('coordinator_scope_sha256')!=sha256_file(scope_path)
        or adopted.get('scope')!={k:expected[k] for k in ('plan_id','claim')}
        or adopted.get('implementation_sha256')!=sha256_file(contained(root,MODULE))
        or adopted.get('gateway_sha256')!=sha256_file(contained(root,POLICY))
        or sha256_file(contained(root,scope['plan']['path']))!=scope['plan']['sha256']):
        raise UnauthorizedOperation('Exact daily recovery operation is not adopted or bindings changed')
    return scope


def prepare_adoption(root, coordinator_scope_path):
    """Same five-line gateway hook pattern as causal_1d_native.prepare_adoption."""
    if not coordinator_scope_path.startswith(BASE+'/') or not coordinator_scope_path.endswith('.json'):
        raise UnauthorizedOperation('Recovery scope must be within its new attempt')
    path=contained(root,coordinator_scope_path)
    scope=read_record(path)
    require(scope.get('schema')=='daily_identity_recovery_coordinator/1', 'Wrong coordinator scope schema')
    require(sha256_file(contained(root,scope['plan']['path']))==scope['plan']['sha256'], 'Prepared recovery plan changed')
    original=contained(root,POLICY).read_bytes().decode('utf-8')
    newline='\r\n' if '\r\n' in original else '\n'
    anchor='    from .discovery import require_adopted_scope'+newline
    require(original.count(anchor)==1 and OPERATION not in original,'Unexpected gateway predecessor')
    hook=(f'    if operation == {OPERATION!r}:\n'
          '        from .daily_identity_authorization import require_scope\n'
          '        require_scope(Path(__file__).resolve().parents[2], scope)\n'
          '        return\n\n').replace('\n',newline)
    successor=original.replace(anchor,hook+anchor).encode('utf-8')
    adopted=signed(dict(schema='daily_identity_recovery_adoption/1',state='ADOPTED_INACTIVE_RECEIPT_REQUIRED',
        operation=OPERATION,scope={'plan_id':scope['plan_id'],'claim':CLAIM},
        coordinator_scope_path=coordinator_scope_path,coordinator_scope_sha256=sha256_file(path),
        gateway_sha256=sha256(successor).hexdigest(),predecessor_gateway_sha256=sha256_file(contained(root,POLICY)),
        implementation_sha256=sha256_file(contained(root,MODULE))))
    return successor,adopted
