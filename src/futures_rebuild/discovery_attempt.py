"""Add durable fit/attempt accounting to the existing ES/NQ resource budget."""
from pathlib import Path

from .canonical import assert_no_linklike_ancestors, canonical_bytes
from .discovery import (APPROVED, HISTORY, History, check_batch, finish_attempt,
                        require_adopted_scope, reserve_attempt)
from .errors import IntegrityError
from .es_nq_transfer_access import Budget, read, FAILURE
from .locking import FileLease


class CampaignBudget(Budget):
    """Existing clock, memory, exact output reservations and fatal-stop behavior.

    A distinct wrapper is needed to charge fits before the original fitter runs
    and retain that charge when the fitter fails. It adds no research reader.
    """
    def __init__(self, root, op, path, *, started=None, **options):
        self.root, self.operation_id = Path(root), op["operation_id"]
        from .es_nq_transfer_access import scope
        match = require_adopted_scope(self.root, op["operation_name"], scope(op))
        self.batch = read(self.root / APPROVED)["batch"]
        self.reservation = match["reservation"]
        if {k: v for k, v in self.reservation.items() if k != "fits"} != op["limits"]:
            raise IntegrityError("runtime operation limits differ from reviewed reservation")
        self.history = History(self.root / HISTORY)
        self.fits = 0
        super().__init__(path, op["limits"], started=started, **options)
        event = reserve_attempt(self.history, self.batch, self.operation_id, self.reservation)
        self._account_event(event)

    def _account_event(self, event):
        records = self.history.records()
        position = next(i for i, r in enumerate(records, 1) if r["event_id"] == event["event_id"])
        path = self.history.directory / f"{position:08d}.json"
        self.external.append(path)
        self.used += path.stat().st_size
        self.check()

    def reserve_fit(self):
        try:
            self.check()
            if self.fits >= self.reservation["fits"]:
                raise IntegrityError("fit budget exhausted before fitter")
            with FileLease(self.history.directory / "journal.lock"):
                records = check_batch(self.history, self.batch)
                payload = {"operation_id": self.operation_id, "fit_number": self.fits + 1}
                # Reserve a conservative 4 KiB metadata event before appending it.
                if self.used + 4096 + 512*1024 > self.limits["new_output_bytes"]:
                    raise IntegrityError("fit event would consume failure reserve")
                event = self.history._append(records, "FIT", payload)
            self.fits += 1
            self._account_event(event)
        except BaseException:
            self.failed = True
            raise

    def finish(self, status):
        self.check()
        if self.used + 4096 + 512*1024 > self.limits["new_output_bytes"]:
            raise IntegrityError("terminal event would consume failure reserve")
        event = finish_attempt(self.history, self.operation_id, {"execution": status, "fits": self.fits,
            "automatic_continuation": False, "confirmation_authorized": False})
        self._account_event(event)

    def fatal(self, exc):
        saved = super().fatal(exc)
        if saved:
            fatal_path = self.path / "fatal_stop.json"
            self.used += fatal_path.stat().st_size
            # Failure metadata uses the already reserved failure capacity. No
            # reader, fitter, retry or normal budget check can resume this run.
            if self.used + 4096 <= self.limits["new_output_bytes"]:
                event = finish_attempt(self.history, self.operation_id, {
                    "execution": "FAILED", "fits": self.fits, "fatal_record": str(fatal_path),
                    "automatic_continuation": False, "confirmation_authorized": False})
                records = self.history.records()
                path = self.history.directory / f"{len(records):08d}.json"
                self.external.append(path)
                self.used += path.stat().st_size
            from .discovery import result_summary, bind, markdown, CATALOG_SCHEMA
            records = self.history.records()
            study = next(r["payload"] for r in records if r["kind"] == "STUDY"
                         and r["payload"]["scientific_id"] in self.batch["studies"])
            summary = result_summary(study, execution="FAILED", completed={"terminal_operations": 0},
                expected={"terminal_operations": 1}, attempts=[self.operation_id],
                reason=f"Mechanical failure; see fatal_stop.json. Fits reserved: {self.fits}; completed fit count is not inferred.",
                data_scope={**{k:v for k,v in self.batch["scope"].items() if k != "stage"}, "data_kind":"DEVELOPMENT_REPORTED"})
            catalog = bind({"schema_version": CATALOG_SCHEMA, "payload_kind":"METADATA_ONLY", "summaries":[summary]}, "catalog_id")
            outputs = {"failed_study_summary.json": canonical_bytes(summary)+b"\n",
                       "failed_study_catalog.json": canonical_bytes(catalog)+b"\n",
                       "failed_study_summary.md": markdown(summary).encode("utf-8")}
            size = sum(map(len, outputs.values()))
            if size < FAILURE-4096 and self.used+size <= self.limits["new_output_bytes"]:
                for name, raw in outputs.items():
                    path = self.path/name
                    assert_no_linklike_ancestors(path)
                    with path.open("xb") as stream:
                        stream.write(raw)
                    self.used += len(raw)
        return saved

    def publish_transfer(self, result, *, data_kind="DEVELOPMENT_REPORTED"):
        from .discovery import bind, transfer_summary, markdown, CATALOG_SCHEMA
        records = self.history.records()
        study = next(r["payload"] for r in records if r["kind"] == "STUDY" and
                     r["payload"]["scientific_id"] == result["study_id"])
        summary = transfer_summary(study, result, fits=self.fits, operation_id=self.operation_id,
                                   data_kind=data_kind)
        terminal = {r["payload"]["operation_id"]: r["payload"]["summary"]["execution"]
                    for r in records if r["kind"] == "RESULT"}
        attempts = [r["payload"] for r in records if r["kind"] == "ATTEMPT"
                    and study["scientific_id"] in r["payload"]["studies"]]
        summary["attempts"] = [a["operation_id"] + " (" + (
            "COMPLETE" if a["operation_id"] == self.operation_id else
            terminal.get(a["operation_id"], "INCOMPLETE_NO_TERMINAL_RECORD")) + ")" for a in attempts]
        summary["attempts"] = [r["payload"]["reference"] + " (" + r["payload"]["execution"] + "; historical)"
            for r in records if r["kind"] == "LEGACY_ATTEMPT" and r["payload"]["scientific_id"] == study["scientific_id"]] + summary["attempts"]
        from .discovery import intervals
        summary["exposure"] = intervals(study["exposure"] + [x for a in attempts for x in a["exposure"]])
        summary.pop("result_id")
        summary = bind(summary, "result_id")
        self.save("study_summary", summary)
        self.save("study_catalog", bind({"schema_version": CATALOG_SCHEMA, "payload_kind": "METADATA_ONLY",
                                         "summaries": [summary]}, "catalog_id"))
        raw = markdown(summary).encode("utf-8")
        self.check()
        if self.used + len(raw) + FAILURE > self.limits["new_output_bytes"]:
            raise IntegrityError("Markdown report would consume failure reserve")
        path = self.path / "study_summary.md"
        assert_no_linklike_ancestors(path)
        with path.open("xb") as stream:
            stream.write(raw)
        self.used += len(raw)
        self.check()
