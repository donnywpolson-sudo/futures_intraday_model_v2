# Reproducibility

The package is source-reproducible: a clean checkout of the pushed remediation source builds a self-checking cockpit with the same source and package inputs. It is not bit-reproducible: the clean candidate executable SHA-256 was `9fc2269b...`, while the canonical executable is `76b1dadd...`. This is consistent with nondeterministic PyInstaller output and is recorded as an accepted residual, not concealed as byte identity.

The exact legacy cockpit test lane is reproducible through `scripts/run_windows_host_root_pytest.ps1`. UI timings are bounded observations, not deterministic benchmarks, because OS page-cache state was uncontrolled.
