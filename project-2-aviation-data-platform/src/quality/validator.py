"""Quality validation engine — executes checks against DuckDB."""

from __future__ import annotations

from typing import Any

import duckdb

from src.observability.logger import get_logger
from src.quality.checks import (
    ALL_CHECKS,
    CheckResult,
    CheckSeverity,
    CheckStatus,
    QualityCheck,
)

log = get_logger(__name__)


class QualityValidator:
    """Runs data quality checks and collects results."""

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def run_all(self, layer: str | None = None) -> list[CheckResult]:
        """Execute all checks (optionally filtered by layer) and return results."""
        checks = ALL_CHECKS if layer is None else [c for c in ALL_CHECKS if c.layer == layer]
        results: list[CheckResult] = []

        for check in checks:
            result = self._execute_check(check)
            results.append(result)

        passed = sum(1 for r in results if r.status == CheckStatus.PASS)
        failed = sum(1 for r in results if r.status == CheckStatus.FAIL)
        warnings = sum(1 for r in results if r.status == CheckStatus.WARNING)

        log.info(
            "quality_validation_complete",
            total=len(results),
            passed=passed,
            failed=failed,
            warnings=warnings,
            layer=layer or "all",
        )
        return results

    def _execute_check(self, check: QualityCheck) -> CheckResult:
        """Execute a single quality check."""
        try:
            row = self._conn.execute(check.sql).fetchone()
            value = row[0] if row else None
        except Exception as e:
            log.error("quality_check_error", check=check.name, error=str(e)[:200])
            return CheckResult(
                check=check,
                status=CheckStatus.FAIL,
                actual_value=None,
                message=f"Check execution error: {e}",
            )

        # Evaluate result
        if check.threshold is not None:
            # For "not empty" checks: value must be > threshold
            if check.name.endswith("_not_empty") or check.name.endswith("_coverage"):
                if value is not None and value > check.threshold:
                    status = CheckStatus.PASS
                    message = f"Value {value} > threshold {check.threshold}"
                else:
                    status = CheckStatus.FAIL if check.severity == CheckSeverity.ERROR else CheckStatus.WARNING
                    message = f"Value {value} <= threshold {check.threshold}"
            # For null-rate checks (percentage): value must be <= threshold
            elif "null_rate" in check.name:
                actual = value if value is not None else 0.0
                if actual <= check.threshold:
                    status = CheckStatus.PASS
                    message = f"Null rate {actual} <= threshold {check.threshold}"
                else:
                    status = CheckStatus.WARNING if check.severity == CheckSeverity.WARNING else CheckStatus.FAIL
                    message = f"Null rate {actual} > threshold {check.threshold}"
            # For count-based checks: value should equal threshold (usually 0 = no violations)
            else:
                if value is not None and value <= check.threshold:
                    status = CheckStatus.PASS
                    message = f"Count {value} <= threshold {check.threshold}"
                else:
                    status = CheckStatus.FAIL if check.severity == CheckSeverity.ERROR else CheckStatus.WARNING
                    message = f"Count {value} > threshold {check.threshold}"
        else:
            status = CheckStatus.PASS if value == 0 else CheckStatus.FAIL
            message = f"Value: {value}"

        log.debug("quality_check_result", check=check.name, status=status.value, value=value)
        return CheckResult(check=check, status=status, actual_value=value, message=message)
