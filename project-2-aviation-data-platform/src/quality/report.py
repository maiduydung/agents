"""Quality report generation — formats check results for display."""

from __future__ import annotations

from tabulate import tabulate

from src.quality.checks import CheckResult, CheckStatus


def generate_report(results: list[CheckResult]) -> str:
    """Generate a formatted quality report from check results."""
    lines: list[str] = []
    lines.append("")
    lines.append("=" * 80)
    lines.append("  DATA QUALITY REPORT")
    lines.append("=" * 80)
    lines.append("")

    # Summary
    total = len(results)
    passed = sum(1 for r in results if r.status == CheckStatus.PASS)
    failed = sum(1 for r in results if r.status == CheckStatus.FAIL)
    warnings = sum(1 for r in results if r.status == CheckStatus.WARNING)

    lines.append(f"  Total checks: {total}")
    lines.append(f"  Passed:       {passed}")
    lines.append(f"  Failed:       {failed}")
    lines.append(f"  Warnings:     {warnings}")
    lines.append("")

    # Detailed results by layer
    for layer in ("bronze", "silver", "gold"):
        layer_results = [r for r in results if r.check.layer == layer]
        if not layer_results:
            continue

        lines.append(f"  --- {layer.upper()} LAYER ---")
        table_data = []
        for r in layer_results:
            status_icon = {
                CheckStatus.PASS: "[PASS]",
                CheckStatus.FAIL: "[FAIL]",
                CheckStatus.WARNING: "[WARN]",
            }[r.status]
            table_data.append([
                status_icon,
                r.check.name,
                r.check.description,
                str(r.actual_value),
                r.message,
            ])

        lines.append(tabulate(
            table_data,
            headers=["Status", "Check", "Description", "Value", "Details"],
            tablefmt="simple",
            maxcolwidths=[8, 40, 45, 10, 40],
        ))
        lines.append("")

    # Overall verdict
    lines.append("-" * 80)
    if failed > 0:
        lines.append("  VERDICT: FAILED — Critical quality issues detected")
    elif warnings > 0:
        lines.append("  VERDICT: PASSED WITH WARNINGS — Review recommended")
    else:
        lines.append("  VERDICT: PASSED — All quality checks passed")
    lines.append("=" * 80)

    return "\n".join(lines)


def print_report(results: list[CheckResult]) -> None:
    """Print the quality report to stdout."""
    report = generate_report(results)
    print(report)
