"""CLI entry point for the Aviation Data Platform."""

from __future__ import annotations

import sys
from datetime import date, datetime

import click

from src.config import get_config
from src.observability.logger import setup_logging


@click.group()
@click.option("--log-level", default="INFO", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]))
def cli(log_level: str) -> None:
    """Aviation Data Platform — Medallion Architecture Pipeline."""
    setup_logging(log_level)


@cli.command()
@click.option("--records", default=1000, help="Number of records per source to generate")
@click.option("--start-date", default=None, help="Start date (YYYY-MM-DD)")
@click.option("--end-date", default=None, help="End date (YYYY-MM-DD)")
def generate(records: int, start_date: str | None, end_date: str | None) -> None:
    """Generate synthetic aviation test data."""
    from generators.aviation_data import AviationDataGenerator

    config = get_config()
    generator = AviationDataGenerator(config)

    sd = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    ed = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

    counts = generator.generate_all(num_records=records, start_date=sd, end_date=ed)
    click.echo(f"\nGenerated data:")
    for source, count in counts.items():
        click.echo(f"  {source}: {count:,} records")
    click.echo(f"\nData written to: {config.data_dir}/raw/")


@cli.command()
@click.option("--layer", type=click.Choice(["bronze", "silver", "gold"]), default=None, help="Run a specific layer")
@click.option("--full", is_flag=True, help="Run the full pipeline (bronze -> silver -> gold)")
def run(layer: str | None, full: bool) -> None:
    """Run the data pipeline."""
    from src.pipeline.orchestrator import PipelineOrchestrator

    orchestrator = PipelineOrchestrator()

    try:
        if full or layer is None:
            summary = orchestrator.run_full()
            click.echo(f"\nPipeline complete. Batch: {summary['batch_id']}")
            click.echo(f"  Bronze: {summary['bronze']}")
            click.echo(f"  Gold totals: {summary['gold_totals']}")
        elif layer == "bronze":
            counts = orchestrator.run_bronze()
            click.echo(f"\nBronze ingestion complete: {counts}")
        elif layer == "silver":
            # Silver needs bronze to have run first; use latest batch
            results = orchestrator.run_silver()
            click.echo(f"\nSilver processing complete: {results}")
        elif layer == "gold":
            counts = orchestrator.run_gold()
            click.echo(f"\nGold build complete: {counts}")
    finally:
        orchestrator.close()


@cli.command()
@click.option("--layer", type=click.Choice(["bronze", "silver", "gold"]), default=None, help="Check a specific layer")
@click.option("--report", is_flag=True, default=True, help="Print quality report")
def quality(layer: str | None, report: bool) -> None:
    """Run data quality checks."""
    from src.pipeline.orchestrator import PipelineOrchestrator

    orchestrator = PipelineOrchestrator()
    try:
        orchestrator.run_quality(layer)
    finally:
        orchestrator.close()


@cli.command()
@click.option("--start-date", required=True, help="Backfill start date (YYYY-MM-DD)")
@click.option("--end-date", required=True, help="Backfill end date (YYYY-MM-DD)")
def backfill(start_date: str, end_date: str) -> None:
    """Backfill historical data for a date range (idempotent)."""
    from src.pipeline.orchestrator import PipelineOrchestrator

    sd = datetime.strptime(start_date, "%Y-%m-%d").date()
    ed = datetime.strptime(end_date, "%Y-%m-%d").date()

    orchestrator = PipelineOrchestrator()
    try:
        result = orchestrator.backfill(sd, ed)
        click.echo(f"\nBackfill complete for {start_date} to {end_date}")
        click.echo(f"  Batch: {result['batch_id']}")
        click.echo(f"  Generated: {result['generated']}")
        click.echo(f"  Gold totals: {result['gold_totals']}")
    finally:
        orchestrator.close()


if __name__ == "__main__":
    cli()
