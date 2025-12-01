#!/usr/bin/env python3
"""
Compare CCGrid results with V2 single-site results for mesh-10 configuration.
Handles both old format (separate CSV per agent) and V2 format (all_jobs.csv).
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Dict, List
from scipy import stats
import argparse

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 10)
plt.rcParams['font.size'] = 10


class CCGridDataLoader:
    """Load data from CCGrid format (separate CSV files per agent)."""

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.runs = []

    def load_all_runs(self):
        """Load all runs from CCGrid directory."""
        print(f"Loading CCGrid data from {self.base_dir}...")

        run_dirs = sorted([d for d in self.base_dir.glob("run*") if d.is_dir()],
                         key=lambda x: int(x.name.replace('run', '')))

        for run_dir in run_dirs:
            run_data = self.load_single_run(run_dir)
            if run_data:
                self.runs.append(run_data)

        print(f"  Loaded {len(self.runs)} runs")
        return self.runs

    def load_single_run(self, run_dir: Path) -> Dict:
        """Load a single run from CCGrid format."""
        try:
            all_selection_times = []
            all_scheduling_latencies = []
            all_wait_times = []

            # Load selection times from all agent files
            for csv_file in sorted(run_dir.glob("selection_time_*.csv")):
                df = pd.read_csv(csv_file)
                all_selection_times.extend(df['selection_time'].dropna().tolist())

            # Load scheduling latencies
            for csv_file in sorted(run_dir.glob("scheduling_latency_*.csv")):
                df = pd.read_csv(csv_file)
                all_scheduling_latencies.extend(df['scheduling_latency'].dropna().tolist())

            # Load wait times
            for csv_file in sorted(run_dir.glob("wait_time_*.csv")):
                df = pd.read_csv(csv_file)
                all_wait_times.extend(df['wait_time'].dropna().tolist())

            if not all_selection_times:
                return None

            return {
                'run_name': run_dir.name,
                'selection_times': all_selection_times,
                'scheduling_latencies': all_scheduling_latencies,
                'wait_times': all_wait_times,
            }

        except Exception as e:
            print(f"    Warning: Could not load {run_dir.name}: {e}")
            return None


class V2DataLoader:
    """Load data from V2 format (all_jobs.csv)."""

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.runs = []

    def load_all_runs(self):
        """Load all runs from V2 format directory."""
        print(f"Loading V2 data from {self.base_dir}...")

        run_dirs = sorted([d for d in self.base_dir.glob("run*") if d.is_dir()],
                         key=lambda x: int(x.name.replace('run', '')))

        for run_dir in run_dirs:
            run_data = self.load_single_run(run_dir)
            if run_data:
                self.runs.append(run_data)

        print(f"  Loaded {len(self.runs)} runs")
        return self.runs

    def load_single_run(self, run_dir: Path) -> Dict:
        """Load a single run from V2 format."""
        jobs_file = run_dir / "all_jobs.csv"

        if not jobs_file.exists():
            return None

        try:
            df_jobs = pd.read_csv(jobs_file)

            # Calculate metrics
            df_jobs['selection_time'] = df_jobs['assigned_at'] - df_jobs['selection_started_at']

            # Calculate wait time: use started_at if available, otherwise use assigned_at as fallback
            df_jobs['wait_time'] = df_jobs['started_at'] - df_jobs['submitted_at']
            df_jobs.loc[df_jobs['started_at'] == 0, 'wait_time'] = df_jobs['assigned_at'] - df_jobs['submitted_at']

            df_jobs['total_time'] = df_jobs['completed_at'] - df_jobs['submitted_at']

            # Extract scheduling latency from all_jobs.csv if column exists
            scheduling_latencies = []
            if 'scheduling_latency' in df_jobs.columns:
                scheduling_latencies = df_jobs['scheduling_latency'].dropna().tolist()
            else:
                # Fallback: try to load from separate file (for backward compatibility)
                latency_file = run_dir / "reasoning_vs_latency.csv"
                if latency_file.exists():
                    df_latency = pd.read_csv(latency_file)
                    scheduling_latencies = df_latency['scheduling_latency'].dropna().tolist()

            return {
                'run_name': run_dir.name,
                'selection_times': df_jobs['selection_time'].dropna().tolist(),
                'scheduling_latencies': scheduling_latencies,
                'wait_times': df_jobs['wait_time'].dropna().tolist(),
            }

        except Exception as e:
            print(f"    Warning: Could not load {run_dir.name}: {e}")
            return None


class ComparisonAnalyzer:
    """Compare CCGrid and V2 results."""

    def __init__(self, ccgrid_runs: List[Dict], v2_runs: List[Dict], output_dir: str):
        self.ccgrid_runs = ccgrid_runs
        self.v2_runs = v2_runs
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)

    def aggregate_metrics(self, runs: List[Dict], label: str) -> pd.DataFrame:
        """Aggregate metrics from runs."""
        records = []

        for run_data in runs:
            selection_times = run_data['selection_times']
            scheduling_latencies = run_data['scheduling_latencies']
            wait_times = run_data['wait_times']

            record = {
                'dataset': label,
                'run': run_data['run_name'],
                'mean_selection_time': np.mean(selection_times) if selection_times else np.nan,
                'median_selection_time': np.median(selection_times) if selection_times else np.nan,
                'std_selection_time': np.std(selection_times) if selection_times else np.nan,
                'p95_selection_time': np.percentile(selection_times, 95) if selection_times else np.nan,
                'p99_selection_time': np.percentile(selection_times, 99) if selection_times else np.nan,
                'mean_scheduling_latency': np.mean(scheduling_latencies) if scheduling_latencies else np.nan,
                'median_scheduling_latency': np.median(scheduling_latencies) if scheduling_latencies else np.nan,
                'mean_wait_time': np.mean(wait_times) if wait_times else np.nan,
                'median_wait_time': np.median(wait_times) if wait_times else np.nan,
            }

            records.append(record)

        return pd.DataFrame(records)

    def plot_distribution_comparison(self):
        """Plot distribution comparison for key metrics."""
        metrics = [
            ('selection_times', 'Selection Time (s)', 'Selection Time Distribution'),
            ('scheduling_latencies', 'Scheduling Latency (s)', 'Scheduling Latency Distribution'),
            ('wait_times', 'Wait Time (s)', 'Wait Time Distribution'),
        ]

        for metric_key, xlabel, title in metrics:
            fig, axes = plt.subplots(2, 2, figsize=(16, 12))
            fig.suptitle(f'{title}: CCGrid\'25 vs SWARM-Enhanced', fontsize=16, fontweight='bold')

            # Collect all data
            ccgrid_data = []
            v2_data = []

            for run in self.ccgrid_runs:
                if run[metric_key]:
                    ccgrid_data.extend(run[metric_key])

            for run in self.v2_runs:
                if run[metric_key]:
                    v2_data.extend(run[metric_key])

            # Skip if either dataset is empty
            if not ccgrid_data or not v2_data:
                print(f"  Skipping {metric_key} - insufficient data")
                plt.close(fig)
                continue

            # Box plot
            ax = axes[0, 0]
            ax.boxplot([ccgrid_data, v2_data], tick_labels=['CCGrid\'25', 'SWARM-Enhanced'])
            ax.set_ylabel(xlabel)
            ax.set_title('Box Plot Comparison')
            ax.grid(True, alpha=0.3)

            # Violin plot
            ax = axes[0, 1]
            parts = ax.violinplot([ccgrid_data, v2_data], positions=[1, 2], showmeans=True, showmedians=True)
            ax.set_xticks([1, 2])
            ax.set_xticklabels(['CCGrid\'25', 'SWARM-Enhanced'])
            ax.set_ylabel(xlabel)
            ax.set_title('Violin Plot Comparison')
            ax.grid(True, alpha=0.3)

            # Histogram overlay - use log scale if ranges differ significantly
            ax = axes[1, 0]
            ccgrid_range = max(ccgrid_data) - min(ccgrid_data)
            v2_range = max(v2_data) - min(v2_data)

            # If one range is >10x the other, use log scale
            if ccgrid_range > 10 * v2_range or v2_range > 10 * ccgrid_range:
                # Plot on log scale
                ccgrid_bins = np.logspace(np.log10(min(ccgrid_data) + 1e-10),
                                          np.log10(max(ccgrid_data) + 1e-10), 30)
                v2_bins = np.logspace(np.log10(min(v2_data) + 1e-10),
                                      np.log10(max(v2_data) + 1e-10), 30)
                ax.hist(ccgrid_data, bins=ccgrid_bins, alpha=0.5, label='CCGrid\'25', density=True, color='blue')
                ax.hist(v2_data, bins=v2_bins, alpha=0.5, label='SWARM-Enhanced', density=True, color='orange')
                ax.set_xscale('log')
                ax.set_title('Distribution Overlay (Log Scale)')
            else:
                # Regular linear scale
                ax.hist(ccgrid_data, bins=30, alpha=0.5, label='CCGrid\'25', density=True, color='blue')
                ax.hist(v2_data, bins=30, alpha=0.5, label='SWARM-Enhanced', density=True, color='orange')
                ax.set_title('Distribution Overlay')

            ax.set_xlabel(xlabel)
            ax.set_ylabel('Density')
            ax.legend()
            ax.grid(True, alpha=0.3)

            # CDF comparison
            ax = axes[1, 1]
            ccgrid_sorted = np.sort(ccgrid_data)
            v2_sorted = np.sort(v2_data)
            ccgrid_cdf = np.arange(1, len(ccgrid_sorted) + 1) / len(ccgrid_sorted)
            v2_cdf = np.arange(1, len(v2_sorted) + 1) / len(v2_sorted)

            ax.plot(ccgrid_sorted, ccgrid_cdf, label='CCGrid\'25', linewidth=2)
            ax.plot(v2_sorted, v2_cdf, label='SWARM-Enhanced', linewidth=2)
            ax.set_xlabel(xlabel)
            ax.set_ylabel('Cumulative Probability')
            ax.set_title('Cumulative Distribution Function')
            ax.legend()
            ax.grid(True, alpha=0.3)

            plt.tight_layout()
            filename = f'{metric_key}_distribution_comparison.png'
            plt.savefig(self.output_dir / filename, dpi=300, bbox_inches='tight')
            print(f"Saved: {self.output_dir / filename}")
            plt.close()

    def plot_run_metrics_comparison(self, df_ccgrid: pd.DataFrame, df_v2: pd.DataFrame):
        """Plot per-run metrics comparison."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Per-Run Metrics: CCGrid\'25 vs SWARM-Enhanced', fontsize=16, fontweight='bold')

        metrics = [
            ('mean_selection_time', 'Mean Selection Time (s)'),
            ('p95_selection_time', 'P95 Selection Time (s)'),
            ('mean_scheduling_latency', 'Mean Scheduling Latency (s)'),
            ('mean_wait_time', 'Mean Wait Time (s)'),
        ]

        for idx, (metric, ylabel) in enumerate(metrics):
            ax = axes[idx // 2, idx % 2]

            # Plot both datasets
            ax.plot(df_ccgrid.index, df_ccgrid[metric], 'o-', label='CCGrid\'25', alpha=0.6, markersize=3)
            ax.plot(df_v2.index, df_v2[metric], 's-', label='SWARM-Enhanced', alpha=0.6, markersize=3)

            # Add mean lines
            ccgrid_mean = df_ccgrid[metric].mean()
            v2_mean = df_v2[metric].mean()
            ax.axhline(ccgrid_mean, color='blue', linestyle='--', alpha=0.5, label=f'CCGrid\'25 mean: {ccgrid_mean:.4f}')
            ax.axhline(v2_mean, color='orange', linestyle='--', alpha=0.5, label=f'SWARM-Enhanced mean: {v2_mean:.4f}')

            ax.set_xlabel('Run Index')
            ax.set_ylabel(ylabel)
            ax.set_title(ylabel)
            ax.legend(fontsize=8)
            ax.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'per_run_comparison.png', dpi=300, bbox_inches='tight')
        print(f"Saved: {self.output_dir / 'per_run_comparison.png'}")
        plt.close()

    def statistical_tests(self, df_ccgrid: pd.DataFrame, df_v2: pd.DataFrame):
        """Perform statistical tests to compare the two datasets."""
        print("\n" + "="*80)
        print("STATISTICAL COMPARISON: CCGrid'25 vs SWARM-Enhanced")
        print("="*80)

        metrics = [
            ('mean_selection_time', 'Mean Selection Time'),
            ('median_selection_time', 'Median Selection Time'),
            ('p95_selection_time', 'P95 Selection Time'),
            ('mean_scheduling_latency', 'Mean Scheduling Latency'),
            ('mean_wait_time', 'Mean Wait Time'),
        ]

        results = []

        for metric, name in metrics:
            ccgrid_values = df_ccgrid[metric].dropna()
            v2_values = df_v2[metric].dropna()

            if len(ccgrid_values) == 0 or len(v2_values) == 0:
                continue

            # T-test
            t_stat, t_pvalue = stats.ttest_ind(ccgrid_values, v2_values)

            # Mann-Whitney U test (non-parametric)
            u_stat, u_pvalue = stats.mannwhitneyu(ccgrid_values, v2_values, alternative='two-sided')

            # Calculate effect size (Cohen's d)
            pooled_std = np.sqrt((ccgrid_values.std()**2 + v2_values.std()**2) / 2)
            cohens_d = (ccgrid_values.mean() - v2_values.mean()) / pooled_std if pooled_std > 0 else 0

            # Percentage change
            pct_change = ((v2_values.mean() - ccgrid_values.mean()) / ccgrid_values.mean()) * 100

            print(f"\n{name}:")
            print(f"  CCGrid'25:        mean={ccgrid_values.mean():.4f}, std={ccgrid_values.std():.4f}, "
                  f"median={ccgrid_values.median():.4f}")
            print(f"  SWARM-Enhanced:   mean={v2_values.mean():.4f}, std={v2_values.std():.4f}, "
                  f"median={v2_values.median():.4f}")
            print(f"  Change:  {pct_change:+.2f}%")
            print(f"  T-test:  t={t_stat:.4f}, p={t_pvalue:.6f} {'***' if t_pvalue < 0.001 else '**' if t_pvalue < 0.01 else '*' if t_pvalue < 0.05 else 'ns'}")
            print(f"  Mann-Whitney U: U={u_stat:.1f}, p={u_pvalue:.6f}")
            print(f"  Cohen's d: {cohens_d:.4f} ({'large' if abs(cohens_d) > 0.8 else 'medium' if abs(cohens_d) > 0.5 else 'small'})")

            results.append({
                'metric': name,
                'ccgrid_mean': ccgrid_values.mean(),
                'ccgrid_std': ccgrid_values.std(),
                'v2_mean': v2_values.mean(),
                'v2_std': v2_values.std(),
                'pct_change': pct_change,
                't_statistic': t_stat,
                't_pvalue': t_pvalue,
                'u_statistic': u_stat,
                'u_pvalue': u_pvalue,
                'cohens_d': cohens_d,
            })

        # Save statistical results
        df_stats = pd.DataFrame(results)
        stats_file = self.output_dir / 'statistical_comparison.csv'
        df_stats.to_csv(stats_file, index=False)
        print(f"\nStatistical results saved to: {stats_file}")

    def generate_summary_table(self, df_ccgrid: pd.DataFrame, df_v2: pd.DataFrame):
        """Generate a summary table comparing both datasets."""
        summary = {
            'Metric': [],
            'CCGrid Mean': [],
            'CCGrid Std': [],
            'SWARM-Enhanced Mean': [],
            'SWARM-Enhanced Std': [],
            'Change (%)': [],
        }

        metrics = [
            ('mean_selection_time', 'Mean Selection Time (s)'),
            ('median_selection_time', 'Median Selection Time (s)'),
            ('p95_selection_time', 'P95 Selection Time (s)'),
            ('p99_selection_time', 'P99 Selection Time (s)'),
            ('mean_scheduling_latency', 'Mean Scheduling Latency (s)'),
            ('mean_wait_time', 'Mean Wait Time (s)'),
        ]

        for metric, name in metrics:
            ccgrid_values = df_ccgrid[metric].dropna()
            v2_values = df_v2[metric].dropna()

            if len(ccgrid_values) == 0 or len(v2_values) == 0:
                continue

            pct_change = ((v2_values.mean() - ccgrid_values.mean()) / ccgrid_values.mean()) * 100

            summary['Metric'].append(name)
            summary['CCGrid Mean'].append(f"{ccgrid_values.mean():.4f}")
            summary['CCGrid Std'].append(f"{ccgrid_values.std():.4f}")
            summary['SWARM-Enhanced Mean'].append(f"{v2_values.mean():.4f}")
            summary['SWARM-Enhanced Std'].append(f"{v2_values.std():.4f}")
            summary['Change (%)'].append(f"{pct_change:+.2f}")

        df_summary = pd.DataFrame(summary)

        # Save as CSV
        summary_file = self.output_dir / 'comparison_summary.csv'
        df_summary.to_csv(summary_file, index=False)
        print(f"\nSummary table saved to: {summary_file}")

        # Print summary
        print("\n" + "="*80)
        print("SUMMARY TABLE")
        print("="*80)
        print(df_summary.to_string(index=False))

    def run_comparison(self):
        """Run the complete comparison analysis."""
        print("\n" + "="*80)
        print("CCGrid'25 vs SWARM-Enhanced Comparison")
        print("="*80)

        # Aggregate metrics
        print("\nAggregating metrics...")
        df_ccgrid = self.aggregate_metrics(self.ccgrid_runs, 'CCGrid')
        df_v2 = self.aggregate_metrics(self.v2_runs, 'V2')

        # Save aggregated data
        ccgrid_file = self.output_dir / 'ccgrid_aggregated.csv'
        v2_file = self.output_dir / 'v2_aggregated.csv'
        df_ccgrid.to_csv(ccgrid_file, index=False)
        df_v2.to_csv(v2_file, index=False)
        print(f"CCGrid aggregated data saved to: {ccgrid_file}")
        print(f"V2 aggregated data saved to: {v2_file}")

        # Generate plots
        print("\nGenerating comparison plots...")
        self.plot_distribution_comparison()
        self.plot_run_metrics_comparison(df_ccgrid, df_v2)

        # Statistical tests
        self.statistical_tests(df_ccgrid, df_v2)

        # Summary table
        self.generate_summary_table(df_ccgrid, df_v2)

        print("\n" + "="*80)
        print(f"Comparison complete! All outputs saved to: {self.output_dir}")
        print("="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Compare CCGrid and V2 mesh-10 results'
    )
    parser.add_argument(
        '--ccgrid-dir',
        type=str,
        default='runs/ccgrid/swarm/multi-jobs/10/repeated',
        help='CCGrid results directory'
    )
    parser.add_argument(
        '--v2-dir',
        type=str,
        default='single-site/run-mesh-10-100',
        help='V2 results directory'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='single-site/comparison_ccgrid_vs_v2',
        help='Output directory for comparison results'
    )

    args = parser.parse_args()

    # Load CCGrid data
    ccgrid_loader = CCGridDataLoader(args.ccgrid_dir)
    ccgrid_runs = ccgrid_loader.load_all_runs()

    # Load V2 data
    v2_loader = V2DataLoader(args.v2_dir)
    v2_runs = v2_loader.load_all_runs()

    if not ccgrid_runs or not v2_runs:
        print("Error: Could not load data from one or both directories")
        return

    # Run comparison
    analyzer = ComparisonAnalyzer(ccgrid_runs, v2_runs, args.output_dir)
    analyzer.run_comparison()


if __name__ == '__main__':
    main()
