#!/usr/bin/env python3
"""
Comprehensive plotting script for analyzing multiple runs across different configurations.
Supports comparison of topologies (ring, mesh) and scales (10, 30, 90 agents).
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Dict, List, Tuple
import argparse
from scipy import stats

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10


class MultiRunAnalyzer:
    """Analyzes and plots results from multiple experimental runs."""

    def __init__(self, base_dir: str, output_dir: str = None):
        self.base_dir = Path(base_dir)
        self.output_dir = Path(output_dir) if output_dir else self.base_dir / "plots"
        self.output_dir.mkdir(exist_ok=True)

        # Store aggregated data
        self.data = {}

    def parse_config_name(self, config_name: str) -> Tuple[str, int, int]:
        """Parse configuration name to extract topology, agents, jobs."""
        # e.g., "run-ring-10-100" -> ("ring", 10, 100)
        parts = config_name.split('-')
        if len(parts) >= 4:
            topology = parts[1]
            agents = int(parts[2])
            jobs = int(parts[3])
            return topology, agents, jobs
        return None, None, None

    def load_all_runs(self):
        """Load data from all configurations and runs."""
        print("Loading data from all runs...")

        for config_dir in sorted(self.base_dir.glob("run-*")):
            if not config_dir.is_dir():
                continue

            config_name = config_dir.name
            topology, agents, jobs = self.parse_config_name(config_name)

            if topology is None:
                continue

            print(f"  Processing {config_name}...")

            config_data = {
                'topology': topology,
                'agents': agents,
                'jobs': jobs,
                'runs': []
            }

            # Load each run
            for run_dir in sorted(config_dir.glob("run*")):
                run_data = self.load_single_run(run_dir)
                if run_data:
                    config_data['runs'].append(run_data)

            self.data[config_name] = config_data
            print(f"    Loaded {len(config_data['runs'])} runs")

        print(f"\nTotal configurations loaded: {len(self.data)}")

    def load_single_run(self, run_dir: Path) -> Dict:
        """Load data from a single run directory."""
        jobs_file = run_dir / "all_jobs.csv"
        latency_file = run_dir / "reasoning_vs_latency.csv"

        if not jobs_file.exists():
            return None

        try:
            df_jobs = pd.read_csv(jobs_file)

            # Calculate metrics
            run_data = {
                'run_dir': run_dir.name,
                'jobs': df_jobs
            }

            # Add latency data if available
            if latency_file.exists():
                df_latency = pd.read_csv(latency_file)
                run_data['latency'] = df_latency

            # Calculate derived metrics
            df_jobs['selection_time'] = df_jobs['assigned_at'] - df_jobs['selection_started_at']
            df_jobs['wait_time'] = df_jobs['started_at'] - df_jobs['submitted_at']
            df_jobs['execution_time'] = df_jobs['completed_at'] - df_jobs['started_at']
            df_jobs['total_time'] = df_jobs['completed_at'] - df_jobs['submitted_at']

            # Handle zero timestamps (not started)
            df_jobs.loc[df_jobs['started_at'] == 0, 'wait_time'] = np.nan
            df_jobs.loc[df_jobs['completed_at'] == 0, 'execution_time'] = np.nan
            df_jobs.loc[df_jobs['completed_at'] == 0, 'total_time'] = np.nan

            return run_data

        except Exception as e:
            print(f"    Warning: Could not load {run_dir}: {e}")
            return None

    def aggregate_metrics(self) -> pd.DataFrame:
        """Aggregate metrics across all runs and configurations."""
        records = []

        for config_name, config_data in self.data.items():
            topology = config_data['topology']
            agents = config_data['agents']
            jobs = config_data['jobs']

            for run_data in config_data['runs']:
                df_jobs = run_data['jobs']

                # Calculate aggregate metrics for this run
                record = {
                    'config': config_name,
                    'topology': topology,
                    'agents': agents,
                    'jobs': jobs,
                    'run': run_data['run_dir'],

                    # Latency metrics
                    'mean_selection_time': df_jobs['selection_time'].mean(),
                    'median_selection_time': df_jobs['selection_time'].median(),
                    'p95_selection_time': df_jobs['selection_time'].quantile(0.95),
                    'p99_selection_time': df_jobs['selection_time'].quantile(0.99),

                    # Wait time metrics
                    'mean_wait_time': df_jobs['wait_time'].mean(),
                    'median_wait_time': df_jobs['wait_time'].median(),

                    # Execution metrics
                    'mean_execution_time': df_jobs['execution_time'].mean(),
                    'total_execution_time': df_jobs['execution_time'].sum(),

                    # Total time metrics
                    'mean_total_time': df_jobs['total_time'].mean(),
                    'median_total_time': df_jobs['total_time'].median(),

                    # Job completion
                    'completed_jobs': (df_jobs['exit_status'] == 0).sum(),
                    'failed_jobs': (df_jobs['exit_status'] != 0).sum(),
                    'success_rate': (df_jobs['exit_status'] == 0).mean(),

                    # Load balancing
                    'unique_leaders': df_jobs['leader_id'].nunique(),
                    'leader_entropy': self.calculate_entropy(df_jobs['leader_id']),
                }

                # Add scheduling latency if available
                if 'latency' in run_data:
                    df_latency = run_data['latency']
                    record['mean_scheduling_latency'] = df_latency['scheduling_latency'].mean()
                    record['median_scheduling_latency'] = df_latency['scheduling_latency'].median()
                    record['mean_reasoning_time'] = df_latency['reasoning_time'].mean()

                records.append(record)

        return pd.DataFrame(records)

    def calculate_entropy(self, series):
        """Calculate Shannon entropy for load distribution."""
        value_counts = series.value_counts()
        probabilities = value_counts / len(series)
        return -np.sum(probabilities * np.log2(probabilities + 1e-10))

    def plot_latency_comparison(self, df: pd.DataFrame):
        """Plot latency metrics comparison across configurations."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Latency Metrics Comparison Across Configurations', fontsize=16, fontweight='bold')

        # Prepare data
        df_plot = df.copy()
        df_plot['config_label'] = df_plot['topology'] + '-' + df_plot['agents'].astype(str)

        # Mean Selection Time
        ax = axes[0, 0]
        sns.boxplot(data=df_plot, x='config_label', y='mean_selection_time', hue='topology', ax=ax)
        ax.set_title('Mean Selection Time')
        ax.set_xlabel('Configuration')
        ax.set_ylabel('Time (seconds)')
        ax.tick_params(axis='x', rotation=45)

        # P95 Selection Time
        ax = axes[0, 1]
        sns.boxplot(data=df_plot, x='config_label', y='p95_selection_time', hue='topology', ax=ax)
        ax.set_title('P95 Selection Time')
        ax.set_xlabel('Configuration')
        ax.set_ylabel('Time (seconds)')
        ax.tick_params(axis='x', rotation=45)

        # Mean Total Time
        ax = axes[1, 0]
        sns.boxplot(data=df_plot, x='config_label', y='mean_total_time', hue='topology', ax=ax)
        ax.set_title('Mean Total Job Time (Submission to Completion)')
        ax.set_xlabel('Configuration')
        ax.set_ylabel('Time (seconds)')
        ax.tick_params(axis='x', rotation=45)

        # Scheduling Latency (if available)
        ax = axes[1, 1]
        if 'mean_scheduling_latency' in df_plot.columns:
            sns.boxplot(data=df_plot, x='config_label', y='mean_scheduling_latency', hue='topology', ax=ax)
            ax.set_title('Mean Scheduling Latency')
        else:
            ax.text(0.5, 0.5, 'Scheduling latency data not available',
                   ha='center', va='center', transform=ax.transAxes)
        ax.set_xlabel('Configuration')
        ax.set_ylabel('Time (seconds)')
        ax.tick_params(axis='x', rotation=45)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'latency_comparison.png', dpi=300, bbox_inches='tight')
        print(f"Saved: {self.output_dir / 'latency_comparison.png'}")
        plt.close()

    def plot_scaling_analysis(self, df: pd.DataFrame):
        """Plot how metrics scale with number of agents."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Scaling Analysis: Impact of Agent Count', fontsize=16, fontweight='bold')

        metrics = [
            ('mean_selection_time', 'Mean Selection Time (s)'),
            ('mean_total_time', 'Mean Total Time (s)'),
            ('leader_entropy', 'Load Distribution Entropy'),
            ('success_rate', 'Success Rate')
        ]

        for idx, (metric, ylabel) in enumerate(metrics):
            ax = axes[idx // 2, idx % 2]

            for topology in df['topology'].unique():
                df_topo = df[df['topology'] == topology]

                # Group by agent count and calculate mean and std
                grouped = df_topo.groupby('agents')[metric].agg(['mean', 'std', 'count'])

                # Calculate 95% confidence interval
                ci = 1.96 * grouped['std'] / np.sqrt(grouped['count'])

                ax.errorbar(grouped.index, grouped['mean'], yerr=ci,
                           marker='o', label=topology.capitalize(), capsize=5, capthick=2)

            ax.set_title(ylabel)
            ax.set_xlabel('Number of Agents')
            ax.set_ylabel(ylabel)
            ax.legend()
            ax.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'scaling_analysis.png', dpi=300, bbox_inches='tight')
        print(f"Saved: {self.output_dir / 'scaling_analysis.png'}")
        plt.close()

    def plot_topology_comparison(self, df: pd.DataFrame):
        """Direct comparison between ring and mesh topologies."""
        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        fig.suptitle('Ring vs Mesh Topology Comparison', fontsize=16, fontweight='bold')

        metrics = [
            ('mean_selection_time', 'Selection Time (s)'),
            ('mean_total_time', 'Total Job Time (s)'),
            ('leader_entropy', 'Load Distribution\nEntropy')
        ]

        for idx, (metric, ylabel) in enumerate(metrics):
            ax = axes[idx]

            # Prepare data for each agent count
            data_to_plot = []
            labels = []

            for agents in sorted(df['agents'].unique()):
                df_agents = df[df['agents'] == agents]

                for topology in ['ring', 'mesh']:
                    df_subset = df_agents[df_agents['topology'] == topology]
                    if len(df_subset) > 0:
                        data_to_plot.append(df_subset[metric].values)
                        labels.append(f'{topology.capitalize()}\n{agents} agents')

            bp = ax.boxplot(data_to_plot, labels=labels, patch_artist=True)

            # Color by topology
            colors = []
            for label in labels:
                if 'Ring' in label:
                    colors.append('lightblue')
                else:
                    colors.append('lightcoral')

            for patch, color in zip(bp['boxes'], colors):
                patch.set_facecolor(color)

            ax.set_title(ylabel)
            ax.set_ylabel(ylabel.split('\n')[0])
            ax.tick_params(axis='x', rotation=45)
            ax.grid(True, alpha=0.3, axis='y')

        plt.tight_layout()
        plt.savefig(self.output_dir / 'topology_comparison.png', dpi=300, bbox_inches='tight')
        print(f"Saved: {self.output_dir / 'topology_comparison.png'}")
        plt.close()

    def plot_load_distribution(self):
        """Plot load distribution across agents for each configuration."""
        fig, axes = plt.subplots(2, 3, figsize=(20, 12))
        fig.suptitle('Job Assignment Distribution Across Agents', fontsize=16, fontweight='bold')

        axes = axes.flatten()

        for idx, (config_name, config_data) in enumerate(sorted(self.data.items())):
            if idx >= len(axes):
                break

            ax = axes[idx]

            # Aggregate job assignments across all runs
            all_assignments = []
            for run_data in config_data['runs']:
                all_assignments.extend(run_data['jobs']['leader_id'].tolist())

            # Count assignments per agent
            from collections import Counter
            assignment_counts = Counter(all_assignments)

            agents = sorted(assignment_counts.keys())
            counts = [assignment_counts[a] for a in agents]

            ax.bar(agents, counts, color='steelblue', alpha=0.7)
            ax.set_title(f"{config_data['topology'].capitalize()}: "
                        f"{config_data['agents']} agents, {config_data['jobs']} jobs")
            ax.set_xlabel('Agent ID')
            ax.set_ylabel('Jobs Assigned (Total across runs)')
            ax.grid(True, alpha=0.3, axis='y')

            # Add mean line
            mean_assignments = np.mean(counts)
            ax.axhline(mean_assignments, color='red', linestyle='--',
                      label=f'Mean: {mean_assignments:.1f}')
            ax.legend()

        plt.tight_layout()
        plt.savefig(self.output_dir / 'load_distribution.png', dpi=300, bbox_inches='tight')
        print(f"Saved: {self.output_dir / 'load_distribution.png'}")
        plt.close()

    def plot_success_rates(self, df: pd.DataFrame):
        """Plot job success rates across configurations."""
        fig, ax = plt.subplots(figsize=(12, 6))

        df_plot = df.copy()
        df_plot['config_label'] = (df_plot['topology'] + '-' +
                                   df_plot['agents'].astype(str) + '-' +
                                   df_plot['jobs'].astype(str))

        # Group by configuration
        success_stats = df_plot.groupby('config_label')['success_rate'].agg(['mean', 'std', 'count'])
        success_stats['ci'] = 1.96 * success_stats['std'] / np.sqrt(success_stats['count'])

        x_pos = np.arange(len(success_stats))

        ax.bar(x_pos, success_stats['mean'], yerr=success_stats['ci'],
               capsize=5, alpha=0.7, color='green')
        ax.set_xticks(x_pos)
        ax.set_xticklabels(success_stats.index, rotation=45, ha='right')
        ax.set_ylabel('Success Rate')
        ax.set_title('Job Success Rates Across Configurations (with 95% CI)')
        ax.set_ylim([0, 1.05])
        ax.grid(True, alpha=0.3, axis='y')
        ax.axhline(1.0, color='red', linestyle='--', alpha=0.5)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'success_rates.png', dpi=300, bbox_inches='tight')
        print(f"Saved: {self.output_dir / 'success_rates.png'}")
        plt.close()

    def generate_summary_statistics(self, df: pd.DataFrame):
        """Generate summary statistics table."""
        print("\n" + "="*80)
        print("SUMMARY STATISTICS")
        print("="*80)

        for config_name in sorted(df['config'].unique()):
            df_config = df[df['config'] == config_name]

            print(f"\n{config_name}:")
            print(f"  Runs: {len(df_config)}")
            print(f"  Mean Selection Time: {df_config['mean_selection_time'].mean():.4f} ± "
                  f"{df_config['mean_selection_time'].std():.4f} s")
            print(f"  Mean Total Time: {df_config['mean_total_time'].mean():.4f} ± "
                  f"{df_config['mean_total_time'].std():.4f} s")
            print(f"  P95 Selection Time: {df_config['p95_selection_time'].mean():.4f} s")
            print(f"  Success Rate: {df_config['success_rate'].mean():.2%}")
            print(f"  Load Entropy: {df_config['leader_entropy'].mean():.4f}")

        # Save detailed statistics to CSV
        summary_file = self.output_dir / 'summary_statistics.csv'
        summary_stats = df.groupby('config').agg({
            'mean_selection_time': ['mean', 'std', 'min', 'max'],
            'p95_selection_time': ['mean', 'std'],
            'mean_total_time': ['mean', 'std', 'min', 'max'],
            'success_rate': ['mean', 'std'],
            'leader_entropy': ['mean', 'std'],
            'completed_jobs': ['mean', 'sum'],
        })
        summary_stats.to_csv(summary_file)
        print(f"\nDetailed statistics saved to: {summary_file}")

        # Topology comparison
        print("\n" + "="*80)
        print("TOPOLOGY COMPARISON (averaged across all scales)")
        print("="*80)

        for topology in sorted(df['topology'].unique()):
            df_topo = df[df['topology'] == topology]
            print(f"\n{topology.upper()}:")
            print(f"  Mean Selection Time: {df_topo['mean_selection_time'].mean():.4f} s")
            print(f"  Mean Total Time: {df_topo['mean_total_time'].mean():.4f} s")
            print(f"  Load Entropy: {df_topo['leader_entropy'].mean():.4f}")

    def run_analysis(self):
        """Run complete analysis pipeline."""
        print("="*80)
        print("Multi-Run Analysis Pipeline")
        print("="*80)

        # Load data
        self.load_all_runs()

        if not self.data:
            print("No data found! Check the base directory path.")
            return

        # Aggregate metrics
        print("\nAggregating metrics...")
        df_metrics = self.aggregate_metrics()

        # Save aggregated metrics
        metrics_file = self.output_dir / 'aggregated_metrics.csv'
        df_metrics.to_csv(metrics_file, index=False)
        print(f"Aggregated metrics saved to: {metrics_file}")

        # Generate plots
        print("\nGenerating plots...")
        self.plot_latency_comparison(df_metrics)
        self.plot_scaling_analysis(df_metrics)
        self.plot_topology_comparison(df_metrics)
        self.plot_load_distribution()
        self.plot_success_rates(df_metrics)

        # Generate summary statistics
        self.generate_summary_statistics(df_metrics)

        print("\n" + "="*80)
        print(f"Analysis complete! All outputs saved to: {self.output_dir}")
        print("="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Analyze and plot results from multiple SwarmAgents runs'
    )
    parser.add_argument(
        '--base-dir',
        type=str,
        default='single-site',
        help='Base directory containing run-* subdirectories (default: single-site)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help='Output directory for plots and statistics (default: base-dir/plots)'
    )

    args = parser.parse_args()

    analyzer = MultiRunAnalyzer(args.base_dir, args.output_dir)
    analyzer.run_analysis()


if __name__ == '__main__':
    main()
