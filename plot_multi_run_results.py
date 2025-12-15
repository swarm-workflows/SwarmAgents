#!/usr/bin/env python3
"""
Comprehensive plotting script for analyzing multiple runs across different configurations.
Supports comparison of topologies (ring, mesh, hierarchical) and scales.
FIXED VERSION: Handles cases where started_at timestamps are missing/zero.
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
import warnings

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

            # Add latency data if available (either from separate file or from all_jobs.csv)
            if latency_file.exists():
                df_latency = pd.read_csv(latency_file)
                run_data['latency'] = df_latency
            elif 'scheduling_latency' in df_jobs.columns:
                # Multi-site data has scheduling_latency in all_jobs.csv
                run_data['latency'] = df_jobs[['job_id', 'leader_id', 'scheduling_latency', 'reasoning_time']].copy()

            # Calculate derived metrics
            df_jobs['selection_time'] = df_jobs['assigned_at'] - df_jobs['selection_started_at']

            # FIX: Handle missing started_at timestamps
            # Use assigned_at as a proxy for started_at when it's 0
            df_jobs['effective_started_at'] = np.where(
                df_jobs['started_at'] == 0,
                df_jobs['assigned_at'],
                df_jobs['started_at']
            )

            df_jobs['wait_time'] = df_jobs['effective_started_at'] - df_jobs['submitted_at']
            df_jobs['execution_time'] = df_jobs['completed_at'] - df_jobs['effective_started_at']
            df_jobs['total_time'] = df_jobs['completed_at'] - df_jobs['submitted_at']

            # Handle zero completed_at timestamps (not completed)
            df_jobs.loc[df_jobs['completed_at'] == 0, 'execution_time'] = np.nan
            df_jobs.loc[df_jobs['completed_at'] == 0, 'total_time'] = np.nan

            return run_data

        except Exception as e:
            print(f"    Warning: Could not load {run_dir}: {e}")
            return None

    def get_hierarchy_level(self, agent_id: int, topology: str, total_agents: int) -> int:
        """Determine hierarchy level for an agent in hierarchical topology.

        Returns:
            0 for Level 0 (worker) agents
            1 for Level 1 (coordinator) agents
            -1 for non-hierarchical topologies
        """
        if topology != 'hierarchical':
            return -1

        # For hierarchical topologies, coordinators are typically the highest numbered agents
        # This logic needs to match your actual agent ID assignment scheme
        # Adjust based on your specific hierarchical configuration

        # Common patterns:
        # - Hier-30: 5 groups of 5 workers (0-24) + 1 group of 5 coordinators (25-29)
        # - Hier-110: 10 groups of 10 workers (0-99) + 1 group of 10 coordinators (100-109)
        # - Hier-250: 25 groups of 9 workers (0-224) + 1 group of 25 coordinators (225-249)
        # - Hier-990: Complex 3-level structure

        if total_agents == 30:
            # Hier-30: agents 0-24 are Level 0, 25-29 are Level 1
            return 1 if agent_id >= 25 else 0
        elif total_agents == 110:
            # Hier-110: agents 0-99 are Level 0, 100-109 are Level 1
            return 1 if agent_id >= 100 else 0
        elif total_agents == 250:
            # Hier-250: agents 0-224 are Level 0, 225-249 are Level 1
            return 1 if agent_id >= 225 else 0
        elif total_agents == 990:
            # Hier-990: 3-level structure, treat Level 1 and Level 2 coordinators separately
            # 880 Level 0 workers (88 groups × 10) + 88 Level 1 coordinators + 22 Level 2 coordinators
            if agent_id < 880:
                return 0  # Level 0 workers
            elif agent_id < 968:
                return 1  # Level 1 coordinators
            else:
                return 2  # Level 2 coordinators
        else:
            # Default heuristic: top 10% are coordinators
            coordinator_threshold = int(total_agents * 0.9)
            return 1 if agent_id >= coordinator_threshold else 0

    def aggregate_metrics(self) -> pd.DataFrame:
        """Aggregate metrics across all runs and configurations."""
        records = []

        for config_name, config_data in self.data.items():
            topology = config_data['topology']
            agents = config_data['agents']
            jobs = config_data['jobs']

            for run_data in config_data['runs']:
                df_jobs = run_data['jobs']

                # For hierarchical topologies, add hierarchy level column
                if topology == 'hierarchical':
                    df_jobs['hierarchy_level'] = df_jobs['leader_id'].apply(
                        lambda x: self.get_hierarchy_level(x, topology, agents)
                    )

                # Calculate aggregate metrics for this run (with robust handling of NaN)
                record = {
                    'config': config_name,
                    'topology': topology,
                    'agents': agents,
                    'jobs': jobs,
                    'run': run_data['run_dir'],

                    # Latency metrics (all jobs)
                    'mean_selection_time': self.safe_mean(df_jobs['selection_time']),
                    'median_selection_time': self.safe_median(df_jobs['selection_time']),
                    'p95_selection_time': self.safe_quantile(df_jobs['selection_time'], 0.95),
                    'p99_selection_time': self.safe_quantile(df_jobs['selection_time'], 0.99),

                    # Wait time metrics
                    'mean_wait_time': self.safe_mean(df_jobs['wait_time']),
                    'median_wait_time': self.safe_median(df_jobs['wait_time']),

                    # Execution metrics
                    'mean_execution_time': self.safe_mean(df_jobs['execution_time']),
                    'total_execution_time': self.safe_sum(df_jobs['execution_time']),

                    # Total time metrics
                    'mean_total_time': self.safe_mean(df_jobs['total_time']),
                    'median_total_time': self.safe_median(df_jobs['total_time']),

                    # Job completion
                    'completed_jobs': (df_jobs['exit_status'] == 0).sum(),
                    'failed_jobs': (df_jobs['exit_status'] != 0).sum(),
                    'success_rate': (df_jobs['exit_status'] == 0).mean(),

                    # Load balancing
                    'unique_leaders': df_jobs['leader_id'].nunique(),
                    'leader_entropy': self.calculate_entropy(df_jobs['leader_id']),
                }

                # Add per-level metrics for hierarchical topologies
                if topology == 'hierarchical':
                    for level in sorted(df_jobs['hierarchy_level'].unique()):
                        df_level = df_jobs[df_jobs['hierarchy_level'] == level]

                        record[f'level{level}_mean_selection_time'] = self.safe_mean(df_level['selection_time'])
                        record[f'level{level}_median_selection_time'] = self.safe_median(df_level['selection_time'])
                        record[f'level{level}_p95_selection_time'] = self.safe_quantile(df_level['selection_time'], 0.95)
                        record[f'level{level}_job_count'] = len(df_level)
                        record[f'level{level}_agent_count'] = df_level['leader_id'].nunique()

                # Add scheduling latency if available
                if 'latency' in run_data:
                    df_latency = run_data['latency']
                    record['mean_scheduling_latency'] = self.safe_mean(df_latency['scheduling_latency'])
                    record['median_scheduling_latency'] = self.safe_median(df_latency['scheduling_latency'])
                    record['mean_reasoning_time'] = self.safe_mean(df_latency['reasoning_time'])

                    # Per-level scheduling latency for hierarchical topologies
                    if topology == 'hierarchical' and 'leader_id' in df_latency.columns:
                        df_latency['hierarchy_level'] = df_latency['leader_id'].apply(
                            lambda x: self.get_hierarchy_level(x, topology, agents)
                        )
                        for level in sorted(df_latency['hierarchy_level'].unique()):
                            df_level_lat = df_latency[df_latency['hierarchy_level'] == level]
                            record[f'level{level}_mean_scheduling_latency'] = self.safe_mean(df_level_lat['scheduling_latency'])
                            record[f'level{level}_median_scheduling_latency'] = self.safe_median(df_level_lat['scheduling_latency'])

                records.append(record)

        return pd.DataFrame(records)

    # Helper methods for safe statistics calculation
    def safe_mean(self, series):
        """Calculate mean, returning 0 if all values are NaN."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = series.mean()
            return result if not pd.isna(result) else 0.0

    def safe_median(self, series):
        """Calculate median, returning 0 if all values are NaN."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = series.median()
            return result if not pd.isna(result) else 0.0

    def safe_quantile(self, series, q):
        """Calculate quantile, returning 0 if all values are NaN."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = series.quantile(q)
            return result if not pd.isna(result) else 0.0

    def safe_sum(self, series):
        """Calculate sum, returning 0 if all values are NaN."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = series.sum()
            return result if not pd.isna(result) else 0.0

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
        sns.boxplot(data=df_plot, x='config_label', y='mean_selection_time', hue='topology', ax=ax, legend=False)
        ax.set_title('Mean Selection Time')
        ax.set_xlabel('Configuration')
        ax.set_ylabel('Time (seconds)')
        ax.tick_params(axis='x', rotation=45)

        # P95 Selection Time
        ax = axes[0, 1]
        sns.boxplot(data=df_plot, x='config_label', y='p95_selection_time', hue='topology', ax=ax, legend=False)
        ax.set_title('P95 Selection Time')
        ax.set_xlabel('Configuration')
        ax.set_ylabel('Time (seconds)')
        ax.tick_params(axis='x', rotation=45)

        # Mean Total Time
        ax = axes[1, 0]
        sns.boxplot(data=df_plot, x='config_label', y='mean_total_time', hue='topology', ax=ax, legend=False)
        ax.set_title('Mean Total Job Time (Submission to Completion)')
        ax.set_xlabel('Configuration')
        ax.set_ylabel('Time (seconds)')
        ax.tick_params(axis='x', rotation=45)

        # Scheduling Latency (if available)
        ax = axes[1, 1]
        if 'mean_scheduling_latency' in df_plot.columns and df_plot['mean_scheduling_latency'].sum() > 0:
            sns.boxplot(data=df_plot, x='config_label', y='mean_scheduling_latency', hue='topology', ax=ax, legend=False)
            ax.set_title('Mean Scheduling Latency')
            ax.set_ylabel('Time (seconds)')
        else:
            ax.text(0.5, 0.5, 'Scheduling latency data not available',
                   ha='center', va='center', transform=ax.transAxes)
            ax.set_ylabel('Time (seconds)')
        ax.set_xlabel('Configuration')
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

            for topology in sorted(df['topology'].unique()):
                df_topo = df[df['topology'] == topology]

                # Group by agent count and calculate mean and std
                grouped = df_topo.groupby('agents')[metric].agg(['mean', 'std', 'count'])

                # Calculate 95% confidence interval
                ci = 1.96 * grouped['std'] / np.sqrt(grouped['count'])
                ci = ci.fillna(0)  # Handle NaN in CI

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
        """Direct comparison between different topologies."""
        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        fig.suptitle('Topology Comparison', fontsize=16, fontweight='bold')

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

                for topology in sorted(df['topology'].unique()):
                    df_subset = df_agents[df_agents['topology'] == topology]
                    if len(df_subset) > 0:
                        data_to_plot.append(df_subset[metric].values)
                        labels.append(f'{topology.capitalize()}\n{agents} agents')

            if data_to_plot:
                bp = ax.boxplot(data_to_plot, labels=labels, patch_artist=True)

                # Color by topology
                topology_colors = {
                    'ring': 'lightblue',
                    'mesh': 'lightcoral',
                    'hierarchical': 'lightgreen'
                }

                colors = []
                for label in labels:
                    for topo, color in topology_colors.items():
                        if topo.capitalize() in label:
                            colors.append(color)
                            break

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
        num_configs = len(self.data)
        num_cols = 3
        num_rows = (num_configs + num_cols - 1) // num_cols

        fig, axes = plt.subplots(num_rows, num_cols, figsize=(20, 6 * num_rows))
        fig.suptitle('Job Assignment Distribution Across Agents', fontsize=16, fontweight='bold')

        if num_rows == 1:
            axes = axes.reshape(1, -1)

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

            if assignment_counts:
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

        # Hide extra subplots
        for idx in range(len(self.data), len(axes)):
            axes[idx].set_visible(False)

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
        success_stats['ci'] = success_stats['ci'].fillna(0)

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
            topology = df_config['topology'].iloc[0]

            print(f"\n{config_name}:")
            print(f"  Runs: {len(df_config)}")
            print(f"  Mean Selection Time: {df_config['mean_selection_time'].mean():.4f} ± "
                  f"{df_config['mean_selection_time'].std():.4f} s")
            print(f"  Mean Total Time: {df_config['mean_total_time'].mean():.4f} ± "
                  f"{df_config['mean_total_time'].std():.4f} s")
            print(f"  P95 Selection Time: {df_config['p95_selection_time'].mean():.4f} s")

            # Print per-level metrics for hierarchical topologies
            if topology == 'hierarchical':
                # Detect available levels from column names
                level_cols = [col for col in df_config.columns if col.startswith('level') and 'mean_selection_time' in col]
                levels = sorted(set(int(col.replace('level', '').replace('_mean_selection_time', '')) for col in level_cols))

                print(f"  Per-Level Metrics:")
                for level in levels:
                    mean_col = f'level{level}_mean_selection_time'
                    median_col = f'level{level}_median_selection_time'
                    p95_col = f'level{level}_p95_selection_time'
                    job_count_col = f'level{level}_job_count'
                    agent_count_col = f'level{level}_agent_count'

                    if mean_col in df_config.columns and not df_config[mean_col].isna().all():
                        print(f"    Level {level}:")
                        print(f"      Mean Selection Time: {df_config[mean_col].mean():.4f} ± {df_config[mean_col].std():.4f} s")
                        if median_col in df_config.columns:
                            print(f"      Median Selection Time: {df_config[median_col].mean():.4f} s")
                        if p95_col in df_config.columns:
                            print(f"      P95 Selection Time: {df_config[p95_col].mean():.4f} s")
                        if job_count_col in df_config.columns:
                            print(f"      Jobs Assigned: {df_config[job_count_col].mean():.0f}")
                        if agent_count_col in df_config.columns:
                            print(f"      Active Agents: {df_config[agent_count_col].mean():.0f}")

                        # Per-level scheduling latency if available
                        sched_lat_col = f'level{level}_mean_scheduling_latency'
                        if sched_lat_col in df_config.columns and not df_config[sched_lat_col].isna().all():
                            print(f"      Mean Scheduling Latency: {df_config[sched_lat_col].mean():.4f} ± "
                                  f"{df_config[sched_lat_col].std():.4f} s")

            # Print scheduling latency if available
            if 'mean_scheduling_latency' in df_config.columns and not df_config['mean_scheduling_latency'].isna().all():
                print(f"  Mean Scheduling Latency: {df_config['mean_scheduling_latency'].mean():.4f} ± "
                      f"{df_config['mean_scheduling_latency'].std():.4f} s")

            print(f"  Success Rate: {df_config['success_rate'].mean():.2%}")
            print(f"  Load Entropy: {df_config['leader_entropy'].mean():.4f}")

        # Save detailed statistics to CSV
        summary_file = self.output_dir / 'summary_statistics.csv'
        agg_dict = {
            'mean_selection_time': ['mean', 'std', 'min', 'max'],
            'p95_selection_time': ['mean', 'std'],
            'mean_total_time': ['mean', 'std', 'min', 'max'],
            'success_rate': ['mean', 'std'],
            'leader_entropy': ['mean', 'std'],
            'completed_jobs': ['mean', 'sum'],
        }

        # Add scheduling latency if available
        if 'mean_scheduling_latency' in df.columns:
            agg_dict['mean_scheduling_latency'] = ['mean', 'std', 'min', 'max']
            agg_dict['median_scheduling_latency'] = ['mean', 'std']

        summary_stats = df.groupby('config').agg(agg_dict)
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
        print("Multi-Run Analysis Pipeline (FIXED VERSION)")
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
        description='Analyze and plot results from multiple SwarmAgents runs (FIXED VERSION)'
    )
    parser.add_argument(
        '--base-dir',
        type=str,
        default='runs/single-site',
        help='Base directory containing run-* subdirectories (default: runs/single-site)'
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
