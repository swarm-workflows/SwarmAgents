#!/usr/bin/env python3
"""
Generate publication-quality plots for SWARM V2 paper evaluation section.
Focuses on the three key contributions: Scalability, Resilience, Data-awareness.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import argparse

# Publication-quality settings
plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['font.size'] = 11
plt.rcParams['font.family'] = 'serif'
plt.rcParams['axes.labelsize'] = 12
plt.rcParams['axes.titlesize'] = 13
plt.rcParams['xtick.labelsize'] = 10
plt.rcParams['ytick.labelsize'] = 10
plt.rcParams['legend.fontsize'] = 10
plt.rcParams['figure.titlesize'] = 14
sns.set_style("whitegrid")
sns.set_palette("colorblind")


class PaperEvaluationPlots:
    """Generate plots specifically for paper evaluation section."""

    def __init__(self, single_site_dir: str, multi_site_dir: str, output_dir: str):
        self.single_site_dir = Path(single_site_dir)
        self.multi_site_dir = Path(multi_site_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Load aggregated data
        self.df_single = pd.read_csv(self.single_site_dir / "plots/aggregated_metrics.csv")
        self.df_multi = pd.read_csv(self.multi_site_dir / "plots/aggregated_metrics.csv")

    def plot_baseline_comparison(self):
        """Plot improvement over SWARM V1 baseline (40s -> 0.34s, 325s -> 3.7s)."""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

        # Data from abstract
        swarm_v1 = {'selection': 40.0, 'scheduling': 325.0}

        # Get SWARM V2 best performance (hierarchical-110-1000 from single-site)
        df_hier = self.df_single[self.df_single['config'] == 'run-hierarchical-110-1000']
        if len(df_hier) > 0:
            swarm_v2_selection = df_hier['mean_selection_time'].mean()
            swarm_v2_scheduling = df_hier['mean_total_time'].mean()
        else:
            # Use best available config
            swarm_v2_selection = self.df_single['mean_selection_time'].min()
            swarm_v2_scheduling = self.df_single['mean_total_time'].min()

        # Selection time comparison
        systems = ['CCGrid\'25', 'SWARM-Enhanced']
        selection_times = [swarm_v1['selection'], swarm_v2_selection]
        colors = ['#d62728', '#2ca02c']

        bars1 = ax1.bar(systems, selection_times, color=colors, alpha=0.7, edgecolor='black')
        ax1.set_ylabel('Mean Selection Time (seconds)')
        ax1.set_title('Job Selection Time Comparison')
        ax1.set_ylim([0, max(selection_times) * 1.1])

        # Add improvement percentage
        improvement_sel = ((swarm_v1['selection'] - swarm_v2_selection) / swarm_v1['selection']) * 100
        ax1.text(0.5, max(selection_times) * 0.95, f'{improvement_sel:.1f}% reduction',
                ha='center', fontweight='bold', fontsize=12)

        # Add value labels on bars
        for bar, val in zip(bars1, selection_times):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{val:.2f}s', ha='center', va='bottom', fontweight='bold')

        # Scheduling latency comparison
        scheduling_times = [swarm_v1['scheduling'], swarm_v2_scheduling]
        bars2 = ax2.bar(systems, scheduling_times, color=colors, alpha=0.7, edgecolor='black')
        ax2.set_ylabel('Mean Job Assignment Latency (seconds)')
        ax2.set_title('End-to-End Job Assignment Comparison')
        ax2.set_ylim([0, max(scheduling_times) * 1.1])

        improvement_sched = ((swarm_v1['scheduling'] - swarm_v2_scheduling) / swarm_v1['scheduling']) * 100
        ax2.text(0.5, max(scheduling_times) * 0.95, f'{improvement_sched:.1f}% reduction',
                ha='center', fontweight='bold', fontsize=12)

        for bar, val in zip(bars2, scheduling_times):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{val:.1f}s', ha='center', va='bottom', fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.output_dir / 'baseline_comparison.pdf', bbox_inches='tight')
        plt.savefig(self.output_dir / 'baseline_comparison.png', bbox_inches='tight')
        print(f"Saved: {self.output_dir / 'baseline_comparison.pdf'}")
        plt.close()

    def plot_scaling_efficiency(self):
        """Plot scalability showing hierarchical O(log n) advantage."""
        fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))

        metrics = [
            ('mean_selection_time', 'Selection Time (s)', 'log'),
            ('mean_total_time', 'Total Job Time (s)', 'linear'),
            ('leader_entropy', 'Load Distribution Entropy', 'linear')
        ]

        for ax, (metric, ylabel, scale) in zip(axes, metrics):
            for topology in sorted(self.df_single['topology'].unique()):
                df_topo = self.df_single[self.df_single['topology'] == topology]
                grouped = df_topo.groupby('agents')[metric].agg(['mean', 'std', 'count'])

                ci = 1.96 * grouped['std'] / np.sqrt(grouped['count'])
                ci = ci.fillna(0)

                marker = 'o' if topology == 'hierarchical' else ('s' if topology == 'mesh' else '^')
                ax.errorbar(grouped.index, grouped['mean'], yerr=ci,
                           marker=marker, label=topology.capitalize(),
                           capsize=4, capthick=1.5, linewidth=2, markersize=8)

            ax.set_xlabel('Number of Agents')
            ax.set_ylabel(ylabel)
            ax.set_yscale(scale)
            ax.legend(loc='best')
            ax.grid(True, alpha=0.3, which='both')

            # Add O(log n) reference line for selection time
            if metric == 'mean_selection_time':
                agents = np.array(sorted(self.df_single['agents'].unique()))
                # Fit to hierarchical data
                df_hier = self.df_single[self.df_single['topology'] == 'hierarchical']
                if len(df_hier) > 0:
                    hier_data = df_hier.groupby('agents')[metric].mean()
                    # Scale O(log n) to match data
                    if len(hier_data) > 0:
                        scale_factor = hier_data.values[0] / np.log2(hier_data.index[0])
                        log_n_line = scale_factor * np.log2(agents)
                        ax.plot(agents, log_n_line, '--', color='gray',
                               label='O(log n) reference', linewidth=1.5, alpha=0.7)
                        ax.legend(loc='best')

        plt.tight_layout()
        plt.savefig(self.output_dir / 'scaling_efficiency.pdf', bbox_inches='tight')
        plt.savefig(self.output_dir / 'scaling_efficiency.png', bbox_inches='tight')
        print(f"Saved: {self.output_dir / 'scaling_efficiency.pdf'}")
        plt.close()

    def plot_latency_cdf(self):
        """Plot CDF comparing single-site vs multi-site for same configurations."""
        # Find common configurations between single-site and multi-site
        single_configs = set(self.df_single['config'].unique())
        multi_configs = set(self.df_multi['config'].unique())
        common_configs = sorted(list(single_configs & multi_configs))

        if not common_configs:
            print("  Warning: No common configurations between single-site and multi-site")
            return

        # Create subplots - one per common configuration
        n_configs = len(common_configs)
        fig, axes = plt.subplots(1, min(n_configs, 3), figsize=(6*min(n_configs, 3), 5))
        if n_configs == 1:
            axes = [axes]

        for idx, config in enumerate(common_configs[:3]):  # Limit to 3 panels
            ax = axes[idx] if n_configs > 1 else axes[0]

            # Single-site data
            df_single_config = self.df_single[self.df_single['config'] == config]
            if len(df_single_config) > 0:
                values = df_single_config['mean_selection_time'].values
                values_sorted = np.sort(values)
                cdf = np.arange(1, len(values_sorted) + 1) / len(values_sorted)
                ax.plot(values_sorted, cdf, linewidth=2.5, label='Single-Site',
                       color='#2ca02c', linestyle='-')

            # Multi-site data
            df_multi_config = self.df_multi[self.df_multi['config'] == config]
            if len(df_multi_config) > 0:
                values = df_multi_config['mean_selection_time'].values
                values_sorted = np.sort(values)
                cdf = np.arange(1, len(values_sorted) + 1) / len(values_sorted)
                ax.plot(values_sorted, cdf, linewidth=2.5, label='Multi-Site',
                       color='#d62728', linestyle='--')

            # Calculate median slowdown
            if len(df_single_config) > 0 and len(df_multi_config) > 0:
                single_median = df_single_config['mean_selection_time'].median()
                multi_median = df_multi_config['mean_selection_time'].median()
                slowdown = multi_median / single_median if single_median > 0 else 0

                # Add annotation
                ax.text(0.98, 0.05, f'{slowdown:.2f}× slowdown',
                       transform=ax.transAxes, ha='right', va='bottom',
                       bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
                       fontsize=9)

            config_label = config.replace('run-', '').replace('-', ' ').title()
            ax.set_xlabel('Selection Time (seconds)', fontweight='bold')
            ax.set_ylabel('CDF', fontweight='bold')
            ax.set_title(f'{config_label}', fontsize=12, fontweight='bold')
            ax.legend(loc='lower right', fontsize=10)
            ax.grid(True, alpha=0.3)
            ax.set_xlim(left=0)

        plt.suptitle('Single-Site vs Multi-Site: Selection Time CDF Comparison',
                    fontsize=14, fontweight='bold', y=0.98)
        plt.tight_layout()
        plt.savefig(self.output_dir / 'latency_cdf.pdf', bbox_inches='tight')
        plt.savefig(self.output_dir / 'latency_cdf.png', bbox_inches='tight', dpi=300)
        print(f"Saved: {self.output_dir / 'latency_cdf.pdf'}")
        plt.close()

    def plot_topology_efficiency(self):
        """Direct topology comparison with key metrics."""
        fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))

        # Prepare data
        topology_metrics = self.df_single.groupby('topology').agg({
            'mean_selection_time': ['mean', 'std'],
            'mean_total_time': ['mean', 'std'],
            'leader_entropy': ['mean', 'std']
        })

        topologies = sorted(self.df_single['topology'].unique())
        colors = {'ring': '#1f77b4', 'mesh': '#ff7f0e', 'hierarchical': '#2ca02c'}

        # Selection time
        ax = axes[0]
        means = [topology_metrics.loc[t, ('mean_selection_time', 'mean')] for t in topologies]
        stds = [topology_metrics.loc[t, ('mean_selection_time', 'std')] for t in topologies]
        bars = ax.bar(range(len(topologies)), means, yerr=stds,
                     color=[colors[t] for t in topologies], alpha=0.7, capsize=5, edgecolor='black')
        ax.set_xticks(range(len(topologies)))
        ax.set_xticklabels([t.capitalize() for t in topologies])
        ax.set_ylabel('Mean Selection Time (s)')
        ax.set_title('Selection Time by Topology')
        ax.grid(True, alpha=0.3, axis='y')

        # Add value labels
        for bar, val in zip(bars, means):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{val:.2f}s', ha='center', va='bottom', fontsize=9)

        # Total time
        ax = axes[1]
        means = [topology_metrics.loc[t, ('mean_total_time', 'mean')] for t in topologies]
        stds = [topology_metrics.loc[t, ('mean_total_time', 'std')] for t in topologies]
        bars = ax.bar(range(len(topologies)), means, yerr=stds,
                     color=[colors[t] for t in topologies], alpha=0.7, capsize=5, edgecolor='black')
        ax.set_xticks(range(len(topologies)))
        ax.set_xticklabels([t.capitalize() for t in topologies])
        ax.set_ylabel('Mean Total Time (s)')
        ax.set_title('End-to-End Job Time by Topology')
        ax.grid(True, alpha=0.3, axis='y')

        for bar, val in zip(bars, means):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{val:.1f}s', ha='center', va='bottom', fontsize=9)

        # Load entropy
        ax = axes[2]
        means = [topology_metrics.loc[t, ('leader_entropy', 'mean')] for t in topologies]
        stds = [topology_metrics.loc[t, ('leader_entropy', 'std')] for t in topologies]
        bars = ax.bar(range(len(topologies)), means, yerr=stds,
                     color=[colors[t] for t in topologies], alpha=0.7, capsize=5, edgecolor='black')
        ax.set_xticks(range(len(topologies)))
        ax.set_xticklabels([t.capitalize() for t in topologies])
        ax.set_ylabel('Load Distribution Entropy')
        ax.set_title('Load Balancing Quality')
        ax.grid(True, alpha=0.3, axis='y')

        for bar, val in zip(bars, means):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{val:.2f}', ha='center', va='bottom', fontsize=9)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'topology_efficiency.pdf', bbox_inches='tight')
        plt.savefig(self.output_dir / 'topology_efficiency.png', bbox_inches='tight')
        print(f"Saved: {self.output_dir / 'topology_efficiency.pdf'}")
        plt.close()

    def plot_multi_site_overhead(self):
        """Compare single-site vs multi-site overhead."""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

        # Get matching configurations (30 agents, 500 jobs)
        configs = ['ring-30-500', 'mesh-30-500', 'hierarchical-30-500', 'hierarchical-110-1000']

        data_comparison = []
        for config in configs:
            single = self.df_single[self.df_single['config'] == f'run-{config}']
            multi = self.df_multi[self.df_multi['config'] == f'run-{config}']

            if len(single) > 0 and len(multi) > 0:
                data_comparison.append({
                    'config': config,
                    'single_selection': single['mean_selection_time'].mean(),
                    'multi_selection': multi['mean_selection_time'].mean(),
                    'single_total': single['mean_total_time'].mean(),
                    'multi_total': multi['mean_total_time'].mean(),
                })

        if data_comparison:
            df_comp = pd.DataFrame(data_comparison)

            # Selection time comparison
            x = np.arange(len(df_comp))
            width = 0.35

            bars1 = ax1.bar(x - width/2, df_comp['single_selection'], width,
                          label='Single-Site', color='steelblue', alpha=0.7, edgecolor='black')
            bars2 = ax1.bar(x + width/2, df_comp['multi_selection'], width,
                          label='Multi-Site', color='coral', alpha=0.7, edgecolor='black')

            ax1.set_ylabel('Mean Selection Time (s)')
            ax1.set_title('Selection Time: Single-Site vs Multi-Site')
            ax1.set_xticks(x)
            ax1.set_xticklabels([c.replace('-', ' ').title() for c in df_comp['config']])
            ax1.legend()
            ax1.grid(True, alpha=0.3, axis='y')

            # Add overhead percentage
            for i, row in df_comp.iterrows():
                overhead = ((row['multi_selection'] - row['single_selection']) / row['single_selection']) * 100
                ax1.text(i, max(row['single_selection'], row['multi_selection']) * 1.05,
                        f'+{overhead:.1f}%', ha='center', fontsize=9, fontweight='bold')

            # Total time comparison
            bars3 = ax2.bar(x - width/2, df_comp['single_total'], width,
                          label='Single-Site', color='steelblue', alpha=0.7, edgecolor='black')
            bars4 = ax2.bar(x + width/2, df_comp['multi_total'], width,
                          label='Multi-Site', color='coral', alpha=0.7, edgecolor='black')

            ax2.set_ylabel('Mean Total Time (s)')
            ax2.set_title('End-to-End Time: Single-Site vs Multi-Site')
            ax2.set_xticks(x)
            ax2.set_xticklabels([c.replace('-', ' ').title() for c in df_comp['config']])
            ax2.legend()
            ax2.grid(True, alpha=0.3, axis='y')

            for i, row in df_comp.iterrows():
                overhead = ((row['multi_total'] - row['single_total']) / row['single_total']) * 100
                ax2.text(i, max(row['single_total'], row['multi_total']) * 1.05,
                        f'+{overhead:.1f}%', ha='center', fontsize=9, fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.output_dir / 'multi_site_overhead.pdf', bbox_inches='tight')
        plt.savefig(self.output_dir / 'multi_site_overhead.png', bbox_inches='tight')
        print(f"Saved: {self.output_dir / 'multi_site_overhead.pdf'}")
        plt.close()

    def plot_performance_summary_table(self):
        """Generate a summary table figure with key metrics."""
        # Prepare summary data
        summary_data = []

        for config in ['run-hierarchical-110-1000', 'run-mesh-30-500', 'run-ring-30-500']:
            df_config = self.df_single[self.df_single['config'] == config]
            if len(df_config) > 0:
                topology, agents, jobs = config.replace('run-', '').split('-')
                summary_data.append({
                    'Configuration': f'{topology.capitalize()}\n{agents} agents',
                    'Selection\nTime (s)': f"{df_config['mean_selection_time'].mean():.2f} ± {df_config['mean_selection_time'].std():.2f}",
                    'P95 Selection\nTime (s)': f"{df_config['p95_selection_time'].mean():.2f}",
                    'Total Job\nTime (s)': f"{df_config['mean_total_time'].mean():.1f} ± {df_config['mean_total_time'].std():.1f}",
                    'Success\nRate': f"{df_config['success_rate'].mean():.1%}",
                    'Load\nEntropy': f"{df_config['leader_entropy'].mean():.2f}"
                })

        df_summary = pd.DataFrame(summary_data)

        # Create figure with table
        fig, ax = plt.subplots(figsize=(12, 3))
        ax.axis('tight')
        ax.axis('off')

        table = ax.table(cellText=df_summary.values,
                        colLabels=df_summary.columns,
                        cellLoc='center',
                        loc='center',
                        bbox=[0, 0, 1, 1])

        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 2)

        # Style header
        for i in range(len(df_summary.columns)):
            table[(0, i)].set_facecolor('#4472C4')
            table[(0, i)].set_text_props(weight='bold', color='white')

        # Alternate row colors
        for i in range(1, len(df_summary) + 1):
            for j in range(len(df_summary.columns)):
                if i % 2 == 0:
                    table[(i, j)].set_facecolor('#E7E6E6')
                else:
                    table[(i, j)].set_facecolor('white')

        plt.title('Performance Summary: Key Configurations', fontsize=14, fontweight='bold', pad=20)
        plt.savefig(self.output_dir / 'performance_summary.pdf', bbox_inches='tight')
        plt.savefig(self.output_dir / 'performance_summary.png', bbox_inches='tight')
        print(f"Saved: {self.output_dir / 'performance_summary.pdf'}")
        plt.close()

    def generate_all_plots(self):
        """Generate all evaluation plots for the paper."""
        print("="*80)
        print("Generating Publication-Quality Evaluation Plots")
        print("="*80)

        print("\n1. Baseline comparison (SWARM V1 vs V2)...")
        self.plot_baseline_comparison()

        print("\n2. Scaling efficiency...")
        self.plot_scaling_efficiency()

        print("\n3. Latency CDF...")
        self.plot_latency_cdf()

        print("\n4. Topology efficiency...")
        self.plot_topology_efficiency()

        print("\n5. Multi-site overhead...")
        self.plot_multi_site_overhead()

        print("\n6. Performance summary table...")
        self.plot_performance_summary_table()

        print("\n" + "="*80)
        print(f"All plots saved to: {self.output_dir}")
        print("Formats: PDF (for paper) and PNG (for preview)")
        print("="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Generate publication-quality plots for SWARM V2 paper'
    )
    parser.add_argument(
        '--single-site',
        type=str,
        default='runs/single-site',
        help='Single-site results directory'
    )
    parser.add_argument(
        '--multi-site',
        type=str,
        default='runs/multi-site',
        help='Multi-site results directory'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='paper_plots',
        help='Output directory for plots'
    )

    args = parser.parse_args()

    plotter = PaperEvaluationPlots(args.single_site, args.multi_site, args.output)
    plotter.generate_all_plots()


if __name__ == '__main__':
    main()
