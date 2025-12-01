#!/usr/bin/env python3
"""
Generate publication-quality CCGrid vs V2 comparison plot using actual data.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

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

def plot_ccgrid_comparison_detailed():
    """Create detailed CCGrid vs V2 comparison with actual data for both single-site and multi-site."""

    # Load comparison data for both scenarios
    single_file = Path("runs/single-site/comparison_ccgrid_vs_v2/comparison_summary.csv")
    multi_file = Path("runs/multi-site/comparison_ccgrid_vs_v2/comparison_summary.csv")

    df_single = pd.read_csv(single_file)
    df_multi = pd.read_csv(multi_file)

    # Use only metrics available in both datasets
    common_metrics = set(df_single['Metric']) & set(df_multi['Metric'])

    # Create figure with 2x2 subplots
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))

    # Use key metrics: selection time and scheduling latency
    metrics = [
        ('Mean Selection Time (s)', 0, 0),
        ('P95 Selection Time (s)', 0, 1),
        ('Mean Scheduling Latency (s)', 1, 0),
        ('Mean Wait Time (s)', 1, 1)
    ]

    # Updated colors for 4 bars: CCGrid single, V2 single, CCGrid, V2 multi
    colors = ['#d62728', '#2ca02c', '#ff7f0e', '#1f77b4']

    for metric_name, row, col in metrics:
        ax = axes[row, col]

        # Get data for this metric from both scenarios
        try:
            single_data = df_single[df_single['Metric'] == metric_name].iloc[0]
            multi_data = df_multi[df_multi['Metric'] == metric_name].iloc[0]
        except IndexError:
            print(f"  Warning: Metric '{metric_name}' not found in data, skipping...")
            ax.text(0.5, 0.5, f'{metric_name}\nData not available',
                   ha='center', va='center', transform=ax.transAxes, fontsize=12)
            ax.set_xticks([])
            ax.set_yticks([])
            continue

        # Single-site data
        ccgrid_single_mean = single_data['CCGrid Mean']
        ccgrid_single_std = single_data['CCGrid Std']
        v2_single_mean = single_data['SWARM-Enhanced Mean']
        v2_single_std = single_data['SWARM-Enhanced Std']

        # Multi-site data
        ccgrid_multi_mean = multi_data['CCGrid Mean']
        ccgrid_multi_std = multi_data['CCGrid Std']
        v2_multi_mean = multi_data['SWARM-Enhanced Mean']
        v2_multi_std = multi_data['SWARM-Enhanced Std']

        # Create grouped bar chart
        x = np.arange(2)  # Two groups: Single-Site, Multi-Site
        width = 0.35

        systems = ['Single-Site', 'Multi-Site']
        ccgrid_means = [ccgrid_single_mean, ccgrid_multi_mean]
        ccgrid_stds = [ccgrid_single_std, ccgrid_multi_std]
        v2_means = [v2_single_mean, v2_multi_mean]
        v2_stds = [v2_single_std, v2_multi_std]

        bars1 = ax.bar(x - width/2, ccgrid_means, width, yerr=ccgrid_stds,
                      label='CCGrid\'25', color=colors[0], alpha=0.7,
                      edgecolor='black', capsize=4, linewidth=1.5)
        bars2 = ax.bar(x + width/2, v2_means, width, yerr=v2_stds,
                      label='SWARM V2', color=colors[1], alpha=0.7,
                      edgecolor='black', capsize=4, linewidth=1.5)

        # Add value labels on bars
        for bars, means, stds in [(bars1, ccgrid_means, ccgrid_stds),
                                   (bars2, v2_means, v2_stds)]:
            for bar, mean, std in zip(bars, means, stds):
                height = bar.get_height()
                if mean < 1:
                    label = f'{mean:.2f}'
                elif mean < 10:
                    label = f'{mean:.1f}'
                else:
                    label = f'{mean:.1f}'
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       label, ha='center', va='bottom', fontsize=8, fontweight='bold')

        # Add improvement percentages
        single_improvement = abs(single_data['Change (%)'])
        multi_improvement = abs(multi_data['Change (%)'])

        ax.text(0, max(ccgrid_single_mean, v2_single_mean) * 1.1,
               f'{single_improvement:.0f}%↓', ha='center', fontsize=9,
               bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.5))
        ax.text(1, max(ccgrid_multi_mean, v2_multi_mean) * 1.1,
               f'{multi_improvement:.0f}%↓', ha='center', fontsize=9,
               bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.5))

        ax.set_ylabel(metric_name.split('(')[0].strip())
        ax.set_title(metric_name)
        ax.set_xticks(x)
        ax.set_xticklabels(systems)
        ax.legend(loc='upper right', fontsize=9)
        ax.grid(True, alpha=0.3, axis='y')

    plt.suptitle('SWARM+ vs CCGrid\'25: Performance Improvements\n(Mesh-10, 100 jobs, Single-Site & Multi-Site)',
                fontsize=15, fontweight='bold', y=0.995)
    plt.tight_layout()

    # Save
    output_dir = Path("../paper/figures")
    output_dir.mkdir(exist_ok=True, parents=True)
    plt.savefig(output_dir / 'ccgrid_vs_v2_detailed.pdf', bbox_inches='tight')
    plt.savefig(output_dir / 'ccgrid_vs_v2_detailed.png', bbox_inches='tight', dpi=300)
    print(f"Saved: {output_dir / 'ccgrid_vs_v2_detailed.pdf'}")
    plt.close()


def plot_ccgrid_multi_comparison_compact():
    """Create compact single-column comparison for multi-site only (saves space in paper)."""

    # Load multi-site data only
    multi_file = Path("runs/multi-site/comparison_ccgrid_vs_v2/comparison_summary.csv")
    df_multi = pd.read_csv(multi_file)

    # Create compact 2x1 layout (one column, two rows)
    fig, axes = plt.subplots(2, 1, figsize=(7, 10))

    # Colors for bars
    ccgrid_color = '#d62728'
    v2_color = '#2ca02c'
    systems = ['CCGrid\'25', 'SWARM+']

    # Selection time - top panel
    ax = axes[0]
    try:
        selection_data = df_multi[df_multi['Metric'] == 'Mean Selection Time (s)'].iloc[0]
        ccgrid_sel = selection_data['CCGrid Mean']
        v2_sel = selection_data['SWARM-Enhanced Mean']
        improvement_sel = abs(selection_data['Change (%)'])

        selection_times = [ccgrid_sel, v2_sel]
        colors = [ccgrid_color, v2_color]

        bars = ax.bar(systems, selection_times, color=colors, alpha=0.7, edgecolor='black', linewidth=2)
        ax.set_ylabel('Mean Selection Time (seconds)', fontweight='bold')
        ax.set_title('Job Selection Time (Multi-Site)', fontsize=13, fontweight='bold')
        ax.set_ylim([0, max(selection_times) * 1.35])
        ax.grid(True, alpha=0.3, axis='y')

        # Add value labels
        for bar, val in zip(bars, selection_times):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height * 1.02,
                    f'{val:.2f}s', ha='center', va='bottom', fontweight='bold', fontsize=10)

        # Add improvement annotation
        ax.text(0.5, max(selection_times) * 1.25,
                f'{improvement_sel:.1f}% reduction\n({ccgrid_sel/v2_sel:.0f}× speedup)',
                ha='center', fontsize=10, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.7))

    except (IndexError, KeyError) as e:
        print(f"  Warning: Selection time data not found: {e}")
        ax.text(0.5, 0.5, 'Selection Time\nData not available',
               ha='center', va='center', transform=ax.transAxes, fontsize=12)

    # Scheduling Latency - bottom panel
    ax = axes[1]
    try:
        sched_data = df_multi[df_multi['Metric'] == 'Mean Scheduling Latency (s)'].iloc[0]
        ccgrid_sched = sched_data['CCGrid Mean']
        v2_sched = sched_data['SWARM-Enhanced Mean']
        improvement_sched = abs(sched_data['Change (%)'])

        sched_times = [ccgrid_sched, v2_sched]
        colors = [ccgrid_color, v2_color]

        bars = ax.bar(systems, sched_times, color=colors, alpha=0.7, edgecolor='black', linewidth=2)
        ax.set_ylabel('Mean Scheduling Latency (seconds)', fontweight='bold')
        ax.set_title('Scheduling Latency (Multi-Site)', fontsize=13, fontweight='bold')
        ax.set_ylim([0, max(sched_times) * 1.35])
        ax.grid(True, alpha=0.3, axis='y')

        for bar, val in zip(bars, sched_times):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height * 1.02,
                    f'{val:.1f}s', ha='center', va='bottom', fontweight='bold', fontsize=10)

        ax.text(0.5, max(sched_times) * 1.25,
                f'{improvement_sched:.1f}% reduction\n({ccgrid_sched/v2_sched:.0f}× speedup)',
                ha='center', fontsize=10, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.7))

    except (IndexError, KeyError) as e:
        print(f"  Warning: Scheduling latency data not found: {e}")
        ax.text(0.5, 0.5, 'Scheduling Latency\nData not available',
               ha='center', va='center', transform=ax.transAxes, fontsize=12)

    plt.suptitle('SWARM+ vs CCGrid\'25: Multi-Site Performance',
                fontsize=15, fontweight='bold')
    plt.tight_layout()

    output_dir = Path("../paper/figures")
    plt.savefig(output_dir / 'ccgrid_vs_v2_multi_compact.pdf', bbox_inches='tight')
    plt.savefig(output_dir / 'ccgrid_vs_v2_multi_compact.png', bbox_inches='tight', dpi=300)
    print(f"Saved: {output_dir / 'ccgrid_vs_v2_multi_compact.pdf'}")
    plt.close()

def plot_ccgrid_comparison_compact():
    """Create compact side-by-side comparison for main paper figure with both single-site and multi-site."""

    # Load both datasets
    single_file = Path("runs/single-site/comparison_ccgrid_vs_v2/comparison_summary.csv")
    multi_file = Path("runs/multi-site/comparison_ccgrid_vs_v2/comparison_summary.csv")
    df_single = pd.read_csv(single_file)
    df_multi = pd.read_csv(multi_file)

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Colors for grouped bars
    ccgrid_color = '#d62728'
    v2_color = '#2ca02c'

    scenarios = ['Single-Site', 'Multi-Site']
    x = np.arange(len(scenarios))
    width = 0.35

    # Selection time - top row
    for idx, (df, scenario) in enumerate([(df_single, 'Single-Site'), (df_multi, 'Multi-Site')]):
        ax = axes[0, idx]
        try:
            selection_data = df[df['Metric'] == 'Mean Selection Time (s)'].iloc[0]
            ccgrid_sel = selection_data['CCGrid Mean']
            v2_sel = selection_data['SWARM-Enhanced Mean']
            improvement_sel = abs(selection_data['Change (%)'])
        except (IndexError, KeyError) as e:
            print(f"  Warning: Selection time data not found for {scenario}: {e}")
            ax.text(0.5, 0.5, f'Selection Time\n{scenario}\nData not available',
                   ha='center', va='center', transform=ax.transAxes, fontsize=12)
            continue

        systems = ['CCGrid\'25', 'SWARM+']
        selection_times = [ccgrid_sel, v2_sel]
        colors = [ccgrid_color, v2_color]

        bars = ax.bar(systems, selection_times, color=colors, alpha=0.7, edgecolor='black', linewidth=2)
        ax.set_ylabel('Mean Selection Time (seconds)', fontweight='bold')
        ax.set_title(f'Job Selection Time ({scenario})', fontsize=13, fontweight='bold')
        ax.set_ylim([0, max(selection_times) * 1.35])
        ax.grid(True, alpha=0.3, axis='y')

        # Add value labels
        for bar, val in zip(bars, selection_times):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height * 1.02,
                    f'{val:.2f}s', ha='center', va='bottom', fontweight='bold', fontsize=10)

        # Add improvement annotation
        ax.text(0.5, max(selection_times) * 1.25,
                f'{improvement_sel:.1f}% reduction\n({ccgrid_sel/v2_sel:.0f}× speedup)',
                ha='center', fontsize=10, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.7))

    # Scheduling Latency - bottom row
    for idx, (df, scenario) in enumerate([(df_single, 'Single-Site'), (df_multi, 'Multi-Site')]):
        ax = axes[1, idx]
        try:
            sched_data = df[df['Metric'] == 'Mean Scheduling Latency (s)'].iloc[0]
            ccgrid_sched = sched_data['CCGrid Mean']
            v2_sched = sched_data['SWARM-Enhanced Mean']
            improvement_sched = abs(sched_data['Change (%)'])
        except (IndexError, KeyError) as e:
            print(f"  Warning: Scheduling latency data not found for {scenario}: {e}")
            ax.text(0.5, 0.5, f'Scheduling Latency\n{scenario}\nData not available',
                   ha='center', va='center', transform=ax.transAxes, fontsize=12)
            continue

        sched_times = [ccgrid_sched, v2_sched]
        colors = [ccgrid_color, v2_color]

        bars = ax.bar(systems, sched_times, color=colors, alpha=0.7, edgecolor='black', linewidth=2)
        ax.set_ylabel('Mean Scheduling Latency (seconds)', fontweight='bold')
        ax.set_title(f'Scheduling Latency ({scenario})', fontsize=13, fontweight='bold')
        ax.set_ylim([0, max(sched_times) * 1.35])
        ax.grid(True, alpha=0.3, axis='y')

        for bar, val in zip(bars, sched_times):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height * 1.02,
                    f'{val:.1f}s', ha='center', va='bottom', fontweight='bold', fontsize=10)

        ax.text(0.5, max(sched_times) * 1.25,
                f'{improvement_sched:.1f}% reduction\n({ccgrid_sched/v2_sched:.0f}× speedup)',
                ha='center', fontsize=10, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.7))

    plt.suptitle('SWARM+ Performance Improvements over CCGrid\'25',
                fontsize=15, fontweight='bold')
    plt.tight_layout()

    output_dir = Path("../paper/figures")
    plt.savefig(output_dir / 'ccgrid_vs_v2_compact.pdf', bbox_inches='tight')
    plt.savefig(output_dir / 'ccgrid_vs_v2_compact.png', bbox_inches='tight', dpi=300)
    print(f"Saved: {output_dir / 'ccgrid_vs_v2_compact.pdf'}")
    plt.close()


def plot_ccgrid_boxplot_comparison():
    """Create box plot comparison using per-run data for both single-site and multi-site."""

    # Load single-site data
    single_ccgrid = Path("runs/single-site/comparison_ccgrid_vs_v2/ccgrid_aggregated.csv")
    single_v2 = Path("runs/single-site/comparison_ccgrid_vs_v2/v2_aggregated.csv")

    # Load multi-site data
    multi_ccgrid = Path("runs/multi-site/comparison_ccgrid_vs_v2/ccgrid_aggregated.csv")
    multi_v2 = Path("runs/multi-site/comparison_ccgrid_vs_v2/v2_aggregated.csv")

    df_single_ccgrid = pd.read_csv(single_ccgrid)
    df_single_v2 = pd.read_csv(single_v2)
    df_multi_ccgrid = pd.read_csv(multi_ccgrid)
    df_multi_v2 = pd.read_csv(multi_v2)

    # Add labels
    df_single_ccgrid['System'] = 'CCGrid\'25'
    df_single_ccgrid['Scenario'] = 'Single-Site'
    df_single_v2['System'] = 'SWARM+'
    df_single_v2['Scenario'] = 'Single-Site'
    df_multi_ccgrid['System'] = 'CCGrid\'25'
    df_multi_ccgrid['Scenario'] = 'Multi-Site'
    df_multi_v2['System'] = 'SWARM+'
    df_multi_v2['Scenario'] = 'Multi-Site'

    # Combine all data
    df_combined = pd.concat([df_single_ccgrid, df_single_v2, df_multi_ccgrid, df_multi_v2])
    df_combined['System-Scenario'] = df_combined['System'] + '\n' + df_combined['Scenario']

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Selection time boxplot
    ax = axes[0]
    order = ['CCGrid\'25\nSingle-Site', 'SWARM+\nSingle-Site',
             'CCGrid\'25\nMulti-Site', 'SWARM+\nMulti-Site']
    palette = {
        'CCGrid\'25\nSingle-Site': '#d62728',
        'SWARM+\nSingle-Site': '#2ca02c',
        'CCGrid\'25\nMulti-Site': '#ff7f0e',
        'SWARM+\nMulti-Site': '#1f77b4'
    }
    sns.boxplot(data=df_combined, x='System-Scenario', y='mean_selection_time',
               hue='System-Scenario', ax=ax, order=order, palette=palette, legend=False)
    ax.set_ylabel('Mean Selection Time (seconds)', fontweight='bold')
    ax.set_xlabel('')
    ax.set_title('Selection Time Distribution (50 runs each)', fontsize=13, fontweight='bold')
    ax.grid(True, alpha=0.3, axis='y')
    ax.tick_params(axis='x', rotation=0)

    # Add median values
    for i, sys_scenario in enumerate(order):
        data = df_combined[df_combined['System-Scenario'] == sys_scenario]['mean_selection_time']
        if len(data) > 0:
            median = data.median()
            ax.text(i, median, f'{median:.2f}',
                   ha='center', va='bottom', fontsize=8, fontweight='bold',
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8, pad=0.2))

    # Scheduling latency boxplot
    ax = axes[1]
    sns.boxplot(data=df_combined, x='System-Scenario', y='mean_scheduling_latency',
               hue='System-Scenario', ax=ax, order=order, palette=palette, legend=False)
    ax.set_ylabel('Mean Scheduling Latency (seconds)', fontweight='bold')
    ax.set_xlabel('')
    ax.set_title('Scheduling Latency Distribution (50 runs each)', fontsize=13, fontweight='bold')
    ax.grid(True, alpha=0.3, axis='y')
    ax.tick_params(axis='x', rotation=0)

    for i, sys_scenario in enumerate(order):
        data = df_combined[df_combined['System-Scenario'] == sys_scenario]['mean_scheduling_latency']
        if len(data) > 0:
            median = data.median()
            ax.text(i, median, f'{median:.1f}',
                   ha='center', va='bottom', fontsize=8, fontweight='bold',
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8, pad=0.2))

    plt.suptitle('CCGrid\'25 vs SWARM+: Statistical Comparison (Single-Site & Multi-Site)',
                fontsize=15, fontweight='bold')
    plt.tight_layout()

    output_dir = Path("../paper/figures")
    plt.savefig(output_dir / 'ccgrid_vs_v2_boxplot.pdf', bbox_inches='tight')
    plt.savefig(output_dir / 'ccgrid_vs_v2_boxplot.png', bbox_inches='tight', dpi=300)
    print(f"Saved: {output_dir / 'ccgrid_vs_v2_boxplot.pdf'}")
    plt.close()


def plot_ccgrid_unified_comparison():
    """Create unified comparison showing all four scenarios in one figure."""

    # Load data
    single_file = Path("runs/single-site/comparison_ccgrid_vs_v2/comparison_summary.csv")
    multi_file = Path("runs/multi-site/comparison_ccgrid_vs_v2/comparison_summary.csv")
    df_single = pd.read_csv(single_file)
    df_multi = pd.read_csv(multi_file)

    # Create figure with 1x2 subplots (selection time and scheduling latency)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # Define colors and patterns
    colors = {
        'CCGrid': '#d62728',
        'Enhanced Single': '#2ca02c',
        'CCGrid': '#ff7f0e',
        'Enhanced Multi': '#1f77b4'
    }

    # Selection time comparison
    try:
        selection_single = df_single[df_single['Metric'] == 'Mean Selection Time (s)'].iloc[0]
        selection_multi = df_multi[df_multi['Metric'] == 'Mean Selection Time (s)'].iloc[0]

        systems = ['CCGrid\'25\nSingle', 'Enhanced\nSingle', 'CCGrid\'25\nMulti', 'Enhanced\nMulti']
        selection_means = [
            selection_single['CCGrid Mean'],
            selection_single['SWARM-Enhanced Mean'],
            selection_multi['CCGrid Mean'],
            selection_multi['SWARM-Enhanced Mean']
        ]
        selection_stds = [
            selection_single['CCGrid Std'],
            selection_single['SWARM-Enhanced Std'],
            selection_multi['CCGrid Std'],
            selection_multi['SWARM-Enhanced Std']
        ]
    except (IndexError, KeyError) as e:
        print(f"  Warning: Selection time data not found: {e}")
        ax1.text(0.5, 0.5, 'Selection Time\nData not available',
                ha='center', va='center', transform=ax1.transAxes, fontsize=14)
        selection_means = []

    if selection_means:
        bars1 = ax1.bar(systems, selection_means, yerr=selection_stds,
                       color=[colors['CCGrid'], colors['Enhanced Single'],
                              colors['CCGrid'], colors['Enhanced Multi']],
                       alpha=0.7, edgecolor='black', capsize=5, linewidth=1.5)

        # Add value labels
        for bar, mean in zip(bars1, selection_means):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height * 1.02,
                    f'{mean:.2f}s', ha='center', va='bottom',
                    fontweight='bold', fontsize=10)

        # Add improvement arrows between CCGrid and V2 for each scenario
        single_improvement = abs(selection_single['Change (%)'])
        multi_improvement = abs(selection_multi['Change (%)'])

        # Arrow for single-site improvement
        ax1.annotate('', xy=(1, selection_single['SWARM-Enhanced Mean']),
                    xytext=(0, selection_single['CCGrid Mean']),
                    arrowprops=dict(arrowstyle='->', lw=2, color='green', alpha=0.5))
        ax1.text(0.5, max(selection_single['CCGrid Mean'], selection_single['SWARM-Enhanced Mean']) * 1.1,
                f'{single_improvement:.0f}%↓', ha='center', fontsize=11, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.6))

        # Arrow for multi-site improvement
        ax1.annotate('', xy=(3, selection_multi['SWARM-Enhanced Mean']),
                    xytext=(2, selection_multi['CCGrid Mean']),
                    arrowprops=dict(arrowstyle='->', lw=2, color='green', alpha=0.5))
        ax1.text(2.5, max(selection_multi['CCGrid Mean'], selection_multi['SWARM-Enhanced Mean']) * 1.1,
                f'{multi_improvement:.0f}%↓', ha='center', fontsize=11, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.6))

        ax1.set_ylabel('Mean Selection Time (seconds)', fontweight='bold', fontsize=12)
        ax1.set_title('Job Selection Time Comparison', fontsize=14, fontweight='bold')
        ax1.grid(True, alpha=0.3, axis='y')
        ax1.set_ylim([0, max(selection_means) * 1.25])

    # Scheduling latency comparison
    try:
        scheduling_single = df_single[df_single['Metric'] == 'Mean Scheduling Latency (s)'].iloc[0]
        scheduling_multi = df_multi[df_multi['Metric'] == 'Mean Scheduling Latency (s)'].iloc[0]

        scheduling_means = [
            scheduling_single['CCGrid Mean'],
            scheduling_single['SWARM-Enhanced Mean'],
            scheduling_multi['CCGrid Mean'],
            scheduling_multi['SWARM-Enhanced Mean']
        ]
        scheduling_stds = [
            scheduling_single['CCGrid Std'],
            scheduling_single['SWARM-Enhanced Std'],
            scheduling_multi['CCGrid Std'],
            scheduling_multi['SWARM-Enhanced Std']
        ]
    except (IndexError, KeyError) as e:
        print(f"  Warning: Scheduling latency data not found: {e}")
        ax2.text(0.5, 0.5, 'Scheduling Latency\nData not available',
                ha='center', va='center', transform=ax2.transAxes, fontsize=14)
        scheduling_means = []

    if scheduling_means:
        bars2 = ax2.bar(systems, scheduling_means, yerr=scheduling_stds,
                       color=[colors['CCGrid'], colors['Enhanced Single'],
                              colors['CCGrid'], colors['Enhanced Multi']],
                       alpha=0.7, edgecolor='black', capsize=5, linewidth=1.5)

        for bar, mean in zip(bars2, scheduling_means):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height * 1.02,
                    f'{mean:.1f}s', ha='center', va='bottom',
                    fontweight='bold', fontsize=10)

        # Improvement annotations
        single_sched_improvement = abs(scheduling_single['Change (%)'])
        multi_sched_improvement = abs(scheduling_multi['Change (%)'])

        ax2.annotate('', xy=(1, scheduling_single['SWARM-Enhanced Mean']),
                    xytext=(0, scheduling_single['CCGrid Mean']),
                    arrowprops=dict(arrowstyle='->', lw=2, color='green', alpha=0.5))
        ax2.text(0.5, max(scheduling_single['CCGrid Mean'], scheduling_single['SWARM-Enhanced Mean']) * 1.1,
                f'{single_sched_improvement:.0f}%↓', ha='center', fontsize=11, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.6))

        ax2.annotate('', xy=(3, scheduling_multi['SWARM-Enhanced Mean']),
                    xytext=(2, scheduling_multi['CCGrid Mean']),
                    arrowprops=dict(arrowstyle='->', lw=2, color='green', alpha=0.5))
        ax2.text(2.5, max(scheduling_multi['CCGrid Mean'], scheduling_multi['SWARM-Enhanced Mean']) * 1.1,
                f'{multi_sched_improvement:.0f}%↓', ha='center', fontsize=11, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.6))

        ax2.set_ylabel('Mean Scheduling Latency (seconds)', fontweight='bold', fontsize=12)
        ax2.set_title('End-to-End Scheduling Latency Comparison', fontsize=14, fontweight='bold')
        ax2.grid(True, alpha=0.3, axis='y')
        ax2.set_ylim([0, max(scheduling_means) * 1.25])

    plt.suptitle('SWARM+ vs CCGrid\'25: Comprehensive Performance Comparison',
                fontsize=16, fontweight='bold')
    plt.tight_layout()

    output_dir = Path("../paper/figures")
    plt.savefig(output_dir / 'ccgrid_vs_v2_unified.pdf', bbox_inches='tight')
    plt.savefig(output_dir / 'ccgrid_vs_v2_unified.png', bbox_inches='tight', dpi=300)
    print(f"Saved: {output_dir / 'CCGrid_vs_v2_unified.pdf'}")
    plt.close()


if __name__ == '__main__':
    print("="*80)
    print("Generating CCGrid vs V2 Comparison Plots")
    print("="*80)

    print("\n1. Detailed comparison (4-panel, grouped by metric)...")
    plot_ccgrid_comparison_detailed()

    print("\n2. Compact comparison (4-panel, grouped by scenario)...")
    plot_ccgrid_comparison_compact()
    plot_ccgrid_multi_comparison_compact()

    print("\n3. Box plot comparison (statistical distributions)...")
    plot_ccgrid_boxplot_comparison()

    print("\n4. Unified comparison (all four scenarios)...")
    plot_ccgrid_unified_comparison()

    print("\n" + "="*80)
    print("All CCGrid comparison plots saved to: paper/figures/")
    print("="*80)
