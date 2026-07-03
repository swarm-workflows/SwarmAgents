#!/usr/bin/env python3
"""
Create side-by-side comparison visualizations for DTN vs No-DTN runs.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import numpy as np

def load_completed_jobs(csv_path):
    """Load and filter for completed jobs only."""
    df = pd.read_csv(csv_path)
    completed = df[df['exit_status'] == 0].copy()
    completed['selection_time'] = completed['assigned_at'] - completed['selection_started_at']
    return completed

def main():
    base_dir = Path("/Users/kthare10/swarm/agents/SwarmAgents/runs/dtn-local")
    output_dir = base_dir / "comparison_plots"
    output_dir.mkdir(exist_ok=True)

    # Load all data
    data = {}
    for scenario in ['hier110-no-dtn', 'hier110-dtn']:
        data[scenario] = {}
        for run in ['run-1', 'run-2']:
            run_dir = base_dir / scenario / run
            data[scenario][run] = {
                'all': load_completed_jobs(run_dir / 'all_jobs.csv'),
                'level0': load_completed_jobs(run_dir / 'level0_jobs.csv'),
                'level1': load_completed_jobs(run_dir / 'level1_jobs.csv')
            }

    # Combine runs
    combined = {}
    for scenario in ['hier110-no-dtn', 'hier110-dtn']:
        combined[scenario] = {
            'all': pd.concat([data[scenario]['run-1']['all'], data[scenario]['run-2']['all']]),
            'level0': pd.concat([data[scenario]['run-1']['level0'], data[scenario]['run-2']['level0']]),
            'level1': pd.concat([data[scenario]['run-1']['level1'], data[scenario]['run-2']['level1']])
        }

    # 1. Scheduling Latency Distribution (All Jobs)
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    for idx, scenario in enumerate(['hier110-no-dtn', 'hier110-dtn']):
        ax = axes[idx]
        latency = combined[scenario]['all']['scheduling_latency']

        # Filter extreme outliers for better visualization
        latency_filtered = latency[latency <= latency.quantile(0.99)]

        ax.hist(latency_filtered, bins=50, alpha=0.7, edgecolor='black')
        ax.axvline(latency.mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: {latency.mean():.3f}s')
        ax.axvline(latency.median(), color='green', linestyle='--', linewidth=2, label=f'Median: {latency.median():.3f}s')
        ax.axvline(latency.quantile(0.95), color='orange', linestyle='--', linewidth=2, label=f'P95: {latency.quantile(0.95):.3f}s')

        title = 'No-DTN' if 'no-dtn' in scenario else 'DTN-Aware'
        ax.set_title(f'{title} - Scheduling Latency Distribution', fontsize=12, fontweight='bold')
        ax.set_xlabel('Scheduling Latency (s)', fontsize=11)
        ax.set_ylabel('Frequency', fontsize=11)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / 'scheduling_latency_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_dir / 'scheduling_latency_distribution.png'}")

    # 2. Selection Time Distribution (All Jobs)
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    for idx, scenario in enumerate(['hier110-no-dtn', 'hier110-dtn']):
        ax = axes[idx]
        sel_time = combined[scenario]['all']['selection_time']

        # Filter extreme outliers
        sel_time_filtered = sel_time[sel_time <= sel_time.quantile(0.99)]

        ax.hist(sel_time_filtered, bins=50, alpha=0.7, edgecolor='black', color='steelblue')
        ax.axvline(sel_time.mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: {sel_time.mean():.3f}s')
        ax.axvline(sel_time.median(), color='green', linestyle='--', linewidth=2, label=f'Median: {sel_time.median():.3f}s')
        ax.axvline(sel_time.quantile(0.95), color='orange', linestyle='--', linewidth=2, label=f'P95: {sel_time.quantile(0.95):.3f}s')

        title = 'No-DTN' if 'no-dtn' in scenario else 'DTN-Aware'
        ax.set_title(f'{title} - Selection Time Distribution', fontsize=12, fontweight='bold')
        ax.set_xlabel('Selection Time (s)', fontsize=11)
        ax.set_ylabel('Frequency', fontsize=11)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / 'selection_time_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_dir / 'selection_time_distribution.png'}")

    # 3. Box plots by hierarchy level
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Scheduling Latency - Level 0
    ax = axes[0, 0]
    data_plot = [combined['hier110-no-dtn']['level0']['scheduling_latency'],
                 combined['hier110-dtn']['level0']['scheduling_latency']]
    bp = ax.boxplot(data_plot, labels=['No-DTN', 'DTN'], patch_artist=True, showfliers=False)
    for patch, color in zip(bp['boxes'], ['lightcoral', 'lightblue']):
        patch.set_facecolor(color)
    ax.set_title('Level-0: Scheduling Latency', fontsize=12, fontweight='bold')
    ax.set_ylabel('Scheduling Latency (s)', fontsize=11)
    ax.grid(True, alpha=0.3, axis='y')

    # Scheduling Latency - Level 1
    ax = axes[0, 1]
    data_plot = [combined['hier110-no-dtn']['level1']['scheduling_latency'],
                 combined['hier110-dtn']['level1']['scheduling_latency']]
    bp = ax.boxplot(data_plot, labels=['No-DTN', 'DTN'], patch_artist=True, showfliers=False)
    for patch, color in zip(bp['boxes'], ['lightcoral', 'lightblue']):
        patch.set_facecolor(color)
    ax.set_title('Level-1: Scheduling Latency', fontsize=12, fontweight='bold')
    ax.set_ylabel('Scheduling Latency (s)', fontsize=11)
    ax.grid(True, alpha=0.3, axis='y')

    # Selection Time - Level 0
    ax = axes[1, 0]
    data_plot = [combined['hier110-no-dtn']['level0']['selection_time'],
                 combined['hier110-dtn']['level0']['selection_time']]
    bp = ax.boxplot(data_plot, labels=['No-DTN', 'DTN'], patch_artist=True, showfliers=False)
    for patch, color in zip(bp['boxes'], ['lightcoral', 'lightblue']):
        patch.set_facecolor(color)
    ax.set_title('Level-0: Selection Time', fontsize=12, fontweight='bold')
    ax.set_ylabel('Selection Time (s)', fontsize=11)
    ax.grid(True, alpha=0.3, axis='y')

    # Selection Time - Level 1
    ax = axes[1, 1]
    data_plot = [combined['hier110-no-dtn']['level1']['selection_time'],
                 combined['hier110-dtn']['level1']['selection_time']]
    bp = ax.boxplot(data_plot, labels=['No-DTN', 'DTN'], patch_artist=True, showfliers=False)
    for patch, color in zip(bp['boxes'], ['lightcoral', 'lightblue']):
        patch.set_facecolor(color)
    ax.set_title('Level-1: Selection Time', fontsize=12, fontweight='bold')
    ax.set_ylabel('Selection Time (s)', fontsize=11)
    ax.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    plt.savefig(output_dir / 'latency_by_level_boxplot.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_dir / 'latency_by_level_boxplot.png'}")

    # 4. Summary comparison bar chart
    metrics = []
    for scenario in ['No-DTN', 'DTN']:
        scenario_key = 'hier110-no-dtn' if scenario == 'No-DTN' else 'hier110-dtn'
        for level_name, level_key in [('All Jobs', 'all'), ('Level-0', 'level0'), ('Level-1', 'level1')]:
            df = combined[scenario_key][level_key]
            metrics.append({
                'Scenario': scenario,
                'Level': level_name,
                'Mean Scheduling Latency': df['scheduling_latency'].mean(),
                'Median Scheduling Latency': df['scheduling_latency'].median(),
                'P95 Scheduling Latency': df['scheduling_latency'].quantile(0.95),
                'Mean Selection Time': df['selection_time'].mean(),
                'Median Selection Time': df['selection_time'].median(),
            })

    metrics_df = pd.DataFrame(metrics)

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))

    # Mean Scheduling Latency
    ax = axes[0, 0]
    pivot = metrics_df.pivot(index='Level', columns='Scenario', values='Mean Scheduling Latency')
    pivot.plot(kind='bar', ax=ax, color=['lightcoral', 'lightblue'], width=0.7)
    ax.set_title('Mean Scheduling Latency by Level', fontsize=12, fontweight='bold')
    ax.set_ylabel('Scheduling Latency (s)', fontsize=11)
    ax.set_xlabel('')
    ax.legend(title='')
    ax.grid(True, alpha=0.3, axis='y')
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0)

    # P95 Scheduling Latency
    ax = axes[0, 1]
    pivot = metrics_df.pivot(index='Level', columns='Scenario', values='P95 Scheduling Latency')
    pivot.plot(kind='bar', ax=ax, color=['lightcoral', 'lightblue'], width=0.7)
    ax.set_title('P95 Scheduling Latency by Level', fontsize=12, fontweight='bold')
    ax.set_ylabel('Scheduling Latency (s)', fontsize=11)
    ax.set_xlabel('')
    ax.legend(title='')
    ax.grid(True, alpha=0.3, axis='y')
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0)

    # Mean Selection Time
    ax = axes[1, 0]
    pivot = metrics_df.pivot(index='Level', columns='Scenario', values='Mean Selection Time')
    pivot.plot(kind='bar', ax=ax, color=['lightcoral', 'lightblue'], width=0.7)
    ax.set_title('Mean Selection Time by Level', fontsize=12, fontweight='bold')
    ax.set_ylabel('Selection Time (s)', fontsize=11)
    ax.set_xlabel('')
    ax.legend(title='')
    ax.grid(True, alpha=0.3, axis='y')
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0)

    # Median Selection Time
    ax = axes[1, 1]
    pivot = metrics_df.pivot(index='Level', columns='Scenario', values='Median Selection Time')
    pivot.plot(kind='bar', ax=ax, color=['lightcoral', 'lightblue'], width=0.7)
    ax.set_title('Median Selection Time by Level', fontsize=12, fontweight='bold')
    ax.set_ylabel('Selection Time (s)', fontsize=11)
    ax.set_xlabel('')
    ax.legend(title='')
    ax.grid(True, alpha=0.3, axis='y')
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0)

    plt.tight_layout()
    plt.savefig(output_dir / 'summary_comparison_bars.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_dir / 'summary_comparison_bars.png'}")

    print(f"\n\nAll comparison plots saved to: {output_dir}")

if __name__ == "__main__":
    main()
