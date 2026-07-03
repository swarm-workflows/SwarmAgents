#!/usr/bin/env python3
"""
Extract scheduling latency metrics from Hier-110 DTN runs for COMPLETED jobs only.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json

def analyze_csv(csv_path):
    """Analyze a single CSV file for completed jobs metrics."""
    df = pd.read_csv(csv_path)

    # Filter for completed jobs only (exit_status == 0)
    completed = df[df['exit_status'] == 0].copy()
    failed = df[df['exit_status'] != 0]

    # Compute selection_time = assigned_at - selection_started_at
    completed['selection_time'] = completed['assigned_at'] - completed['selection_started_at']

    metrics = {
        'count_completed': len(completed),
        'count_failed': len(failed),
        'scheduling_latency': {
            'mean': completed['scheduling_latency'].mean() if len(completed) > 0 else 0,
            'median': completed['scheduling_latency'].median() if len(completed) > 0 else 0,
            'std': completed['scheduling_latency'].std() if len(completed) > 0 else 0,
            'p95': completed['scheduling_latency'].quantile(0.95) if len(completed) > 0 else 0,
        },
        'selection_time': {
            'mean': completed['selection_time'].mean() if len(completed) > 0 else 0,
            'median': completed['selection_time'].median() if len(completed) > 0 else 0,
            'std': completed['selection_time'].std() if len(completed) > 0 else 0,
            'p95': completed['selection_time'].quantile(0.95) if len(completed) > 0 else 0,
        }
    }

    return metrics

def main():
    base_dir = Path("/Users/kthare10/swarm/agents/SwarmAgents/runs/dtn-local")

    scenarios = {
        'No-DTN': base_dir / "hier110-no-dtn",
        'DTN': base_dir / "hier110-dtn"
    }

    results = {}

    for scenario_name, scenario_path in scenarios.items():
        print(f"\n{'='*60}")
        print(f"Analyzing: {scenario_name}")
        print(f"{'='*60}")

        scenario_results = {
            'runs': {},
            'average': None
        }

        # Analyze each run
        for run_num in [1, 2]:
            run_dir = scenario_path / f"run-{run_num}"

            print(f"\n--- Run {run_num} ---")

            # Check for all_jobs.csv
            all_jobs_path = run_dir / "all_jobs.csv"
            if all_jobs_path.exists():
                print(f"\nAll Jobs: {all_jobs_path}")
                metrics = analyze_csv(all_jobs_path)
                scenario_results['runs'][f'run-{run_num}'] = {'all': metrics}

                print(f"  Completed: {metrics['count_completed']}")
                print(f"  Failed: {metrics['count_failed']}")
                print(f"  Scheduling Latency: mean={metrics['scheduling_latency']['mean']:.3f}s, "
                      f"median={metrics['scheduling_latency']['median']:.3f}s, "
                      f"std={metrics['scheduling_latency']['std']:.3f}s, "
                      f"P95={metrics['scheduling_latency']['p95']:.3f}s")
                print(f"  Selection Time: mean={metrics['selection_time']['mean']:.3f}s, "
                      f"median={metrics['selection_time']['median']:.3f}s, "
                      f"std={metrics['selection_time']['std']:.3f}s, "
                      f"P95={metrics['selection_time']['p95']:.3f}s")

            # Check for level-specific CSVs
            for level in ['level0', 'level1']:
                level_path = run_dir / f"{level}_jobs.csv"
                if level_path.exists():
                    print(f"\n{level.capitalize()} Jobs: {level_path}")
                    metrics = analyze_csv(level_path)
                    scenario_results['runs'][f'run-{run_num}'][level] = metrics

                    print(f"  Completed: {metrics['count_completed']}")
                    print(f"  Failed: {metrics['count_failed']}")
                    print(f"  Scheduling Latency: mean={metrics['scheduling_latency']['mean']:.3f}s, "
                          f"median={metrics['scheduling_latency']['median']:.3f}s, "
                          f"std={metrics['scheduling_latency']['std']:.3f}s, "
                          f"P95={metrics['scheduling_latency']['p95']:.3f}s")
                    print(f"  Selection Time: mean={metrics['selection_time']['mean']:.3f}s, "
                          f"median={metrics['selection_time']['median']:.3f}s, "
                          f"std={metrics['selection_time']['std']:.3f}s, "
                          f"P95={metrics['selection_time']['p95']:.3f}s")

        # Compute average across runs
        avg_metrics = {}

        # Average for all_jobs
        all_completed = []
        all_failed = []
        all_sched_latency = {'mean': [], 'median': [], 'std': [], 'p95': []}
        all_sel_time = {'mean': [], 'median': [], 'std': [], 'p95': []}

        for run_key, run_data in scenario_results['runs'].items():
            if 'all' in run_data:
                m = run_data['all']
                all_completed.append(m['count_completed'])
                all_failed.append(m['count_failed'])
                for metric in ['mean', 'median', 'std', 'p95']:
                    all_sched_latency[metric].append(m['scheduling_latency'][metric])
                    all_sel_time[metric].append(m['selection_time'][metric])

        avg_metrics['all'] = {
            'count_completed': np.mean(all_completed),
            'count_failed': np.mean(all_failed),
            'scheduling_latency': {k: np.mean(v) for k, v in all_sched_latency.items()},
            'selection_time': {k: np.mean(v) for k, v in all_sel_time.items()}
        }

        # Average for level0 and level1
        for level in ['level0', 'level1']:
            level_completed = []
            level_failed = []
            level_sched_latency = {'mean': [], 'median': [], 'std': [], 'p95': []}
            level_sel_time = {'mean': [], 'median': [], 'std': [], 'p95': []}

            for run_key, run_data in scenario_results['runs'].items():
                if level in run_data:
                    m = run_data[level]
                    level_completed.append(m['count_completed'])
                    level_failed.append(m['count_failed'])
                    for metric in ['mean', 'median', 'std', 'p95']:
                        level_sched_latency[metric].append(m['scheduling_latency'][metric])
                        level_sel_time[metric].append(m['selection_time'][metric])

            if level_completed:
                avg_metrics[level] = {
                    'count_completed': np.mean(level_completed),
                    'count_failed': np.mean(level_failed),
                    'scheduling_latency': {k: np.mean(v) for k, v in level_sched_latency.items()},
                    'selection_time': {k: np.mean(v) for k, v in level_sel_time.items()}
                }

        scenario_results['average'] = avg_metrics
        results[scenario_name] = scenario_results

    # Print comparison table
    print(f"\n\n{'='*80}")
    print("COMPARISON TABLE: Average Across 2 Runs (Completed Jobs Only)")
    print(f"{'='*80}")

    print("\n--- ALL JOBS ---")
    print(f"{'Metric':<30} {'No-DTN':<20} {'DTN':<20}")
    print("-" * 70)

    for scenario_name in ['No-DTN', 'DTN']:
        avg = results[scenario_name]['average']['all']
        if scenario_name == 'No-DTN':
            print(f"{'Completed Jobs':<30} {avg['count_completed']:<20.1f} ", end="")
        else:
            print(f"{avg['count_completed']:<20.1f}")

    for scenario_name in ['No-DTN', 'DTN']:
        avg = results[scenario_name]['average']['all']
        if scenario_name == 'No-DTN':
            print(f"{'Failed Jobs':<30} {avg['count_failed']:<20.1f} ", end="")
        else:
            print(f"{avg['count_failed']:<20.1f}")

    print()
    for metric_name, metric_key in [('Mean Scheduling Latency (s)', 'mean'),
                                      ('Median Scheduling Latency (s)', 'median'),
                                      ('Std Scheduling Latency (s)', 'std'),
                                      ('P95 Scheduling Latency (s)', 'p95')]:
        for scenario_name in ['No-DTN', 'DTN']:
            avg = results[scenario_name]['average']['all']
            val = avg['scheduling_latency'][metric_key]
            if scenario_name == 'No-DTN':
                print(f"{metric_name:<30} {val:<20.3f} ", end="")
            else:
                print(f"{val:<20.3f}")

    print()
    for metric_name, metric_key in [('Mean Selection Time (s)', 'mean'),
                                      ('Median Selection Time (s)', 'median'),
                                      ('Std Selection Time (s)', 'std'),
                                      ('P95 Selection Time (s)', 'p95')]:
        for scenario_name in ['No-DTN', 'DTN']:
            avg = results[scenario_name]['average']['all']
            val = avg['selection_time'][metric_key]
            if scenario_name == 'No-DTN':
                print(f"{metric_name:<30} {val:<20.3f} ", end="")
            else:
                print(f"{val:<20.3f}")

    # Level-specific tables
    for level in ['level0', 'level1']:
        if all(level in results[s]['average'] for s in ['No-DTN', 'DTN']):
            print(f"\n--- {level.upper()} JOBS ---")
            print(f"{'Metric':<30} {'No-DTN':<20} {'DTN':<20}")
            print("-" * 70)

            for scenario_name in ['No-DTN', 'DTN']:
                avg = results[scenario_name]['average'][level]
                if scenario_name == 'No-DTN':
                    print(f"{'Completed Jobs':<30} {avg['count_completed']:<20.1f} ", end="")
                else:
                    print(f"{avg['count_completed']:<20.1f}")

            for scenario_name in ['No-DTN', 'DTN']:
                avg = results[scenario_name]['average'][level]
                if scenario_name == 'No-DTN':
                    print(f"{'Failed Jobs':<30} {avg['count_failed']:<20.1f} ", end="")
                else:
                    print(f"{avg['count_failed']:<20.1f}")

            print()
            for metric_name, metric_key in [('Mean Scheduling Latency (s)', 'mean'),
                                              ('Median Scheduling Latency (s)', 'median'),
                                              ('Std Scheduling Latency (s)', 'std'),
                                              ('P95 Scheduling Latency (s)', 'p95')]:
                for scenario_name in ['No-DTN', 'DTN']:
                    avg = results[scenario_name]['average'][level]
                    val = avg['scheduling_latency'][metric_key]
                    if scenario_name == 'No-DTN':
                        print(f"{metric_name:<30} {val:<20.3f} ", end="")
                    else:
                        print(f"{val:<20.3f}")

            print()
            for metric_name, metric_key in [('Mean Selection Time (s)', 'mean'),
                                              ('Median Selection Time (s)', 'median'),
                                              ('Std Selection Time (s)', 'std'),
                                              ('P95 Selection Time (s)', 'p95')]:
                for scenario_name in ['No-DTN', 'DTN']:
                    avg = results[scenario_name]['average'][level]
                    val = avg['selection_time'][metric_key]
                    if scenario_name == 'No-DTN':
                        print(f"{metric_name:<30} {val:<20.3f} ", end="")
                    else:
                        print(f"{val:<20.3f}")

    # Save results to JSON
    output_path = Path("/Users/kthare10/swarm/agents/SwarmAgents/runs/dtn-local/dtn_metrics_analysis.json")
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\n\nResults saved to: {output_path}")

if __name__ == "__main__":
    main()
