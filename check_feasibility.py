#!/usr/bin/env python3
import csv

# Job IDs to check (extracted from XXX format)
target_jobs = [
'398', '416', '399', '348', '422', '370', '467', '447', '397', '482', '353', '321', '192', '392', '368', '279', '449', '402'
]

# Agents 9-30 (since 1-8 are killed)
available_agents = set(str(i) for i in range(9, 31))

csv_path = 'jobs/job_feasibility_mapping.csv'

feasible_jobs = []
infeasible_jobs = []
not_found_jobs = []

with open(csv_path, 'r') as f:
    reader = csv.DictReader(f)
    job_data = {row['job_id']: row for row in reader}

for job_id in target_jobs:
    if job_id not in job_data:
        not_found_jobs.append(job_id)
        continue

    row = job_data[job_id]
    feasible_agents_str = row['feasible_agents']

    if feasible_agents_str:
        feasible_agents = set(feasible_agents_str.split(','))
        # Check if any of agents 9-30 are in the feasible list
        overlap = feasible_agents & available_agents

        if overlap:
            feasible_jobs.append({
                'job_id': job_id,
                'can_run_on': sorted([int(a) for a in overlap]),
                'total_feasible': row['total_feasible'],
                'core': row['core'],
                'ram': row['ram'],
                'disk': row['disk'],
                'gpu': row['gpu']
            })
        else:
            infeasible_jobs.append({
                'job_id': job_id,
                'all_feasible_agents': row['feasible_agents'],
                'core': row['core'],
                'ram': row['ram'],
                'disk': row['disk'],
                'gpu': row['gpu']
            })
    else:
        infeasible_jobs.append({
            'job_id': job_id,
            'all_feasible_agents': 'NONE',
            'core': row['core'],
            'ram': row['ram'],
            'disk': row['disk'],
            'gpu': row['gpu']
        })

print(f"\n{'='*80}")
print(f"FEASIBILITY CHECK: Jobs vs Agents 9-30")
print(f"{'='*80}\n")

print(f"Total jobs checked: {len(target_jobs)}")
print(f"Jobs that CAN run on agents 9-30: {len(feasible_jobs)}")
print(f"Jobs that CANNOT run on agents 9-30: {len(infeasible_jobs)}")
if not_found_jobs:
    print(f"Jobs not found in CSV: {len(not_found_jobs)}")

if feasible_jobs:
    print(f"\n{'='*80}")
    print("FEASIBLE JOBS (can run on agents 9-30):")
    print(f"{'='*80}")
    for job in feasible_jobs:
        print(f"  Job {job['job_id']}: can run on agents {job['can_run_on']}")
        print(f"    Resources: cores={job['core']}, ram={job['ram']}, disk={job['disk']}, gpu={job['gpu']}")

if infeasible_jobs:
    print(f"\n{'='*80}")
    print("INFEASIBLE JOBS (CANNOT run on agents 9-30):")
    print(f"{'='*80}")
    for job in infeasible_jobs:
        print(f"  Job {job['job_id']}: feasible agents = {job['all_feasible_agents']}")
        print(f"    Resources: cores={job['core']}, ram={job['ram']}, disk={job['disk']}, gpu={job['gpu']}")

if not_found_jobs:
    print(f"\n{'='*80}")
    print(f"JOBS NOT FOUND IN CSV: {', '.join(not_found_jobs)}")
    print(f"{'='*80}")

print(f"\n{'='*80}")
print("SUMMARY:")
print(f"{'='*80}")
print(f"✓ Feasible:   {len(feasible_jobs)}/{len(target_jobs)} jobs")
print(f"✗ Infeasible: {len(infeasible_jobs)}/{len(target_jobs)} jobs")
if not_found_jobs:
    print(f"? Not found:  {len(not_found_jobs)}/{len(target_jobs)} jobs")
print()
