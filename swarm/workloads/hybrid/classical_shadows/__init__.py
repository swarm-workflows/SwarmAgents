"""
Classical shadows workload for Swarm 
Author: Vincenzo De Maio (v.demaio@leicester.ac.uk)

This package implements the classical-shadow workload for SwarmAgents.

Classical shadows are a quantum-information technique that allows
expectation values of multiple observables to be estimated from a
collection of randomized quantum measurements.

The workload is organized into several stages:

1. Job construction:
   Divide the overall computation into independent batches.

2. Batch execution:
   Process the measurement data associated with each batch and
   calculate partial observable estimates.

3. Aggregation:
   Combine the partial estimates into final estimates.

4. Workflow orchestration:
   Describe the complete workload and its job dependencies.

The package is designed to integrate with SwarmAgents' distributed
job scheduling and execution infrastructure.

The actual classical-shadow algorithm is kept separate from the
SwarmAgents scheduling logic. This allows different algorithm
implementations to be used without changing the distributed
execution infrastructure.

"""

# Import the public job-construction functions
from .jobs import (
    create_shadow_batch_jobs,
    create_shadow_aggregation_job
)

# Import the workflow constructor
from .workflow import build_classical_shadows_workflow
# Import the batch execution adapter
from .worker import execute_shadow_batch
# Import the result aggregation function
from .aggregation import aggregate_shadow_result

# Define the public API of the package
__all__ = [
    "create_shadow_batch_jobs",
    "create_shadow_aggregation_job",
    "build_classical_shadows_workflow",
    "execute_shadow_batch",
    "aggregate_shadow_results"
]