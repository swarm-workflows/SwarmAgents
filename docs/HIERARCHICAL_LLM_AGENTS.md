# Hierarchical Topology with LLM Agents at Level 1

This guide explains how to run SwarmAgents in hierarchical mode with LLM agents at Level 1 and resource agents at Level 0.

## Overview

In hierarchical topology:
- **Level 0**: Leaf agents organized into groups (default: 10 groups of 5 agents each for 50+ agents)
- **Level 1**: Parent/coordinator agents (one per group) that coordinate the groups

With this implementation, Level 1 agents can be automatically configured as LLM agents while Level 0 agents remain as resource agents.

## How It Works

The system now supports per-agent type configuration through the following mechanism:

1. **Config Generation** (`generate_configs.py:564-568`): Automatically sets `agent_type: llm` for Level 1 agents in hierarchical topology
2. **Agent Startup** (`swarm-multi-start.sh:136-137`): Reads `agent_type` from each config file when using `--use-config-dir`
3. **Main Entry Point** (`main.py:61-69`): Falls back to config file if `agent_type` not provided via CLI

## Quick Start

### Prerequisites

- Minimum 30 agents for hierarchical topology
- OpenAI API key OR Ollama running locally
- Redis running (for shared state)

### 1. Set up LLM provider

**Option A: OpenAI**
```bash
export OPENAI_API_KEY=sk-your-api-key-here
```

**Option B: Ollama (local)**
```bash
# Start Ollama
ollama serve

# In another terminal, export the base URL
export LLM_BASE_URL=http://localhost:11434/v1
```

### 2. Run with automatic LLM configuration

**Using run_test_v2.py (recommended):**
```bash
cd SwarmAgents

python run_test_v2.py \
    --mode local \
    --agent-type resource \
    --agents 30 \
    --topology hierarchical \
    --jobs 500 \
    --db-host localhost \
    --jobs-per-proposal 10 \
    --run-dir runs/hierarchical-llm
```

**Using run_test.py:**
```bash
cd SwarmAgents

python run_test.py \
    --agent-type resource \
    --agents 30 \
    --topology hierarchical \
    --jobs 500 \
    --db-host localhost \
    --jobs-per-interval 20 \
    --run-dir runs/hierarchical-llm
```

**Using swarm-multi-start.sh directly:**
```bash
cd SwarmAgents

# Generate configs first
python3.11 generate_configs.py \
    30 10 config_swarm_multi.yml configs/ hierarchical localhost 500

# Start agents (will automatically use LLM for Level 1)
bash swarm-multi-start.sh resource 30 hierarchical 500 localhost 10 --use-config-dir

# Distribute jobs
python3.11 job_distributor.py \
    --jobs-dir jobs \
    --jobs-per-interval 20 \
    --redis-host localhost \
    --level 1
```

## Agent Distribution

For 30 agents in hierarchical mode:
- **Agents 1-25**: Level 0 (resource agents) in 5 groups of 5
- **Agents 26-30**: Level 1 (LLM agents) - one coordinator per group

For 110 agents in hierarchical mode:
- **Agents 1-100**: Level 0 (resource agents) in 10 groups of 10
- **Agents 101-110**: Level 1 (LLM agents) - one coordinator per group

## Verification

You can verify the configuration is correct by checking a generated config file:

```bash
# Check a Level 0 agent (should be 'resource')
python3.11 -c "import yaml; c=yaml.safe_load(open('configs/config_swarm_multi_1.yml')); print(f'Agent 1: type={c[\"agent_type\"]}, level={c[\"topology\"][\"level\"]}')"
# Output: Agent 1: type=resource, level=0

# Check a Level 1 agent (should be 'llm')
python3.11 -c "import yaml; c=yaml.safe_load(open('configs/config_swarm_multi_26.yml')); print(f'Agent 26: type={c[\"agent_type\"]}, level={c[\"topology\"][\"level\"]}')"
# Output: Agent 26: type=llm, level=1
```

Or run the automated test:
```bash
bash test_hierarchical_llm.sh
```

## Advanced Usage

### Manual Agent Type Override

You can still manually override the agent type via CLI:

```bash
# Start agent 26 as a resource agent (override config)
python3.11 main.py 26 --agent-type resource
```

## Configuration Details

### Config File Structure

Each generated config now includes an `agent_type` field:

```yaml
agent_type: llm  # or 'resource'
capacities:
  core: 8
  ram: 32
  disk: 500
  gpu: 4
topology:
  type: hierarchical
  level: 1
  peer_agents: [27, 28, 29, 30]
  parent: null
  children: [0]
  group: 0
  group_size: 5
  group_count: 5
# ... rest of config
```

### LLM Agent Configuration

LLM-specific settings are controlled in the base config file (`config_swarm_multi.yml`):

```yaml
llm:
  provider: "openai"  # or set to "none" to disable
  model: "gpt-4"      # Any OpenAI-compatible model
  use_for_selection: true  # Enable LLM-based tie-breaking
```

## Remote Mode

The same approach works for remote distributed deployments:

```bash
python run_test_v2.py \
    --mode remote \
    --agent-type resource \
    --agents 110 \
    --agents-per-host 10 \
    --topology hierarchical \
    --jobs 1000 \
    --db-host 10.0.0.5 \
    --agent-hosts-file hosts.txt \
    --run-dir runs/remote-hierarchical-llm
```

Each remote host will automatically start the correct agent types based on the configs.