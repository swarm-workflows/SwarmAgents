# MIT License
#
# Copyright (c) 2024 swarm-workflows

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)
import redis
import argparse


def display_tasks(redis_host='localhost', redis_port=6379, task_list='*', count=False):
    """Connects to Redis and displays tasks or task count from the specified task list."""
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

    task_keys = redis_client.keys(f"{task_list}:*")  # Filter keys based on task list prefix

    if not task_keys:
        print(f"No tasks found in queue '{task_list}'.")
        return

    if count:
        print(f"Total tasks in '{task_list}': {len(task_keys)}")
    else:
        for key in task_keys:
            data = redis_client.get(key)
            print(f"{key}: {data}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Display Redis task queue or count.")
    parser.add_argument("--host", default="localhost", help="Redis host (default: localhost)")
    parser.add_argument("--key", default="*", help="Task key prefix to match (default: *)")
    parser.add_argument("--count", action="store_true", help="Only display the count of entries")

    args = parser.parse_args()
    display_tasks(redis_host=args.host, task_list=args.key, count=args.count)
