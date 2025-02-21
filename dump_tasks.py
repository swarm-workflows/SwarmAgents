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
import sys


def display_tasks(redis_host='zoo-0', redis_port=6379, task_list='*'):
    """Connects to Redis and displays tasks stored in the specified task list."""
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

    task_keys = redis_client.keys(f"{task_list}:*")  # Filter keys based on task list prefix
    if not task_keys:
        print(f"No tasks found in queue '{task_list}'.")
        return

    for key in task_keys:
        data = redis_client.get(key)
        print(f"{key}: {data}")


if __name__ == "__main__":
    # Get queue name from command-line arguments (default to '*')
    queue_name = sys.argv[1] if len(sys.argv) > 1 else '*'

    display_tasks(task_list=queue_name)
