import redis


def display_tasks(redis_host='localhost', redis_port=6379, task_list='tasks'):
    # Connect to the Redis database
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

    task_keys = redis_client.keys('*:*')  # Assuming task keys are prefixed with 'task:'
    for key in task_keys:
        data = redis_client.get(key)
        print(f"{key}: {data}")


if __name__ == "__main__":
    display_tasks()