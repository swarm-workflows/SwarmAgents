import redis
import json

def display_tasks(redis_host='localhost', redis_port=6379, task_list='tasks'):
    # Connect to the Redis database
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

    task_keys = redis_client.keys('task:*')  # Assuming task keys are prefixed with 'task:'
    tasks = []
    for key in task_keys:
        data = redis_client.get(key)
        print(f"{key}: {data}")
    ''' 
    # Retrieve all tasks from the specified Redis list
    tasks = redis_client.lrange(task_list, 0, -1)
    
    if not tasks:
        print("No tasks found in the Redis list.")
        return
    
    print("Tasks in Redis:")
    for task in tasks:
        task_data = json.loads(task)
        print(task_data)
    '''

if __name__ == "__main__":
    display_tasks()

