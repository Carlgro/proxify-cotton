from fetch_data import get_request
from models import JobParameters, PostItems, UsersItems
from job import Job
from typing import List, Union
from yaml import safe_load

def get_args(yaml_path: str):
    with open(yaml_path) as stream:
        return safe_load(stream)


def get_data(url: str, model: Union[PostItems, UsersItems]) -> List[dict]:
    data = get_request(url)
    model(items=data)
    return data


if __name__ == __name__:
    input_args = get_args("jobs.yaml")
    JobParameters(**input_args)
    
    schema_name = input_args['schema_name']
    job = input_args['jobs']
    posts = job.get('posts')
    users = job.get('users')
    
    if posts:
        job = Job(
            duckdb_schema=PostItems.duckdb_schema(),
            pyarrow_schema=PostItems.pyarrow_schema(),
            schema_name=schema_name,
            table_name=posts['table_name'],
            data=get_data(posts.get('url'), PostItems),
            queries=posts['queries'],
        )

        job.run()

    print('\ndone 🚀 🚀')
