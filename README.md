# proxify-cotton

##### design:

1. Decided to use yaml file as a job config manager.
    1.1 This file is responsable of defining configuration for the job. 
2. Split the code into modules.
    2.1 main: where the main code live. 
    2.2 job: stores the Job class, responsable for defining the steps that are inside the run() method. 
    2.3 models: where the model schema definition are. 
    2.4 fetch_data: where functions to call external sources live. 
3. DuckDB + Pyarrow:
    3.1 Decided to use Duckdb and pyarror in order to run all in memory, as data is small size,everything can be handled just in memory. 

##### run:
1. poetry shell
2. poetry install.
3. python src/main.py


##### questions:
1. Number of posts per user.
2. User with le longest post. 
3. User with the latest post (based on post_id). 

##### WIP:
1. Users post its a WIP but will follow the same logic of posts. 
