## How to install
### Install Python interpreter
Program was tested with Python version 3.13

### Create virtual environment and install dependencies
#### linux
```
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
```
#### windows
```
python -m venv venv
.\venv\Scripts\activate
python -m pip install -r requirements.txt
```

#### create .env file with redis credentials
```
REDIS_HOST=localhost
REDIS_PORT=6379
```

## How to run Tests
```
pytest 
pytest -s # show program stdout
pytest ./tests/test_insertion.py # run only tests from specified file 
```
## How to run benchmarks
benchmarks need to be executed with -m and file path translated to module name, due to the way python imports work  
```
# run insert benchmark using transactional algorithm (alternative is redis_script), inserting 10000 rows, using 1 process and having only 5 different values in functional dependency field
python3 -m benchmarks.benchmark_inserts transactional 10000 1 5 

# run select benchmark with nested loop algorithm, with table sizes of 100 and 1000, doing 1 select
python3 -m benchmarks.benchmark_nested_loop_selects 100 1000 1

python3 -m benchmarks.benchmark_primary_key_join_selects 100 1000 1
```
