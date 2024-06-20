import json
import random
import multiprocessing as mp

# in order to achieve uniqness withot use of any if statements
# we can use a set of tuples
# and then convert it back to a list of dictionaries
def remove_duplicates(employees):
    unique_employees_set = {tuple(emp.items()) for emp in employees}
    unique_employees = [dict(t) for t in unique_employees_set]
    return unique_employees

# I changed the aproach from what we discussed in the meeting, 
# since it makes more sence in python and should be more efficient
def proccess_chunk(chunk):
    random.shuffle(chunk)
    # create a list of shifted values by 1
    offset = [chunk[-1]] + chunk[:-1]
    result = [(dwarf["name"], giant["name"]) for dwarf, giant in zip(chunk, offset)]
    return result

with open("data.json") as f:
    data = json.load(f)
    num_processes = mp.cpu_count()
    unique_items = remove_duplicates(data)

    # chunk the data
    chunks = [ unique_items[i::num_processes] for i in range(num_processes)]
    with mp.Pool(num_processes) as p:
        mapped_results = p.map(proccess_chunk, chunks)

    # merge the results
    results = [item for sublist in mapped_results for item in sublist]

    print(results, len(results), len(unique_items))