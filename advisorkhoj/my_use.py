import json
with open("api_responses.txt","r") as file:
    data=file.read()
python_list = json.loads(data)
print(python_list[0])
with open(output_file, 'w') as file:
    json.dump(python_list, file)
