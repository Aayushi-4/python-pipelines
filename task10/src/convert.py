import json

with open(r"C:\Users\Aayushi\Downloads\project.txt",'r') as pf:
    json_data=pf.read()
pf=json.loads(json_data)
with open('project.json','w') as we:
    json.dump(pf,we,indent=4,sort_keys=False)