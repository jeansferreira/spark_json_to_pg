sample_object = {'Name':'John', 'Location':{'City':'Los Angeles','State':'CA'}}

from pandas.io.json import json_normalize
json_normalize(sample_object)

sample_object = [
	{
		"state": "Florida",
		"shortname": "FL",
		"info": {
			"governor": "Rick Scott"
		},
		"counties": [
			{
				"name": "Dade",
				"population": 12345
			},
			{
				"name": "Broward",
				"population": 40000
			},
			{
				"name": "Palm Beach",
				"population": 60000
			}
		]
	},
	{
		"state": "Ohio",
		"shortname": "OH",
		"info": {
			"governor": "John Kasich"
		},
		"counties": [
			{
				"name": "Summit",
				"population": 1234
			},
			{
				"name": "Cuyahoga",
				"population": 1337
			}
		]
	}
]

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

flat = flatten_json(sample_object)
print(flat)
df = json_normalize(flat)
print(df)
