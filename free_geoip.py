import requests as req
import pandas as pd
import json


#df = pd.read_json('http://rest.db.ripe.net/search.json?query-string=193.169.135.170&amp;flags=no-filtering')

aaa = req.get('http://rest.db.ripe.net/search.json?query-string=193.169.135.170&amp;flags=no-filtering')

print(type(aaa))
print(type(aaa.text))
jj = json.loads(aaa.text)




ooo = jj['objects']
print((ooo.items()))
print('===============================')
for iii in ooo.items():
    print('iii', type(iii))
    print(iii)

    print('====>', iii[1])
    print('===============================')
    #print('+++++++>', (iii[1].get('type', 'person')))
    for ii in iii[1]:
        print('ii', (ii))
        print('============>', (ii.get('type', 'person')))

print('===============================')
#print(aaa.text)
