from pyspark import SparkContext
import json
import time

def uniWord(email):
    output = []
    for x in email['text'].split(' '):
        if x not in output:
            output.append(x)
    return output
    
def allWord(email):
    output = []
    for x in email['text'].split(' '):
        output.append(x)
    return output
    

print 'loading'
sc = SparkContext("spark://ec2-54-205-79-23.compute-1.amazonaws.com:7077", "Simple App")
# Replace `lay-k.json` with `*.json` to get a whole lot more data.
lay = sc.textFile('s3n://AKIAJFDTPC4XX2LVETGA:lJPMR8IqPw2rsVKmsSgniUd+cLhpItI42Z6DCFku@6885public/enron/lay-k.json')

json_lay = lay.map(lambda x: json.loads(x)).cache()
print 'json lay count', json_lay.count()

filtered_lay = json_lay.filter(lambda x: 'chairman' in x['text'].lower())
print 'lay filtered to chairman', filtered_lay.count()

#idf
word_list = json_lay.flatMap(uniWord)
idf = word_list.countByValue()

#print 'counted_idf', list(idf)
for x in list(idf):
    print idf(x)
#tf
emails_jay = json_lay.filter(lambda x: 'kenneth.lay@enron.com' in x['sender'].lower())
emails_jay2  = json_lay.filter(lambda x: 'kenneth.lay@enron.com' in x['sender'].lower() or 'rosalee.fleming@enron.com' in x['sender'].lower())
emails_fastow = json_lay.filter(lambda x: 'andrew.fastow@enron.com' in x['sender'].lower())
tf_jay = emails_jay.flatMap(allWord).countByValue()
tf_jay2 = emails_jay.flatMap(allWord).countByValue()
tf_fastow = emails_fastow.flatMap(allWord).countByValue()
print 'lay filtered1', emails_jay.count()
print 'lay filtered2', emails_jay2.count()




to_list = json_lay.flatMap(lambda x: x['to'])
print 'to_list', to_list.count()

counted_values = to_list.countByValue()
# Uncomment the next line to see a dictionary of every `to` mapped to
# the number of times it appeared.
#print 'counted_values', counted_values

# How to use a join to combine two datasets.
frequencies = sc.parallelize([('a', 2), ('the', 3)])
inverted_index = sc.parallelize([('a', ('doc1', 5)), ('the', ('doc1', 6)), ('cats', ('doc2', 1)), ('the', ('doc2', 2))])

# See also rightOuterJoin and leftOuterJoin.
join_result = frequencies.join(inverted_index)

# If you don't want to produce something as confusing as the next
# line's [1][1][0] nonesense, represent your data as dictionaries with
# named fields :).
multiplied_frequencies = join_result.map(lambda x: (x[0], x[1][1][0], x[1][0]*x[1][1][1]))
print 'term-document weighted frequencies', multiplied_frequencies.collect()