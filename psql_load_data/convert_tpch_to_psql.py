import os

dir_path = os.path.dirname(os.path.realpath(__file__))
dirs = os.listdir(dir_path)
for query in [d for d in dirs if d.endswith('.sql')]:
    file = open(query, 'r')
    with open('edited/' + query, "wb") as output:
        for l in file:
            if 'psql.out' in l:
                output.write('\o ' + query + '.out\n')
                output.write('explain analyze\n')
            elif ';' in l:
                l = l.replace(';', '')
                if 'where rownum' in l:
                    temp = l.split()[-1]
                    if temp == '-1':
                        temp = '1'
                    temp = 'limit ' + temp + ';'
                    output.write(temp)
                else:
                    output.write(l)
            else:
                output.write(l)
