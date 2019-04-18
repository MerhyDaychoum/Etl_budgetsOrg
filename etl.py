from pydal import DAL, Field

def conexao():
    dbinfo = "mysql://alunos:alunos@localhost/uniorc"
    db = DAL(dbinfo, folder = './database', migrate = False)
    return db

db = conexao()
db.define_table("orgao", Field('id_orgao'), Field('nm_orgao'), primarykey = ['id_orgao'])

dados = set()
for i,linha in enumerate(open('2019_OrcamentoDespesa.csv')):
    if(i > 0):
       campos = linha.split(';')
       campo1 = campos[1][1:len(campos[1])-1]
       campo2 = campos[2][1:len(campos[2])-1]
       dado = campo1+';'+campo2
       dados.add(dado)

tratados = list()
for l2 in dados:
    c2 = l2.split(';')
    tratados.append({'id_orgao':int(c2[0]), 'nm_orgao':c2[1]})
    c3 = l2.split(';')
    tratados.append({'id_orgaoSubordinado': int(c3[0]), 'nm_orgaoSubordinado': c3[1]})
    c4 = l2.split(';')
    tratados.append({'id_orgaoUndOrcamentaria': int(c4[0]), 'nm_orgaoUndOrcamentaria': c4[1]})
    c5 = l2.split(';')
    tratados.append({'id_funcao': int(c5[0]), 'nm_funcao': c5[1]})
    c6 = l2.split(';')
    tratados.append({'id_subFuncao': int(c6[0]), 'nm_subFuncao': c6[1]})
    c7 = l2.split(';')
    tratados.append({'id_progOrcamentario': int(c7[0]), 'nm_progOrcamentario': c7[1]})
    c8 = l2.split(';')
    tratados.append({'id_acao': int(c8[0]), 'nm_acao': c8[1]})
    c9 = l2.split(';')
    tratados.append({'id_categoriaEconomica': int(c9[0]), 'nm_categoriaEconomica': c9[1]})
    c10 = l2.split(';')
    tratados.append({'id_grupoDespesa': int(c10[0]), 'nm_grupoDespesa': c10[1]})
    c11 = l2.split(';')
    tratados.append({'id_elementoDespesa': int(c11[0]), 'nm_elementoDespesa': c11[1]})
    c12 = l2.split(';')
    tratados.append({'nm_orcamentoInicial': float(c12[0]),
                     'nm_orcamentoAtualizado': float(c12[1]),
                     'nm_orcamentoRealizado': float(c12[2])})

db.orgao.truncate()
db.orgao.bulk_insert(tratados)
db.commit()
rows = db(db.orgao.id_orgao != None).select()
db(db.orgao.id_orgao,)
for row in rows:
    print(row.id_orgao, row.nm_orgao)
    print(row.id_orgaoSubordinado, row.nm_orgaoSubordinado)
    print(row.id_orgaoUndOrcamentaria, row.nm_orgaoUndOrcamentaria)
    print(row.id_funcao, row.nm_funcao)
    print(row.id_subFuncao, row.nm_subFuncao)
    print(row.id_progOrcamentario, row.nm_progOrcamentario)
    print(row.id_acao, row.nm_acao)
    print(row.id_categoriaEconomica, row.nm_categoriaEconomica)
    print(row.id_grupoDespesa, row.nm_grupoDespesa)
    print(row.id_elementoDespesa, row.nm_elementoDespesa)
    print(row.nm_orcamentoInicial, row.nm_orcamentoAtualizado, row.nm_orcamentoRealizado)
