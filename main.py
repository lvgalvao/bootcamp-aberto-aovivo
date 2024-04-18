from time import sleep

def primeira_atividade():
    print("primeira atividade rodou com sucesso")
    sleep(2)

def segunda_atividade():
    print("segunda atividade rodou com sucesso")
    sleep(2)

def tercera_atividade():
    print("tercera atividade rodou com sucesso")
    sleep(2)

def pipeline():
    primeira_atividade()
    segunda_atividade()
    tercera_atividade()

pipeline()