# Script de Descompactação de Arquivos ZIP

Este script tem como objetivo descompactar os arquivos ZIP que estão localizados em um diretório de origem e extrair seu conteúdo para um diretório de destino. Ele foi projetado para processar somente os arquivos ZIP cujo nome (sem a extensão) possua exatamente 4 caracteres, ignorando aqueles que não atendam a essa condição.

---

## Funcionalidade

- **Descompactação Condicional**:  
  O script itera sobre todos os arquivos no diretório de origem e verifica se o arquivo possui a extensão `.zip` e se o nome do arquivo (sem extensão) possui exatamente 4 caracteres.  
  - Se a condição for satisfeita, o arquivo é descompactado para o diretório de destino.
  - Caso contrário, o arquivo é ignorado e uma mensagem informando a razão é exibida.

---

## Argumentos e Parâmetros

- **source_dir (string)**:  
  Diretório de onde os arquivos ZIP serão lidos.  
  *Valor padrão*: `"datalake/raw/zipped"`

- **dest_dir (string)**:  
  Diretório onde os arquivos descompactados serão armazenados.  
  *Valor padrão*: `"datalake/raw/unzipped"`

---

## Funcionamento

1. **Criação do Diretório de Destino**:  
   Utiliza a função `os.makedirs` com a opção `exist_ok=True` para criar o diretório de destino se ele ainda não existir.

2. **Iteração sobre os Arquivos ZIP**:  
   Para cada arquivo encontrado no diretório de origem:
   - Verifica se o nome do arquivo termina com ".zip" (insensível a maiúsculas/minúsculas).
   - Se for um arquivo ZIP, separa o nome da extensão usando `os.path.splitext`.

3. **Verificação do Tamanho do Nome**:  
   Se o nome do arquivo (sem extensão) tiver exatamente 4 caracteres:
   - O script constrói o caminho completo do arquivo.
   - Abre o arquivo ZIP utilizando a biblioteca `zipfile`.
   - Extrai todos os arquivos contidos no ZIP para o diretório de destino.
   - Imprime uma mensagem indicando que o arquivo foi descompactado.
   
   Caso o nome não tenha 4 caracteres, o arquivo é ignorado e é exibida uma mensagem informando que o arquivo não é o "arquivo completo".

---

## Exemplo de Execução

Ao executar o script diretamente, ele exibirá os diretórios de origem e destino e, em seguida, executará a função de descompactação:

```bash
python src/etl/decompress_zip_files.py
```

**Saída Esperada:**

Diretório de origem: datalake/raw/zipped  
Diretório de destino: datalake/raw/unzipped  

Executando a descompactação:  
Descompactado: 2021.zip para datalake/raw/unzipped  
Ignorado: exemplo.zip (não é o arquivo completo)  
...
