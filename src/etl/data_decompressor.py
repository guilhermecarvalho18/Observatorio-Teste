import os
import zipfile

def decompress_zip_files(source_dir, dest_dir):
    """
    Descompacta todos os arquivos ZIP do diretório source_dir para dest_dir.

    Args:
        source_dir (str): Diretório onde estão os arquivos ZIP.
        dest_dir (str): Diretório de destino para os arquivos descompactados.
    """
    os.makedirs(dest_dir, exist_ok=True)
    for file in os.listdir(source_dir):
        if file.lower().endswith(".zip"):
            nome, ext = os.path.splitext(file)
            if len(nome) == 4:
                file_path = os.path.join(source_dir, file)
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(dest_dir)
                print(f"Descompactado: {file} para {dest_dir}")
            else:
                print(f"Ignorado: {file} (não é o arquivo completo)")

if __name__ == "__main__":
    # Cria diretórios temporários para fonte e destino dos arquivos ZIP
    source_dir = "datalake/raw"
    dest_dir = source_dir + "/unzipped"
    print("Diretório de origem:", source_dir)
    print("Diretório de destino:", dest_dir)

    # Executa a função de descompactação
    print("\nExecutando a descompactação:")
    decompress_zip_files(source_dir, dest_dir)