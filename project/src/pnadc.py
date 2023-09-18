import ftplib
import zipfile
import pandas as pd
from loguru import logger


HOST = "ftp.ibge.gov.br"
BASE_PATH = "Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados"
DOCUMENTATION_DIR_PATH = f"{BASE_PATH}/Documentacao"
PNADC_RAW_METADATA_FILENAME = "dicionario_PNADC_microdados_trimestral.xls"
PNADC_FORMATED_METADATA_FILEPATH = "data/pnadc_metadata.csv"

DICT_INIT_ROW = 3

def get_pnadc_metadata():
    dict_ftp_filepath = _get_dict_filepath()
    dict_local_filepath = f"data/{dict_ftp_filepath}"
    _download_file(ftp_path=DOCUMENTATION_DIR_PATH, ftp_filename = dict_ftp_filepath, output_path=dict_local_filepath)
    zip_file = zipfile.ZipFile(dict_local_filepath)
    filename = zip_file.extract(PNADC_RAW_METADATA_FILENAME, path="data")
    _format_dict(filename, output_filepath=PNADC_FORMATED_METADATA_FILEPATH)
    return PNADC_FORMATED_METADATA_FILEPATH


def get_pnadc(ano, trimestre):
    trimestre = str(int(trimestre)).zfill(2)
    
    filename_pattern = f"PNADC_{trimestre}{ano}"
    zip_filename = [x for x in _get_files_list(f"{BASE_PATH}/{ano}") if filename_pattern in x][-1]
    output_zip_filepath = f"data/{zip_filename}"
    txt_filename = f"{filename_pattern}.txt"
    
    logger.info(f"Downloading data from PNADC from '{BASE_PATH}/{ano}/{zip_filename}' and saving in '{output_zip_filepath}'")
    _download_file(f"{BASE_PATH}/{ano}", zip_filename, output_zip_filepath)
    
    logger.info(f"Extracting data from '{output_zip_filepath}' to 'data/{txt_filename}'")
    zip_file = zipfile.ZipFile(output_zip_filepath)
    pnadc_filepath = zip_file.extract(txt_filename, path="data")

    logger.info("Importing PNADC metadata")
    metadata = pd.read_csv(PNADC_FORMATED_METADATA_FILEPATH)
    
    logger.info(f"Reading PNADC dataframe from '{pnadc_filepath}'")
    pnadc = pd.read_fwf(pnadc_filepath, widths=metadata['tamanho'], header=None)
    pnadc.columns = metadata['codigo_da_variavel']

    pnadc_filename = f"data/pnadc_{trimestre}{ano}.parquet"
    logger.info(f"Export PNADC dataframe to '{pnadc_filename}'")
    pnadc.to_parquet(pnadc_filename, compression="brotli")

    logger.info(f"PNADC microdata can be accessed in {pnadc_filename}")

    return pnadc_filename

def _get_dict_filepath():
    ftp = ftplib.FTP(HOST)
    ftp.login()
    ftp.cwd(DOCUMENTATION_DIR_PATH)
    dir_list = []
    ftp.dir(dir_list.append)
    return [f.split()[-1] for f in dir_list if "Dicionario_e_input" in f][0]


def _download_file(ftp_path, ftp_filename, output_path):
    ftp = ftplib.FTP(HOST)
    ftp.login()
    ftp.cwd(ftp_path)
    with open(output_path, 'wb') as file :
        ftp.retrbinary('RETR %s' % ftp_filename, file.write)
    ftp.close()


def _format_dict(dict_filepath, output_filepath = PNADC_FORMATED_METADATA_FILEPATH):
    logger.info(f"Importing {dict_filepath}")
    df = pd.read_excel(dict_filepath).iloc[DICT_INIT_ROW:]

    df.columns = [
        "posicao_incial",
        "tamanho",
        "codigo_da_variavel",
        "quesito_n", 
        "quesito_descricao",
        "categorias_tipo",
        "categorias_descricao",
        "periodo"
    ]

    logger.info(f"Filtering rows")

    def _is_number(x):
        try:
            number = int(x)
            return True
        except:
            return False
    
    variaveis = df[~df["posicao_incial"].isna()]
    variaveis = variaveis[variaveis['posicao_incial'].apply(_is_number)]
    
    logger.info(f"Exporting formatted dataset to {output_filepath}")
    variaveis.to_csv(output_filepath, index=False)


def _get_files_list(path):
    ftp = ftplib.FTP(HOST)
    ftp.login()
    ftp.cwd(path)
    dir_list = []
    ftp.dir(dir_list.append)
    return [x.split()[-1] for x in  dir_list]
