from modules.data_pipeline import export_file

WEEK_NUM = 'June'
DATA_SRC = 'IGV'

export_file.export_to_csv(WEEK_NUM, DATA_SRC)