# Instruction

### Project Strucutre 

1. create `data` folder under the main dir : `.\westwell_da\data\`

2. create `raw` and `processed` folder under `data` folder: `.\westwell_da\data\raw` and `.\westwell_da\data\processed\'

2.2 (OPTIONAL) create `.\westwell_da\data\history\` to store past processed raw data

3. put all raw data from ATs into `raw` folder (all results would be shown in `processed` folder)

### File I/O

1. File input based on project indicator of your data folder name under `.\westwell_da\data\raw\data_folder\`

    e.g. A folder name of `ica19990101` is indicating data relevant to `ica` project on the date of 1999-01-01

2. Outputs will be automatically generated under folder `.\westwell_da\data\processed\project_name\`

### Run

1. open `RUN.py`

2. configure your `WEEK_NUM` (e.g. current week number) and `DATA_SRC` (e.g. IGV, TASK, ERROR)

3. run `RUN.py` 
