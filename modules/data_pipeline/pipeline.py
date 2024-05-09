import os
import pandas as pd
import psutil, time
from modules.data_pipeline import get_path, process_data, export_file


def pipe(week_num=int, data_src=str):
    # record cpu status and start time
    cpu_bf = psutil.cpu_percent()
    start = time.time()
    export_dict = {}

    # Get dict with path and project names
    path_dict = get_path.get(week_num=week_num, data_src=data_src)

    for project, file_path_dict in path_dict.items():
        vessel_name = None
        groupped_df = {}

        # create export folder
        export_folder_name = export_file.__create_folder__(
                                week_num=week_num, 
                                data_src=data_src, 
                                roject=project)

        for folder_name, file_path in file_path_dict.items():
            if process_data.__is_chinese__(folder_name):
                vessel_name = folder_name
            
            # generate ecport path
            export_file_path = export_file.__create_single_path__(data_src=data_src, 
                                                                  export_folder_name=export_folder_name,
                                                                  folder_name=folder_name)
            if os.path.exists(export_file_path):
                print(f'Already processed and exported to: {export_file_path}, move to the next.')

                # Add processed df for weekly file export
                processed_df = pd.read_csv(export_file_path, index_col=0, encoding='utf-8-sig')
                if folder_name not in groupped_df:
                    groupped_df[folder_name] = [processed_df]
                else:
                    groupped_df[folder_name].append(processed_df)
            else:
                # Process raw data
                groupped_df = process_data.process(week_num=week_num, data_src=data_src, project=project,
                        vessel_name=vessel_name, folder_name=folder_name,
                        file_path=file_path, groupped_df=groupped_df)
                
        groupped_df = process_data.__concat_groupped_df__(groupped_df)
        export_dict[project] = groupped_df


    # record after 
    end = time.time()
    cpu_af = psutil.cpu_percent()

    # cal CPU usage and time diff
    cpu_usage = cpu_af - cpu_bf
    time_usage= end - start

    print(f'CPT usage: {cpu_usage:.2f}%')
    print(f'Running time: {time_usage:.2f} seconds\n')
    return