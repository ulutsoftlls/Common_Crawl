import pandas as pd

import requests
import os
import pandas as pd
import io
import time
import pyarrow.parquet as pq
import gc


# filename = "links.txt"  # Replace with your file path
#
# # Open the file and read it line by line
# with open(filename, "r") as file:
#     for line in file:
#         # Process each line as needed
#         print(line.strip())

# Путь к файлу с ссылками
file_path = "/home/bektemir/Desktop/my_projects/Common_Crawl/cc-index-table.paths"

# Папка для сохранения скачанных файлов
download_folder = "/home/bektemir/Desktop/my_projects/Common_Crawl/download"

# Создаем папку, если ее нет
os.makedirs(download_folder, exist_ok=True)

# Создаем пустой DataFrame для хранения фильтрованных данных
all_filtered_data = pd.DataFrame()

# Чтение ссылок из файла
with open(file_path, 'r') as file:
    links = file.read().splitlines()

# Базовая часть URL-а
base_url = "https://data.commoncrawl.org/"

# Цикл для обработки каждой ссылки
i = 0
for link in links:
    i += 1
    if i < 895:

        print(i)
        continue
    # Формируем полный URL
    full_url = f"{base_url}{link}"

    # Загрузка файла Parquet
    response = requests.get(full_url, stream=True)

    if response.status_code == 200:
        file_path = './parquets/output' + str(i) + '.parquet'
        # content = response.content  # Ваш код для обработки контента
        with open(file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=128):
                file.write(chunk)
        del response
        gc.collect()
        df = pd.read_parquet(full_url)
        filtered_df = df[df['content_languages'].str.startswith('kir', na=False)]
        all_filtered_data = pd.concat([all_filtered_data, filtered_df], ignore_index=True)
        print(all_filtered_data)
        all_filtered_data.to_csv('/home/bektemir/Desktop/my_projects/Common_Crawl/download/all_filtered_data.csv', index=False)
        del all_filtered_data
        del df
        gc.collect()
        print('success')
    else:
        print(f"Не удалось скачать файл {full_url}. Статус код: {response.status_code}")

    # Задержка между запросами (по вашему выбору)
    time.sleep(3)  # Например, 1 секунда

# Сохранение общего DataFrame в один CSV файл



# filename = 'part-00292-e565b809-b335-4c1d-90fd-54a9a2b7113d.c000.gz.parquet'
# df = pd.read_parquet(filename)
# df = df[df['content_languages'].str.startswith('kir', na=False)]
# df.to_csv('telugu.csv')
