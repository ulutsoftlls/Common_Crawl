import pandas as pd

import requests
import os
import pandas as pd
import io
import time

# filename = "links.txt"  # Replace with your file path
#
# # Open the file and read it line by line
# with open(filename, "r") as file:
#     for line in file:
#         # Process each line as needed
#         print(line.strip())

# Путь к файлу с ссылками
file_path = "/Users/zarinamacbook/Desktop/Datasets/archives/2023-50/cc-index-table.paths copy"

# Папка для сохранения скачанных файлов
download_folder = "/Users/zarinamacbook/Desktop/Datasets/archives/2023-50/downloaded_files"

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
for link in links:
    # Формируем полный URL
    full_url = f"{base_url}{link}"

    # Загрузка файла Parquet
    response = requests.get(full_url)

    if response.status_code == 200:
        content = response.content  # Ваш код для обработки контента
        # Пример обработки контента: извлечение данных из Parquet
        # Ниже предполагается, что у вас есть столбец 'content_languages' в Parquet
        df = pd.read_parquet(io.BytesIO(content))

        # Фильтрация данных
        filtered_df = df[df['content_languages'].str.startswith('kir', na=False)]

        # Добавление фильтрованных данных к общему DataFrame
        all_filtered_data = pd.concat([all_filtered_data, filtered_df], ignore_index=True)

        print(f"Файл {full_url} успешно обработан.")
    else:
        print(f"Не удалось скачать файл {full_url}. Статус код: {response.status_code}")

    # Задержка между запросами (по вашему выбору)
    time.sleep(3)  # Например, 1 секунда

# Сохранение общего DataFrame в один CSV файл
all_filtered_data.to_csv(os.path.join(download_folder, '/Users/zarinamacbook/Desktop/Datasets/archives/2023-50/all_filtered_data.csv'), index=False)
print("Все данные успешно сохранены в один CSV файл.")


# filename = 'part-00292-e565b809-b335-4c1d-90fd-54a9a2b7113d.c000.gz.parquet'
# df = pd.read_parquet(filename)
# df = df[df['content_languages'].str.startswith('kir', na=False)]
# df.to_csv('telugu.csv')