import pandas as pd
import requests
import os
import io
import time

# Папка, содержащая файлы с ссылками
folder_path = "/Users/zarinamacbook/Desktop/Datasets/archives/2023-50/split_files"

# Папка для сохранения скачанных файлов
download_folder = "/Users/zarinamacbook/Desktop/Datasets/archives/2023-50/downloaded_files"

# Создаем папку, если ее нет
os.makedirs(download_folder, exist_ok=True)

# Базовая часть URL-а
base_url = "https://data.commoncrawl.org/"

# Получаем список файлов с ссылками
link_files = [file for file in os.listdir(folder_path) if file.startswith("2023_50_")]
link_files.sort()

# Создаем пустой DataFrame для хранения фильтрованных данных
all_filtered_data = pd.DataFrame()

# Цикл для обработки каждого файла с ссылками
for link_file in link_files:
    print(link_file)
    # Полный путь к файлу с ссылками
    file_path = os.path.join(folder_path, link_file)

    # Папка для сохранения CSV файла
    csv_folder = os.path.join(download_folder, link_file.replace(".path", ""))
    os.makedirs(csv_folder, exist_ok=True)

    # Чтение ссылок из файла
    with open(file_path, 'r') as file:
        links = file.read().splitlines()

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

            # Сохранение фильтрованного DataFrame в CSV файл
            csv_file_path = os.path.join(csv_folder, f"{link.replace('.path', '.csv')}")
            filtered_df.to_csv(csv_file_path, index=False)

            print(f"Файл {full_url} успешно обработан и сохранен в {csv_file_path}.")

            # Добавление фильтрованных данных к общему DataFrame
            all_filtered_data = pd.concat([all_filtered_data, filtered_df], ignore_index=True)
        else:
            print(f"Не удалось скачать файл {full_url}. Статус код: {response.status_code}")

        # Задержка между запросами (по вашему выбору)
        time.sleep(3)  # Например, 3 секунды

# Сохранение общего DataFrame в один CSV файл
all_filtered_data.to_csv(os.path.join(download_folder, 'all_filtered_data.csv'), index=False)
print("Все данные успешно сохранены в один CSV файл.")
