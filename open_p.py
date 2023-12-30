import pandas as pd

# Чтение файла Parquet
df = pd.read_parquet('output.parquet')

# Отображение первых нескольких строк
print(df.head())

# Фильтрация данных
# filtered_df = df[df['column_name'] > 10]