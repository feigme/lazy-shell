import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


filepath = [
    '/Users/guohui/Downloads/dwd_biz_contract_d__aa84e33f_42d5_4bee_8ef9_deacc62c5f97',
    '/Users/guohui/Downloads/dwd_biz_contract_d__04baa43a_9607_4a8d_95f4_03e7403b5794'
]

def checkColumn(path):
    # 读取 Parquet 文件的元数据
    metadata = pq.read_metadata(path)
    
    # 获取列信息
    schema = metadata.schema
    columns = schema.names

    wrongColumns = []
    # 遍历每一列并尝试读取数据
    for column in columns:
        try:
            # 读取列数据
            column_data = pq.read_table(path, columns=[column])
        
            # 如果读取成功，则不会抛出异常
            # print(f"列 '{column}' 读取成功")
        except Exception as e:
            print(f"列 '{column}', 读取失败: {e}")
            wrongColumns.append(column)
    return wrongColumns        


def changeSchema(path):
    # 读取原始 Parquet 文件的 schema
    original_schema = pq.read_schema(path)

    # 去除所有字段的 required 属性
    fields = []
    for field in original_schema:
        new_field = pa.field(field.name, field.type, nullable=True)
        fields.append(new_field)

    new_schema = pa.schema(fields)

    # 使用修改后的 schema 重新写入 Parquet 文件
    table = pq.read_table(path)
    table = table.cast(new_schema)
    pq.write_table(table, '/Users/guohui/Downloads/dwd_biz_contract_d_parquet')


def writeParquet():
    # 创建示例数据
    data = {
        'A': [1, 2, 3, 4],
        'B': ['foo', 'bar', 'foo', 'bar'],
        'C': [1.1, 2.2, 3.3, 4.4]
    }

    df = pd.DataFrame(data)

    # 将数据保存为 Parquet 文件
    df.to_parquet('/Users/guohui/Downloads/example_1.parquet')


def rewrite(path):
    # 读取 Parquet 文件
    table = pq.read_table(path)

    # 将表格数据转换为 Pandas DataFrame
    df = table.to_pandas()

    # 处理包含 null 值的必需列
    fixed_df = df.fillna("N/A")

    # 将修复后的数据写入新的 Parquet 文件
    fixed_table = pa.Table.from_pandas(fixed_df, schema=table.schema)
    
    # 将修复后的数据写入新的 Parquet 文件
    pq.write_table(fixed_table, '/Users/guohui/Downloads/dwd_biz_contract_d_parquet')

def readRow(path, rowNum):
    # 读取整个 Parquet 文件
    df = pd.read_parquet(path, columns=['id'])

    # 选择指定行的数据
    specific_row = df.iloc[rowNum]  # 选择第 50927 行的数据，行号从 0 开始计数

    # 打印所选行的数据
    print(specific_row)

def readFile(path):
    # 读取整个 Parquet 文件
    df = pd.read_parquet(path)
    print(df)


def writeFile1():
    # 创建包含 required 字段的 schema
    schema = pa.schema([
        ('name', pa.string(), True),  # 指定 required 字段为 false
        ('age', pa.int32(), False)
    ])

    # 创建要写入的数据
    data = [
        {'name': 'Alice', 'age': 7},
        {'name': None, 'age': 7}  # 包含 null 值的数据
    ]

    # 将数据转换为 PyArrow 的表格
    table = pa.Table.from_pandas(pd.DataFrame(data), schema=schema)

    # 将数据写入 Parquet 文件
    pq.write_table(table, '/Users/guohui/Downloads/dwd_null_parquet_required')
    
    readFile('/Users/guohui/Downloads/dwd_null_parquet_required')


def readFile(path):
    # 读取整个 Parquet 文件
    metadata = pq.read_metadata(path)
    print(metadata)
    
    original_schema = pq.read_schema(path)
    print(original_schema)
    
    df = pd.read_parquet(path)
    print(df)

def onlyWriteSchema(path, targetColumn):
    # 读取 Parquet 文件的模式信息
    old_schema = pq.ParquetFile(path).schema.to_arrow_schema()

    # 找到要修改的字段并更新其定义
    field_index = old_schema.get_field_index(targetColumn)
    if field_index >= 0:
        old_field = old_schema[field_index]
        new_field = old_field.with_metadata({'required': 'optional'})
        new_schema = old_schema.set(field_index, new_field)

        # 构造新的 ParquetWriter 对象，并将新模式写入新文件
        with pq.ParquetWriter('/Users/guohui/Downloads/modified_1.parquet', new_schema) as writer:
            # 将旧文件的元数据复制到新文件
            metadata = pq.ParquetFile(path).metadata
            pq.write_metadata(writer, metadata)



# for path in filepath:
#     print(f"文件: {path}")
#     checkColumn(path)

# rewrite(filepath[0])

# writeParquet()

# readRow(filepath[0], 50928)

# writeFile1()

# onlyWriteSchema('/Users/guohui/Downloads/dwd_biz_contract_d__aa84e33f_42d5_4bee_8ef9_deacc62c5f97', 'contract_type')


readFile('/Users/guohui/Downloads/dwd_flow_di__59f5eec3_a369_4f30_9478_3dadf4d4fd66')
# readFile('/Users/guohui/Downloads/parquet/dwd_biz_contract_d_with_wrong')
