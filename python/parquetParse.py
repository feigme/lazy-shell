import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


path_1 = '/Users/guohui/Downloads/dwd_biz_contract_d__aa84e33f_42d5_4bee_8ef9_deacc62c5f97'
path_2 = '/Users/guohui/Downloads/dwd_biz_contract_d__04baa43a_9607_4a8d_95f4_03e7403b5794'

def check_parquet(filepath):
    try:
        table = pq.read_table(filepath)
        print("Parquet文件完整")
    except Exception as e:
        print("Parquet文件不完整或存在错误: ", str(e))
        
# 调用函数进行检查
# check_parquet(path_3)

# 指定要读取的列
columns_to_read = [
    'id',
    'biz_type',
    'biz_id',
    # 'contract_type',
    # 'online_flag',
    'contract_name',
    'contract_url',
    'contract_no',
    'sign_status',
    'start_time',
    'end_time',
    'out_fid',
    'out_contract_no',
    'out_contract_url',
    'create_time',
    'edit_time',
    # 'creator',
    # 'editor',
    'is_deleted',
    # 'version',
    'sign_positions_json',
    'contract_download_url',
    'bu_id'
]

# 读取 Parquet 文件，并只选择指定的列
df = pd.concat([
    pd.read_parquet(path_1, columns=columns_to_read),
    pd.read_parquet(path_2, columns=columns_to_read)
])

# 打印前10行数据
# print(df.head(10))

print(df)

# 获取字符串列的最长字符长度
# print("最长字符长度:", df['creator'].str.len().max())