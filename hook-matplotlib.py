from PyInstaller.utils.hooks import collect_data_files, collect_submodules

# 包含所有Matplotlib子模块
hiddenimports = collect_submodules('matplotlib')

# 包含Matplotlib数据文件
datas = collect_data_files('matplotlib')