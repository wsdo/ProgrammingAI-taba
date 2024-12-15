# 欧盟经济数据分析项目

本项目专注于分析欧盟国家的经济数据，包括GDP、就业率、通货膨胀率等经济指标，最终将与教育投资数据进行关联分析。

## 项目结构

```
economic_analysis/
├── README.md                  # 项目说明文档
├── requirements.txt           # 项目依赖
├── .env                      # 环境变量配置
├── notebooks/                # Jupyter notebooks
│   └── economic_analysis.ipynb  # 主要分析notebook
├── src/                      # 源代码
│   ├── data/                 # 数据相关
│   │   ├── imf_processor.py  # IMF数据处理
│   │   └── data_cleaner.py   # 数据清洗工具
│   ├── analysis/             # 分析工具
│   │   ├── economic_indicators.py  # 经济指标分析
│   │   └── time_series.py    # 时间序列分析
│   └── visualization/        # 可视化工具
│       └── plotters.py       # 绘图工具
└── tests/                    # 测试代码
    └── test_data_processing.py  # 数据处理测试
```

## 安装和使用

1. 克隆项目并安装依赖：
```bash
git clone [项目地址]
cd economic_analysis
pip install -r requirements.txt
```

2. 配置环境变量：
创建.env文件并配置必要的环境变量：
```
IMF_API_KEY=your_api_key
```

3. 运行数据分析：
```bash
jupyter notebook notebooks/economic_analysis.ipynb
```

## 数据源

- IMF API：
  - GDP数据（年度、季度）
  - 就业率数据
  - 通货膨胀率
  - 人均GDP

## 功能特性

1. 数据获取和预处理：
   - 自动从IMF API获取经济数据
   - 数据清洗和标准化
   - 异常值检测和处理

2. 数据分析：
   - 经济指标趋势分析
   - 相关性分析
   - 时间序列分析

3. 可视化：
   - 交互式图表
   - 时间序列可视化
   - 相关性热力图

## 开发指南

1. 代码规范：
   - 遵循PEP 8规范
   - 使用类型注解
   - 编写完整的文档字符串

2. 测试：
   - 运行测试：`pytest tests/`
   - 测试覆盖率报告：`pytest --cov=src tests/`

## 注意事项

- IMF API访问限制和配额管理
- 数据缓存和版本控制
- 错误处理和日志记录

## 贡献指南

1. Fork项目
2. 创建特性分支
3. 提交更改
4. 推送到分支
5. 创建Pull Request

## 许可证

MIT License
