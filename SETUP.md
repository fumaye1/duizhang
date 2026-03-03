# Windows 环境安装与配置（路径 B：Python + Streamlit）

本文档记录本项目在 Windows 上的安装、依赖与 PATH 配置流程，并提供“一键脚本”固定环境。

## 1. 前置条件
- Windows 10/11
- 管理员权限（用于写入系统级 PATH）

## 2. 安装 Python（3.10+）
使用 `winget` 安装 Python 3.11：

```powershell
winget install -e --id Python.Python.3.11 --source winget
```

验证安装：

```powershell
py -3.11 -V
```

## 3. 安装依赖（含测试依赖）
在项目根目录执行：

```powershell
py -3.11 -m pip install -r requirements.txt -r requirements-dev.txt
```

## 4. 固定环境（推荐）
运行一键脚本，将 Python 3.11 与 Scripts 写入系统级 PATH，并去重、置顶：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\fix_python_env.ps1
```

执行后请**重新打开终端**，再验证：

```powershell
python -V
python -m pip --version
```

## 5. 版本验证

```powershell
python -m pytest --version
python -m streamlit --version
```

## 6. 常见问题
- `python` 仍不可用：请关闭并重新打开终端；若仍失败，重新执行脚本。
- `py` 可用但 `python` 不可用：说明 PATH 未刷新或被别名拦截，运行脚本后重启终端。
