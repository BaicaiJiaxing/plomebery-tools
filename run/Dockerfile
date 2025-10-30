FROM docker.m.daocloud.io/python:3.10 AS base


# 设置工作目录
WORKDIR /app
ENV PIP_PROGRESS_BAR=off
RUN pip install --no-deps --upgrade pip --no-cache-dir 
# ============ 第一层：安装依赖，单独缓存 ============
# 只复制 requirements.txt，这一层最多只在你改依赖时重建
COPY requirements.txt .
RUN pip install --no-cache-dir --no-deps  -r requirements.txt

# ============ 第二层：复制代码（不影响依赖缓存） ============

COPY . run

# FastAPI 默认 8000
EXPOSE 8000

# 启动 FastAPI
CMD ["uvicorn", "run.app:app", "--host", "0.0.0.0", "--port", "8999"]
