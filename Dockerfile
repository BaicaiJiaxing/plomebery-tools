FROM docker.m.daocloud.io/python:3.10 AS base


# 设置工作目录
WORKDIR /app
ENV PIP_PROGRESS_BAR=off
RUN pip install --no-deps --upgrade pip --no-cache-dir 

COPY requirements.txt .
RUN pip install --no-cache-dir --no-deps  -r requirements.txt


COPY . run

EXPOSE 8000

# 启动 FastAPI
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8999"]
