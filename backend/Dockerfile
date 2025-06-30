FROM python:3.9-slim

WORKDIR /app

# 시스템 의존성 설치
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 설치
COPY ../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY . .
COPY ../config.py .
COPY ../utils ./utils
COPY ../screeners ./screeners
COPY ../portfolio ./portfolio
COPY ../ranking ./ranking

# 포트 노출
EXPOSE ${BACKEND_PORT:-5000}

# 애플리케이션 실행
CMD ["python", "api_server.py"]