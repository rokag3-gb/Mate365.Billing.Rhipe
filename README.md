# 작동매뉴얼
## Setting
- 환경변수 세팅
  - 운영/개발 변수
  ```
  CRAWLER_ENV
  ```
  - Rhipe 인증 변수
  ```
    client_id
    client_secret
    contractagreement_id
  ```
  - cliper 인증 변수 (삭제예정)
  ```
  cliper_id
  cliper_pw
  s3_access_key
  s3_bucket_hosts
  s3_prefix
  s3_region_name
  s3_secret_key
  ```
  - database
  ```
  DATABASE_HOST
  DATABASE_NAME
  DATABASE_PASSWORD
  DATABASE_PORT
  DATABASE_TYPE
  DATABASE_USER
  ```
  - Teams logging
  ```
  TEAMS_WEBHOOK_INFO_URL
  TEAMS_WEBHOOK_LOG_URL
  ```
  - Update 로직 실행시 필요함
  `CRAWLER_UPDATE_PERIOD`
## 실행
```
python main.py -t crawler // 일별 사용량 수집
python main.py -t update // CRAWLER_UPDATE_PERIOD 변수만큼 ~최근일자까지 업데이트 
python main.py -t price_update // price table 업데이트
python main.py -t invoice // 최신 월 인보이스 업데이트
```