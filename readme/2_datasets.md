# Overview
제공해드리는 데이터들에 대한 정의 및 설명을 기술한 문서입니다. 테이블들은 `Fact`, `Dimension`, `Mart` 타입으로 나뉘며 각 타입별로 다른 방식으로 데이터 ETL이 진행됩니다.

# Fact Tables

Fact 테이블들은 로그 데이터의 원본을 저장하는 테이블들입니다.  
Fact 테이블을 이루는 로그 데이터들에 대한 수집 및 적재는 Airflow 플랫폼 밖에서 Confluent Kafka와 Confluent [Postgres Sink Connector](https://docs.confluent.io/cloud/current/connectors/cc-postgresql-sink.html#postgresql-sink-jdbc-connector-for-ccloud)를 이용해서 이루어지며,  
`[ Webserver → <PUBLISH> → Kafka Log Topic ← <SUBCRIBE> ← Confluent Postgres Sink Connector ]`의 flow를 통해서 Postgres까지 전달됩니다.

과제의 편의성을 위해서 각 record가 Postgres에 INSERT 될때는 created_at 컬럼 기준으로 순서 보장이 되며, UPSERT semantics가 Postgres에 발생시키는 오버헤드는  가정하셔도 됩니다.

## 1. ad_impressions
`ad_impression` 테이블은 광고에 대한 노출(impression) 이벤트를 수집한 로그 테이블이며, 사용된 Postgres Sink Connector의 configuration은 다음과 같습니다.
```
{
  "connector.class": "PostgresSink",
  "name": "PostgresSinkConnectorAdImpressions",
  "input.data.format": "AVRO",
  "kafka.auth.mode": "KAFKA_API_KEY",
  "kafka.api.key": "****************",
  "kafka.api.secret": "****************************************************************",
  "connection.host": "buzzvildeassignment.ap-northeast-1.rds.amazonaws.com",
  "connection.port": "5432",
  "connection.user": "postgres",
  "connection.password": "**************",
  "db.name": "postgres_analytics",
  "topics": "ad_impressions",
  "insert.mode": "UPSERT",
  "db.timezone": "UTC",
  "auto.create": "false",
  "auto.evolve": "true",
  "pk.mode": "record_value",
  "pk.fields": "user_id, created_at, unit_id, lineitem_id",
  "tasks.max": "4"
}
```
Postgres Sink Connector의 **consumer lag**는 별도의 모니터링 시스템으로 트래킹되고 있으며, lag가 600 seconds가 넘는 경우 따로 Alert가 발생하여 on-call 데이터 엔지니어가 핸들링을 하게 됩니다.

## 2. ad_clicks
`ad_click` 테이블은 광고에 대한 클릭(click) 이벤트를 수집한 로그 테이블이며, `ad_impression`과 동일한 방식으로 데이터가 적재됩니다.

# Dimension Tables

Dimension 테이블들은 운영계 DB로부터 CDC(Change Data Capture) 데이터를 받아온 테이블입니다.  
Dimension 데이터가 해당 테이블로 **INSERT**되는 방식은 위 `ad_impression`,  `ad_click` 테이블의 방식처럼 **UPSERT** semantic이 아닌 **INSERT** semantic인점을 제외하고는 동일하며, 해당 테이블을 사용해서 `ad_lineitem` 테이블에 대한 MERGE_UPSERT를 진행합니다.



## 1. ad_linetem
`ad_lineitem` 테이블은 광고 라인아이템 정보를 담고있는 운영계 모델의 데이터를 그대로 가져온 데이터입니다. 
`ad_lineitem_staging` 테이블에 운영계 DB로 부터 CDC 데이터를 전달받아 적재한 테이블입니다.

## 2. units
`unit` 테이블은 광고가 노출되는 지면에 대한 테이블이며, `unit_staging` 테이블에 `ad_lineitem_staging`과 동일하게 운영계 DB로 부터 CDC 데이터를 전달받아 적재한 테이블입니다.

# Mart Tables

## m1_d_ad_mart_metrics

`m1_d_ad_mart_metrics` 테이블은 광고 노출, 클릭 정보를 일단위(KST 기준)로 집계하는 통계 마트 테이블입니다. 자세한 집계 로직은 DAG와 DAG상의 집계 쿼리를 참조해주세요.
