## Buzzvil Data Engineer 채용 과제


과제에 대한 설명은 readme 폴더([1_instructions.md](/readme/1_instructions.md), [2_datasets.md](/readme/2_datasets.md), [3_boilerplated.md](/readme/3_boilerplates.md))에서 확인할 수 있습니다.

데이터 정보.
Fact Tables : 로그데이터의 원본 테이블.
과정 : [ Webserver → <PUBLISH> → Kafka Log Topic ← <SUBCRIBE> ← Confluent Postgres Sink Connector ]
    각 record가 Postgres에 INSERT 될때는 created_at 컬럼 기준으로 순서 보장이 되며, Upsert
종류 : ad_impressions(광고에 대한 노출 이벤트 수집 로그 테이블)
    - Postgres Sink Connector의 consumer lag는 별도의 모니터링 시스템으로 트래킹되고 있으며, lag가 600 seconds가 넘는 경우 따로 Alert가 발생하여 on-call 데이터 엔지니어가 핸들링

    ad_clicks(광고에 대한 클릭 이벤트 수집 로그 테이블)
    - 위와 동일한 방식으로 데이터 적재.

Postgres Sink Connector의 configuration
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
Dimension Tables : 운영계 DB로부터 CDC(Change Data Capture) 데이터를 받아온 테이블
과정 : ad_impression, ad_click 테이블의 방식처럼 UPSERT semantic이 아닌 INSERT semantic인점을 제외하고는 동일. 
      - 해당 테이블을 사용해서 ad_lineitem 테이블에 대한 MERGE_UPSERT를 진행
종류 : ad_linetem(광고 라인아이템 정보를 담고있는 운영계 모델의 데이터를 그대로 가져온 데이터)
      - ad_lineitem_staging 테이블에 운영계 DB로 부터 CDC 데이터를 전달받아 적재한 테이블.

     units(광고가 노출되는 지면에 대한 테이블)
     - unit_staging 테이블에 ad_lineitem_staging과 동일하게 운영계 DB로 부터 CDC 데이터를 전달받아 적재한 테이블

Mart Tables
m1_d_ad_mart_metrics (광고 노출, 클릭 정보를 일단위(KST 기준)로 집계하는 통계 마트 테이블)

미션1. 파이프라인 결함 확인하고 보완하기
1-1. postgres_merge_upsert_ad_lineitem과 postgres_merge_upsert_units DAG가 원본 운영계 테이블과의 정합성을 보장하는 방식으로 ETL이 이루어지는지?
1-2. postgres_transform_load_m1_d_ad_mart_metrics DAG의 Transform + Load가 idempotent하게 구현이 되어있는지?
1-3. postgres_transform_load_m1_d_ad_mart_metrics DAG가 Fact와 Mart 데이터간 정합성을 보장할 수 있게끔 구현이 되어있는지?

미션2. 기존의 Airflow 환경을 셀프 서빙 플랫폼으로 업그레이드하기
1-1. Python의 숙련도가 없는 Data Analyst도 분석 SQL과 YAML만으로도 DAG를 생성할 수 있도록 DAG Builder를 구현하는 것이 목표입니다.
1-2. postgres_merge_upsert 타입의 YAML 스펙(configs/configs/postgres_merge_upsert/README.md), 그리고 해당 스펙들에 맞추어 작성된 YAML 명세를 확인하시고 DAG Builder 구현.
1-3. DBT에서 플랫폼 유저에게 어떤 인터페이스를 제공하는지를 참고하기.