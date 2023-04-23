## Buzzvil Data Engineer 채용 과제


과제에 대한 설명은 readme 폴더([1_instructions.md](/readme/1_instructions.md), [2_datasets.md](/readme/2_datasets.md), [3_boilerplated.md](/readme/3_boilerplates.md))에서 확인할 수 있습니다.

## 변경사항
```bash
 ┣ 📂analytics_db
 ┃ ┣ 📂init_db
 ┃ ┃ ┣ 📜db.init_tables_1.sql      --> scheduler 테이블 추가
 ┃ ┃ ┗ 📜db.init_tables_5_insert_unit.sql   -->  update, create time 추가
 ┣ 📂src
 ┃ ┣ 📂configs
 ┃ ┃ ┗ 📂postgres_merge_upsert
 ┃ ┃ ┃ ┣ 📜postgres_merge_upsert_ad_lineitem.yaml   -->  pipeline_dag_configs.default_args 추가
 ┃ ┃ ┃ ┗ 📜postgres_merge_upsert_unit.yaml    -->  pipeline_dag_configs.default_args 추가
 ┃ ┣ 📂dags
 ┃ ┃ ┣ 📜builder.py   --> part2 과제
 ┃ ┃ ┣ 📜postgres_merge_upsert_ad_lineitem_part1.py   --> part1 과제
 ┃ ┃ ┣ 📜postgres_merge_upsert_unit_part1.py    --> part1 과제
 ┃ ┃ ┗ 📜postgres_transform_load_m1_d_ad_mart_metrics_part1.py    --> part1 과제
``` 

## 과제설명

### 과제1. 파이프라인 결함 확인하고 보완하기

1-1. postgres_merge_upsert_ad_lineitem과 postgres_merge_upsert_units DAG가 원본 운영계 테이블과의 정합성을 보장하는 방식으로 ETL이 이루어지는지?

- ad_lineitem, units는 pk가 id이기 때문에 중복데이터를 적재할 수 없다.

  방법1 : 시간정렬 desc + DISTINCT ON (id)를 이용해서 최근날짜만 적재.

  방법2 : 시간정렬 asc 순차대로 INSERT INTO ON CONFILCT DO UPDATE 사용하여 적재.

  방법3 : MAX(update_at)값으로 JOIN 이용해서 적재.

**src/postgres_merge_upsert_ad_lineitem_part1.py, postgres_merge_upsert_unit_part1.py**
- 기존의 dag는 전체데이터를 대상으로 merge_upsert를 진행했기 때문에 처리할 데이터의 범위가 명확하지 않아 추후 데이터 비교 시 어려울 가능성이 있다. 정합성 체크를 위해서 스케쥴 시간에 맞춰 시간별로 데이터를 적재하고 추후에 카운트 비교할 수 있도록 변경했다.
  
  - {table}_scheduler 테이블을 만들어 시간 별 카운트 수 체크.

- 첫 시행때만 start_date 전의 데이터도 적재하기 위해 BranchPythonOperator 사용하여 task 분기시켜서 적재.

1-2. postgres_transform_load_m1_d_ad_mart_metrics DAG의 Transform + Load가 idempotent하게 구현이 되어있는지?

1-3. postgres_transform_load_m1_d_ad_mart_metrics DAG가 Fact와 Mart 데이터간 정합성을 보장할 수 있게끔 구현이 되어있는지?

**src/postgres_transform_load_m1_d_ad_mart_metrics_part1.py**

- postgres_transform_load_m1_d_ad_mart_metrics DAG의 Transform + Load가 여러번 실행되어도 동일한 결과가 나온다. 하지만 UNION된 두개의 select문에서 중복데이터가 발생할 수 있다.
    -  data_at, lienitem_id, unit_id로 그룹화하여 중복데이터를 없애고, 중복데이터를 방지하기위해 merge_upsert dag처럼 INSERT 전에 현재 적재할 시간범위의 데이터를 mart테이블에서 삭제하고 INSERT하게 변경.
    - 정합성 체크를 위해 {table}_scheduler 테이블을 만들어 시간 별 카운트 수 체크.

### 과제2. 기존의 Airflow 환경을 셀프 서빙 플랫폼으로 업그레이드하기

1-1. Python의 숙련도가 없는 Data Analyst도 분석 SQL과 YAML만으로도 DAG를 생성할 수 있도록 DAG Builder를 구현하는 것이 목표입니다.

1-2. postgres_merge_upsert 타입의 YAML 스펙(configs/configs/postgres_merge_upsert/README.md), 그리고 해당 스펙들에 맞추어 작성된 YAML 명세를 확인하시고 DAG Builder 구현.

**src/builder.py**

- configs/postgres_merge_upsert의 *.yaml 파일을 읽어서 Dag를 생성하는 builder

- configs/postgres_merge_upsert/README.md를 참고하여 src/dags/postgres_merge_upsert_*_part1.py 와 같은 작업을 하는 DAG를 생성하고 globals() 전역변수에 yaml별 dag를 등록해 airflow에서 인식하도록 했다.

- yaml 의 execution_delay_seconds, upstream_dependencies 값에 따라 TimeDeltaSensorAsync, ExternalTaskSensor task를 생성하여 의존성 설정을 추가했다.
