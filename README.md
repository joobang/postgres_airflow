## Buzzvil Data Engineer 채용 과제


과제에 대한 설명은 readme 폴더([1_instructions.md](/readme/1_instructions.md), [2_datasets.md](/readme/2_datasets.md), [3_boilerplated.md](/readme/3_boilerplates.md))에서 확인할 수 있습니다.
## 미션설명

### 미션1. 파이프라인 결함 확인하고 보완하기

1-1. postgres_merge_upsert_ad_lineitem과 postgres_merge_upsert_units DAG가 원본 운영계 테이블과의 정합성을 보장하는 방식으로 ETL이 이루어지는지?

- ad_lineitem, units는 pk가 id이기 때문에 중복데이터를 적재할 수 없다.

  방법1 : 시간정렬 desc + DISTINCT ON (id)를 이용해서 최근날짜만 적재.

  방법2 : 시간정렬 asc 순차대로 INSERT INTO ON CONFILCT DO UPDATE 사용하여 적재.

  방법3 : MAX(update_at)값으로 JOIN 이용해서 적재.

- 정합성 체크를 위해서 스케쥴이 실행되는 정각 기준 한시간씩 시간을 나눠 적재하고 적재카운트를 체크한다.

  스케쥴 시간대에 staging 데이터 개수와 ad_lineitem, units에 적재된 카운트 수를 비교,

  같으면 true, 다르면 false 처리?

1-2. postgres_transform_load_m1_d_ad_mart_metrics DAG의 Transform + Load가 idempotent하게 구현이 되어있는지?

1-3. postgres_transform_load_m1_d_ad_mart_metrics DAG가 Fact와 Mart 데이터간 정합성을 보장할 수 있게끔 구현이 되어있는지?

### 미션2. 기존의 Airflow 환경을 셀프 서빙 플랫폼으로 업그레이드하기

1-1. Python의 숙련도가 없는 Data Analyst도 분석 SQL과 YAML만으로도 DAG를 생성할 수 있도록 DAG Builder를 구현하는 것이 목표입니다.

1-2. postgres_merge_upsert 타입의 YAML 스펙(configs/configs/postgres_merge_upsert/README.md), 그리고 해당 스펙들에 맞추어 작성된 YAML 명세를 확인하시고 DAG Builder 구현.

1-3. DBT에서 플랫폼 유저에게 어떤 인터페이스를 제공하는지를 참고하기.

