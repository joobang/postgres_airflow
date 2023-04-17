# Postgres Merge Upsert
`postgres_merge_upsert` DAG는 `staging_table`의 ROW들을 `destination_table`의 `unique_keys`를 기준으로 새로운 ROW는 INSERT 동작을, 이미 존재하는 ROW는 REPLACE 동작을 한다. `unique_keys` 기준으로 `staging_table`에는 중복이 있더라도, `destination_table`에 merge될때에는 해당 ROW들이 **deduplication** 되어 반영된다. UPSERT가 완료되면 `staging_table`은 비워진다.

## Merge Upsert Configurations
| Parameter | Description | Required | Default | Type |
| --- | --- | --- | --- | --- |
| `merge_upsert.unique_keys` | UPSERT시 table row의 uniqueness를 판단하기 위한 컬럼 조합 | `True` | | `List` |
| `merge_upsert.destination_table` | UPSERT가 이루어져야하는 타겟 테이블 | `True` | | `String`
| `merge_upsert.staging_table` | UPSERT를 위한 데이터가 준비(staged)되어있는  테이블 | `True` | | `String`

## DAG configurations
DAG의 등록과 스케쥴링을 위해 정의되어야 하는 config.
| Parameter | Description | Required | Type |Example |
| --- | --- | --- | --- | --- |
| `pipeline_type` | DAG의 template type. DAG Builder는 각 `pipeline_type`에 맞는 template에 YAML 명세를 적용하여 DAG를 빌드하고 [DagBag](https://github.com/apache/airflow/blob/2.3.3/airflow/models/dagbag.py#L70-L90)에 추가한다. | `True` |  `String` | 
| `pipeline_key` | dag_id에 들어가는 이름이며 `{pipeline_type}_{pipeline_key}`으로 dag_id가 결정된다. | `False` | `String` | `postgres_merge_upsert_ad` |
| `pipeline_dag_configs` | DAG schedule에 영향을 주는 옵션들의 mapping이다. | `True` | `Map` | |
| `pipeline_dag_configs.start_date` | DAG schedule 시작 시각 (UTC) | `True` | `String` | `2021-12-31 15:00:00` |
| `pipeline_dag_configs.end_date` | DAG schedule 종료 시각 (UTC) | `False` | `String` | `2022-03-31 15:00:00` |
| `pipeline_dag_configs.schedule_interval` | CRON expression으로 표현되는 DAG schedule interval | `True` | `CRON` | `0 * * * *` |
| `pipeline_dag_configs.default_args` | DAG에 반영되는 [default_args](https://github.com/apache/airflow/blob/2.3.3/airflow/models/dag.py#L229-L234) dictionary | `True` | `Dict` | `0 * * * *` |

## Utility Configurations
DAG에 부수적인 기능들을 활용하기 위한 옵션들이다.

| Parameter | Description | Required | Default | Acceptable values |
| --- | --- | --- | --- | --- |
| `execution_delay_seconds` | DAG가 실행되기 전에 sleep하는 시간. 데이터 집계 이전 로그 데이터의 ingestion을 기다리거나, 리소스 피크 타임을 피하는 등의 용도로 활용된다. [TimeDeltaSensorAsync](https://github.com/apache/airflow/blob/2.3.3/airflow/sensors/time_delta.py#L43-L58) operator가 `'this'` DAG의 새로운 root가 되는 방식으로 구현되어있다.  | `False` | | [0, 2147483647] 사이의 positive integer |

## Dependency Configurations
서로 다른 DAG간 디펜던시를 설정하기 위한 옵션들이다.  
`upstream_dependency`란 
[ExternalTaskSensor](https://github.com/apache/airflow/blob/2.3.3/airflow/sensors/external_task.py#L53-L100) operator가 `'this'` DAG의 새로운 root가 되어서 `upstream`인 external DAG의 성공 유무를 확인하고 성공했을시에 `downstream`인 `'this'` DAG가 실행되는 방식으로 구현되어있다.

| Parameter | Description | Required | Default | Acceptable values |
| --- | --- | --- | --- | --- |
| `upstream_dependencies` | upstream dependency가 있는 경우 List 형태로 작성 | `False` |
| `upstream_dependencies.dag_id` | upstream DAG의 dag_id | `False` |
| `upstream_dependencies.timedelta_seconds` | 현재 DAG에서 완료되길 기다리는 upstream DAG의 시간 차이, `0`이면 같은 execution_date, `3600` 이면 1시간 이전, `-3600` 이면 **미래 1시간 이후** 이다 | `True` | `0` | `-1, -2, 1, 2 ...` |
| `upstream_dependencies.poke_interval` | upstream checker가 upstream dag가 완료되었는지 확인하는 주기 (초) | `True` | `180` | positive integer |
| `upstream_dependencies.timeout` | upstream checker가 upstream DAG를 기다리는 시간(초). 이 시간이 지나면 실패된다. | `True` | `7200` | positive integer |

